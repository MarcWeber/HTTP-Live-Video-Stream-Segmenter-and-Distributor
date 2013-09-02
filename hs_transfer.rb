#!/usr/bin/env ruby
#
# Copyright (c) 2009-2013 Carson McDonald
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License version 2
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

require 'rubygems'

require 'fileutils'

require 'stringio'

module Transfer
  # note: I'm not sure whether its worth thinking about keeping a FTP/S3 like
  # connection open. There might be timeouts, there might be failures.
  # Reconnecting ensures operation continues if such a failure happens.
  # Otherwise more problems have to be caught and handled

  module Can

    def require_modules
      Array[*self::REQUIRES].each {|v| require v}
    end

    def can
      require_modules
      return true
    rescue LoadError
      return false
    end
  end

  module SetInstanceVars
    attr_reader :url_prefix, :url_prefix_m3u
    def set_instance_vars(config, *args)
      args.each {|v| self.instance_variable_set("@#{v}", config[v]) }
      @url_prefix = config['url_prefix']
      @url_prefix_m3u = config['url_prefix_m3u'] || @url_prefix
    end
  end

  class SCP
    extend Can
    include SetInstanceVars
    REQUIRES = 'net/scp'

    def initialize(log, config)
      @log = log
      self.class.require_modules
      set_instance_vars(config, 'config', 'remote_host', 'user_name', 'directory')
      # optional:
      @password = config['password']
    end

    def withConnection(&blk)
      if @password
        Net::SCP.start!(@remote_host, @user_name, :password => @password, &blk)
      else
        Net::SCP.start!(@remote_host, @user_name, &blk)
      end
    end

    def create_file(destination_file, io)
      withConnection do |scp|
        scp.upload! io, @directory + destination_file
      end
    end

    def try_delete_file(name)
      puts "TODO: delete @directory + destination_file"
    end
  end

  class Copy
    extend Can
    include SetInstanceVars
    REQUIRES = []

    def initialize(log, config)
      @log = log
      self.class.require_modules
      set_instance_vars(config, 'directory')
    end

    def create_file(destination_file, io)
      require 'fileutils'
      file = @directory + destination_file
      FileUtils.mkdir_p File.dirname(file)
      File.open(file, "wb") { |file| file.write(io.read) }
    end

    def try_delete_file(name)
      n = @directory + name
      puts "trying to delete #{n}"
      File.delete(n) if File.exist? n
    end
  end

  class S3
    REQUIRES = 'right_aws'
    extend Can
    include SetInstanceVars

    def initialize(log, config)
      @log = log
      self.class.require_modules
      set_instance_vars(config, 'aws_api_key','aws_api_secret','bucket_name','key_prefix')
      @s3 = RightAws::S3Interface.new(@aws_api_key, @aws_api_secret)


      if (config['cf_distribution_id'])
	@cf =   RightAws::AcfInterface.new(config['cf_aws_api_key'],config['cf_aws_api_secret'])
	@cf_distribution_id = config['cf_distribution_id']
      end
    end

    def create_file(destination_file, io)
      begin
	content_type = destination_file =~ /.*\.m3u8$/ ? 'application/vnd.apple.mpegurl' : 'video/MP2T'
	@s3.put(@bucket_name, "#{@key_prefix}#{destination_file}", io, {'x-amz-acl' => 'public-read', 'content-type' => content_type})
      rescue Exception => e
	@log.debug("error for #{destination_file}")
	puts e.message
	puts e.backtrace
      end
    end

    def invalidate(name)
      @cf.create_invalidation(@cf_distribution_id, :path => [ "/#{@key_prefix}#{name}"]) if @cf
    end

    def try_delete_file(name)
      begin
	n = "#{@key_prefix}/#{name}"
	@s3.delete(@bucket_name, n)
      rescue
	@log.debug( "s3: error deleting #{n}")
      end
    end
  end

  class CF
    extend Can
    include SetInstanceVars
    REQUIRES = 'cloudfiles'

    def initialize(log, config)
      @log = log
      self.class.require_modules
      set_instance_vars(config, 'username', 'api_key', 'container', 'key_prefix')
    end

    def create_file(destination_file, io)
      cf = CloudFiles::Connection.new(:username => @username, :api_key => @api_key)
      container = cf.container(@container)
      object = container.create_object "#{@key_prefix}/#{destination_file}", false
      object.write io
    end
  end

  class FTP
    extend Can
    include SetInstanceVars
    REQUIRES = 'net/ftp'

    def initialize(log, config)
      @log = log
      self.class.require_modules
      set_instance_vars(config, 'remote_host', 'user_name', 'password', 'directory')
    end

    def create_file(destination_file, io)
      Net::FTP.open(@remote_host) do |ftp|
	ftp.login(@user_name, @password)
	files = ftp.chdir(@directory)
	# writing a file multiple times when connecting to many ftp servers
	# might not be too efficient, I don't expect you to do that ..
	Tempfile.open('ftp-tmp') do |f|
	  f.write(io.read)
	  f.close
	  tp.putbinaryfile(f.path, destination_file)
	end
      end
    end
  end
end

class HSTransfer

  QUIT='quit'
  MULTIRATE_INDEX='mr_index'
  DELETE_OLD='delete_old' # TODO: delete old segments on restart, implement this, call this before starting first transfer

  # this implementation may change, eg create classes instead?
  TRANSFER_TYPES = {
    'scp' => Transfer::SCP,
    'ftp' => Transfer::FTP,
    's3' => Transfer::S3,
    'cf' => Transfer::CF,
    'copy' => Transfer::Copy
  }

  def self.init_and_start_transfer_thread(log, config)
    profiles_names = Array[*config['transfer_profile']]
    transfers = profiles_names.map do |profile_name|
      profile = config[profile_name]
      TRANSFER_TYPES[profile['transfer_type']].new(log, profile)
    end
    hstransfer = HSTransfer.new(log, config, transfers)
    hstransfer.start_transfer_thread
    return hstransfer
  end

  def <<(transfer_item)
    @transfer_queue << transfer_item
  end

  def stop_transfer_thread
    @transfer_queue << QUIT
    @transfer_thread.join
  end

  def initialize(log, config, transfers)
    @transfers = transfers
    @transfer_queue = Queue.new
    @log = log
    @config = config
  end

  def start_transfer_thread
    @transfer_thread = Thread.new do
      # why not log which transfer is initiated?
      @log.info('Transfer thread started');
      while (value = @transfer_queue.pop)
        begin
          @log.info("Transfer initiated with value = *#{value}*");

          if value == QUIT
            break
          elsif value == MULTIRATE_INDEX
            create_and_transfer_multirate_index
          else
            create_index_and_run_transfer(value)
          end

          @log.info('Transfer done');
	rescue Exception => e
          puts e.message
          puts e.backtrace
          @log.error("Error running transfer: " + $!.inspect)
        end
      end
      @log.info('Transfer thread terminated');
    end
  end

  # returns whether this transfer_type is available
  def self.can(transfer_type)
    TRANSFER_TYPES.fetch(transfer_type).can
  end

  def self.known(transfer_type)
    TRANSFER_TYPES.include? transfer_type
  end

  # use method_missing or such instead? for backward compatibility
  def self.can_scp; self.can('scp'); end
  def self.can_ftp; self.can('ftp'); end
  def self.can_s3; self.can('s3'); end
  def self.can_cf; self.can('cf'); end

  private

  def create_and_transfer_multirate_index
    # the multirate_index references the individual index files

    @transfers.each do |transfer|

      @log.debug('Creating multirate index')
      StringIO.open do |index_file|
	index_file.write("#EXTM3U\n")

	@config['encoding_profile'].each do |encoding_profile_name|
	  encoding_profile = @config[encoding_profile_name]
	  index_file.write("#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=#{encoding_profile['bandwidth']}\n")
	  index_name = "%s_%s.m3u8" % [@config['index_prefix'], encoding_profile_name]
	  index_file.write("#{transfer.url_prefix_m3u}#{index_name}\n")
	end

	@log.debug('Transfering multirate index')
	transfer.create_file("#{@config["index_prefix"]}_multi.m3u8", index_file)
      end
    end
  end

  def create_index(io, index_segment_count, segment_duration, output_prefix, encoding_profile, http_prefix, first_segment, last_segment, stream_end)
    @log.debug('Creating index');

    io.write("#EXTM3U\n")
    io.write("#EXT-X-TARGETDURATION:#{segment_duration}\n")
    io.write("#EXT-X-MEDIA-SEQUENCE:#{last_segment >= index_segment_count ? last_segment-(index_segment_count-1) : 1}\n")

    first_segment.upto(last_segment) do | segment_index |
      if segment_index > last_segment - index_segment_count
        io.write("#EXTINF:#{segment_duration},\n")
        io.write("#{http_prefix}#{output_prefix}_#{encoding_profile}-%05u.ts\n" % segment_index)
      end
    end

    io.write("#EXT-X-ENDLIST\n") if stream_end

    @log.debug('Done creating index');
  end

  def create_index_and_run_transfer(value)
    (first_segment, last_segment, stream_end, encoding_profile) = value.strip.split(%r{,\s*})

    # Transfer the video stream
    video_filename = "#{@config['temp_dir']}/#{@config['segment_prefix']}_#{encoding_profile}-%05u.ts" % last_segment.to_i
    dest_video_filename = "#{@config['segment_prefix']}_#{encoding_profile}-%05u.ts" % last_segment.to_i
    File.open(video_filename, "rb") do |file|
      create_file(dest_video_filename, file)
    end
    # don't fill up the tmp directory
    File.delete video_filename

    @transfers.each do |transfer|
      # Transfer the index
      destination_file = "%s_%s.m3u8" % [@config['index_prefix'], encoding_profile]
      StringIO.open do |index_file|
	create_index(index_file, @config['index_segment_count'], @config['segment_length'], @config['segment_prefix'], encoding_profile, transfer.url_prefix, first_segment.to_i, last_segment.to_i, stream_end.to_i == 1)
	@log.info("transferring #{destination_file}")
	index_file.rewind
	transfer.create_file(destination_file, index_file)
	if transfer.respond_to? :invalidate
	  @log.info("invalidating #{destination_file}")
	  transfer.invalidate(destination_file) 
	end
      end
    end

    if (@config['delete_nth_segment_back'])
      name = "#{@config['segment_prefix']}_#{encoding_profile}-%05u.ts" % (last_segment.to_i - @config['delete_nth_segment_back'].to_i)
      try_delete_file(name)
    end
  end

  def try_delete_file(name)
    @log.info("trying to delete #{name}")
    @transfers.each do |t|
      t.try_delete_file(name)
    end
  end

  def create_file(destination_file, io_rewindable)
    @log.info("transferring #{destination_file}")
    @transfers.each do |t|
      io_rewindable.rewind
      t.create_file(destination_file, io_rewindable)
    end
  end
end
