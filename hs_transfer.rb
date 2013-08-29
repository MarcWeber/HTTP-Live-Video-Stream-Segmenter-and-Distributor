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

    def require
      Array[self::REQUIRES].each {|v| require v}
    end

    def can
      require
      return true
    rescue LoadError
      return false
    end
  end

  module SetInstanceVars
    def set_instance_vars(config, *args)
      args.each {|v| self.instance_variable_set(v, config..fetch(v)) }
    end
  end

  class SCP
    extend TransferBase
    include SetInstanceVars
    RERQUIRES = 'net/scp'

    def initialize(config)
      set_instance_vars(config, 'config', 'user_name','directory')
      # optional:
      @password = config['password']
    end

    def withConnection(&blk)
      if @password
        Net::SCP.start!(@remote_host, @user_name, :password => password, &blk)
      else
        Net::SCP.start!(@remote_host, @user_name, &blk)
      end
    end

    def create_file(destination_file, io)
      withConnection do |scp|
        scp.upload! io, @directory + destination_file
      end
    end
  end

  class Copy
    extend TransferBase
    include SetInstanceVars
    RERQUIRES = []

    def initialize(config)
      set_instance_vars(config, 'directory')
    end

    def create_file(destination_file, io)
      File.open(@directory + destination_file, "wb") { |file| file.write(io) }
    end
  end

  class S3
    RERQUIRES = 'right_aws'
    extend TransferBase
    include SetInstanceVars

    def initialize(config)
      set_instance_vars(config, 'aws_api_key','aws_api_secret','bucket_name','key_prefix')
      @s3 = RightAws::S3Interface.new(@aws_api_key, @aws_api_secret)
    end

    def create_file(destination_file, io)
      content_type = source_file =~ /.*\.m3u8$/ ? 'application/vnd.apple.mpegurl' : 'video/MP2T'
      @log.debug("Content type: #{content_type}")
      @s3.put(@bucket_name, "#{@key_prefix}/#{destination_file}", io, {'x-amz-acl' => 'public-read', 'content-type' => content_type})
    end
  end

  class CF
    extend TransferBase
    include SetInstanceVars
    RERQUIRES = 'cloudfiles'

    def initialize(config)
      set_instance_vars(config, 'username', 'api_key', 'container', 'key_prefix')
    end

    def create_file(destination_file, io)
      cf = CloudFiles::Connection.new(:username => @username, :api_key => @api_key)
      container = cf.container(@container)
      object = container.create_object "#{@key_prefix}/#{destination_file}", false
      object.write io
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
    'cf' => Transfer::CF
  }

  def self.init_and_start_transfer_thread(log, config)
    transfer_config = @config[@config['transfer_profile']]
    profiles = Array[*transfer_config]
    transfers = profiles.map { |profile| TRANSFER_TYPES[profile['transfer_type']].new(profile) }
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

  def initialize(log, config, transfer)
    @transfer = transfre
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
        rescue
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

  # use method_missing or such instead? for backward compatibility
  def self.can_scp; self.can('scp'); end
  def self.can_ftp; self.can('ftp'); end
  def self.can_s3; self.can('s3'); end
  def self.can_cf; self.can('cf'); end

  private

  def create_and_transfer_multirate_index
    # the multirate_index references the individual index files

    @log.debug('Creating multirate index')
    StringIO.new do |index_file|
      index_file.write("#EXTM3U\n")

      @config['encoding_profile'].each do |encoding_profile_name|
        encoding_profile = @config[encoding_profile_name]
        index_file.write("#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=#{encoding_profile['bandwidth']}\n")
        index_name = "%s_%s.m3u8" % [@config['index_prefix'], encoding_profile_name]
        index_file.write("#{@config['url_prefix']}#{index_name}\n")
      end

      @log.debug('Transfering multirate index')
      create_file("#{@config["index_prefix"]}_multi.m3u8", index_file)
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

    # Transfer the index
    destination_file = "%s_%s.m3u8" % [@config['index_prefix'], encoding_profile]
    StringIO.new do |index_file|
      create_index(index_file, @config['index_segment_count'], @config['segment_length'], @config['segment_prefix'], encoding_profile, @config['url_prefix'], first_segment.to_i, last_segment.to_i, stream_end.to_i == 1)
      create_file(destination_file, index_file)
    end

    # Transfer the video stream
    video_filename = "#{@config['temp_dir']}/#{@config['segment_prefix']}_#{encoding_profile}-%05u.ts" % last_segment.to_i
    dest_video_filename = "#{@config['segment_prefix']}_#{encoding_profile}-%05u.ts" % last_segment.to_i
    File.open(video_filename, "rb") do |file|
      create_file(file, dest_video_filename)
    end
  end

  def create_file(destination_file, io_rewindable)
    @transfers.each do |t|
      io_rewindable.rewind
      t.create_file(destination_file, io_rewindable)
    end
  end
end
