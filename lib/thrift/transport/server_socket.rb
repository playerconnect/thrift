# encoding: ascii-8bit
# 
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
# 

require 'socket'

module Thrift
  class ServerSocket < BaseServerTransport

    attr_reader :handle, :port, :sockets

    TIMEOUT = 60 * 5 # 5 minutes

    # call-seq: initialize(host = nil, port)
    def initialize(host_or_port, port = nil)
      @sockets = []
      if port
        @host = host_or_port
        @port = port
      else
        @host = nil
        @port = host_or_port
      end
      @handle = nil
    end

    def listen
      @handle = TCPServer.new(@host, @port)
    end

    def accept
      cleanup_stale_connections
      unless @handle.nil?
        sock = @handle.accept
        trans = ::Thrift::Socket.new(@host, @port, TIMEOUT)
        sock.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_LINGER, [5, 5].pack("ii"))
        sock.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_KEEPALIVE, false)
        trans.handle = sock
        if !@sockets.include?(trans)
          @sockets << trans
        end
        trans
      end
    end

    def close_for_reads
      # don't accept any more reads on each open socket
      @sockets.each { |sock| sock.handle.close_read if !sock.handle.nil? && !sock.handle.closed? }
    end

    def close_listen_port
      # close the listen port
      @handle.close unless @handle.nil? or @handle.closed?
    end

    def cleanup_stale_connections
      @sockets = @sockets.compact.reduce([]){ |socks, sock| socks << sock if !sock.handle.nil? && !sock.handle.closed? ; socks }
    end

    def close
     @handle.close unless @handle.nil? or @handle.closed?
     @handle = nil
    end

    def closed?
      @handle.nil? or @handle.closed?
    end

    def total_connected
      cleanup_stale_connections
      @sockets.length
    end

    alias to_io handle
  end
end