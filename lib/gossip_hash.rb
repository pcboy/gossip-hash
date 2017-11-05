require "gossip_hash/version"

require 'eventmachine'
require 'socket'
require 'json'
require 'msgpack/rpc'

module GossipHash

  class GossipHash < Hash
    Thread.abort_on_exception = true

    attr_accessor :peers

    module PortHelper
      def self.find_port
        s = TCPServer.new('127.0.0.1', 0)
        s.addr[1]
      ensure
        s.close
      end
    end


    class GossipServer
      def initialize(gossip_hash)
        @gossip_hash = gossip_hash
      end

      def fetch(server_info, peers)
        @gossip_hash.peers |= peers
        @gossip_hash << server_info
        {data: @gossip_hash, peers: @gossip_hash.peers}
      end

      def gossip(hash)
        @gossip_hash.add_data(hash)
      end
    end

    def start(port: nil)
      @peers = []

      Thread.new do
        server = MessagePack::RPC::Server.new
        server_port = port || PortHelper.find_port
        server.listen('0.0.0.0', server_port, GossipServer.new(self))
        puts "listening on 0.0.0.0:#{server_port}"
        @server_info = "0.0.0.0:#{server_port}"
        server.run
      end

      Thread.new do
        loop do
          sleep 3
          sync
        end
      end
    end

    def <<(peer)
      @peers << peer unless peer == @server_info
    end

    def add_data(hash)
      hash.each do |k,v|
        self[k] = v
      end
    end

    def sync
      peers = @peers
      peers.map{|x| x.split(':')}.each do |host, port|
        if result = rpc_call(host, port, :fetch, @server_info, @peers)
          @peers = (@peers | result['peers']) - [@server_info]
          result['data'].each do |k,v|
            #TODO: Check version
            self[k] = v
          end
        end
      end
    end

    def []=(key, val)
      super
      @peers.map{|x| x.split(':')}.each do |host, port|
        rpc_call(host, port, :gossip, {key => val})
      end
    end

    private
    def rpc_call(host, port, method, *params)
      begin
        client = MessagePack::RPC::Client.new(host, port)
        result = client.call(method.to_sym, *params)
      rescue MessagePack::RPC::ConnectionTimeoutError, MessagePack::RPC::TimeoutError
        @peers.delete "#{host}:#{port}"
        nil
      end
    end
  end

end
