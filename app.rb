require 'logger'
require 'heroku-log-parser'
require_relative './queue_io.rb'
require_relative ENV.fetch("WRITER_LIB", "./writer/s3.rb") # provider of `Writer < WriterBase` singleton

class App

  PREFIX = ENV.fetch("FILTER_PREFIX", "")
  PREFIX_LENGTH = PREFIX.length
  LOG_REQUEST_URI = ENV['LOG_REQUEST_URI']

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc do |severity, datetime, progname, msg|
       "[app #{$$} #{Thread.current.object_id}] #{msg}\n"
    end
    @logger.info "initialized"
  end

  def call(env)
    lines = if LOG_REQUEST_URI
      [{ msg: env['REQUEST_URI'], ts: '' }]
    else
      #HerokuLogParser.parse(env['rack.input'].read).collect { |m| { msg: m[:message], ts: m[:emitted_at].strftime('%Y-%m-%dT%H:%M:%S.%L%z') } }
      # 2021-02-08T23:44:58.245+0000 at=info method=GET path="/" host=sw-v3-staging.herokuapp.com request_id=f0333f93-441b-4f95-abbd-3ff1bff74233 fwd="141.157.66.153" dyno=web.1 connect=1ms service=12ms status=200 bytes=345 protocol=https

      # no implicit conversion of Symbol into Integer
      #HerokuLogParser.parse(env['rack.input'].read).collect {|m| "#{m[:emitted_at]} #{m[:proc_id]} #{m[:msg_id]} #{m[:message]}" }

      # HerokuLogParser.parse(env['rack.input'].read).collect {|m| "#{m[:emitted_at]} #{m[:proc_id]} #{m[:message]}" }
      # 2021-02-09 00:18:48 UTC router at=info method=GET path="/" host=sw-v3-staging.herokuapp.com request_id=30163397-f8f0-448b-89c7-c0af88c60ccc fwd="141.157.66.153" dyno=web.1 connect=1ms service=3ms status=200 bytes=345 protocol=https

      HerokuLogParser.parse(env['rack.input'].read).collect { |m| { 
        msg: m[:message], 
        ts: m[:emitted_at].strftime('%Y-%m-%dT%H:%M:%S.%L%z'), 
        procId: m[:proc_id], 
        appName: m[:appName] 
      } }
    end

    lines.each do |line|
      # @logger.info "line = #{line}"
      msg = line[:msg]
      next unless msg.start_with?(PREFIX)
      Writer.instance.write([line[:ts], line[:appName], line[:procId], msg[PREFIX_LENGTH..-1]].join(' ').strip) # WRITER_LIB
    end

  rescue Exception
    @logger.error $!
    @logger.error $@

  ensure
    return [200, { 'Content-Length' => '0' }, []]
  end

end
