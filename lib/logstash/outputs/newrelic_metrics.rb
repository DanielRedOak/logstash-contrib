# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "stud/buffer"


# This output lets you send metrics to
# New Relic based on Logstash events.
# Input will show up as a New Relic Plugin
# https://docs.newrelic.com/docs/plugin-dev/how-new-relic-works-with-plugin-data

# Default queue_size and poll_time are set according to New Relic recommendations.

class LogStash::Outputs::NewRelicMetrics < LogStash::Outputs::Base

  include Stud::Buffer

  config_name "newrelic_metrics"
  milestone 1

  # Your New Relic License key.
  config :license_key, :validate => :string, :required => true

  # The hostname of the agent that is reporting the metric to New Relic
  config :agent_host, :validate => :string, :default => 60

  # The pid of the process that is reporting the metric to New Relic
  config :agent_pid, :validate => :number, :default => 1234

  # Version of the agent that is reporting the metric to New Relic
  config :agent_version, :validate => :number, :default => 1

  # The name of the time series.
  # https://docs.newrelic.com/docs/plugin-dev/metric-naming-reference
  config :metric_hash, :validate => :hash

  # The name of the component that produced the metric. This shows in New Relic.
  # A suggestion is to use the application or app component name.
  config :component_name, :validate => :string, :default => "%{metric_component}"

  # The name of our 'plugin'. Only the text after the last period shows in New Relic.
  # MUST be unique across the New Relic platform. eg. com.yourcompany.plugin_name
  config :component_guid, :validate => :string

  # How many events to queue before flushing to New Relic
  # prior to schedule set in @poll_time.  No more than 500 per New Relic
  config :queue_size, :validate => :number, :default => 500

  # How often (in seconds) to flush queued events to New Relic.  Recommended 60s by New Relic
  config :poll_time, :validate => :number, :default => 60

  # The duration in seconds over which the metric data was collected. 
  config :duration, :validate => :number, :default => 60

  public
  def register
    require 'time'
    require "net/https"
    require "uri"

    @url = "https://platform-api.newrelic.com/platform/v1/metrics"
    @uri = URI.parse(@url)
    @client = Net::HTTP.new(@uri.host, @uri.port)
    @client.use_ssl = true
    @client.verify_mode = OpenSSL::SSL::VERIFY_NONE
    @logger.debug("Client", :client => @client.inspect)
    buffer_initialize(
      :max_items => @queue_size,
      :max_interval => @poll_time,
      :logger => @logger
    )
  end # def register

  public
  def receive(event)
    return unless output?(event)
    return unless @agent_host && @agent_version && @component_name && @component_guid && @poll_time && @metric_hash
    return unless event.sprintf(@queue_size) <= 500

    #Get data for the 'metrics' hash
    nr_metric_hash = Hash[*@metric_hash.collect{|k,v| [event.sprintf(k),event.sprintf(v)]}.flatten]

    #Setup the rest of the components hash
    nr_component = Hash.new
    nr_component['name'] = event.sprintf(@component_name)
    nr_component['guid'] = event.sprintf(@component_guid)
    nr_component['duration'] = event.sprintf(@duration) #might want to update this if it is sent based on q size
    nr_component['metrics'] = nr_metric_hash
    @logger.warn("New Relic metric_hash", :data => nr_metric_hash)

    @logger.info("Queueing metric component", :event => nr_component)
    buffer_receive(nr_component)
  end # def receive

  public
  def flush(events, final=false)
    #Prep to flush

    #Setup the first hash for 'agent'
    nr_agent_hash = Hash.new
    nr_agent_hash['host'] = event.sprintf(@agent_host)
    nr_agent_hash['pid'] = event.sprintf(@agent_pid)
    nr_agent_hash['version'] = event.sprintf(@agent_version)

    #Setup the overall hash
    nr_overall = Hash.new
    nr_overall['agent'] = nr_agent_hash
    nr_overall['components'] = []

    #Add the components to the array
    events.each do |event|
      begin
        nr_overall['components'] << event
      rescue
        @logger.warn("Error adding component to array!", :exception => e)
        next
      end
    end

    request = Net::HTTP::Post.new(@uri.path)

    begin
      request.body = nr_overall.to_json
      request.add_field("X-License-Key", event.sprintf(@license_key))
      request.add_field("Content-Type", 'application/json')
      request.add_field("Accept", 'application/json')
      response = @client.request(request)
      @logger.info("New Relic Metric connection", :request => request.inspect, :response => response.inspect)
      raise unless response.code == '200'
    rescue Exception => e
      @logger.warn("Unhandled exception", :request => request.inspect, :response => response.inspect, :exception => e.inspect)
    end
  end # def flush

end # class LogStash::Outputs::NewRelicMetrics