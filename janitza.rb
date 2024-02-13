require 'logger'

require 'bundler/setup'
Bundler.require

require 'async/scheduler'

MODBUS_HOST       = ENV.fetch('JANITZA_MODBUS_HOST')
MODBUS_PORT       = ENV.fetch('JANITZA_MODBUS_PORT').to_i
MODBUS_UNIT       = ENV.fetch('JANITZA_MODBUS_UNIT').to_i
HASS_MQTT_PREFIX  = ENV.fetch('JANITZA_HASS_MQTT_PREFIX', "janitza-ruby/")
HASS_MQTT         = ENV.fetch('JANITZA_HASS_MQTT', nil)
VENUS_MQTT        = ENV.fetch('JANITZA_VENUS_MQTT', nil)
VENUS_MQTT_MODE   = ENV.fetch('JANITZA_VENUS_MQTT_MODE', 'dbus-grid') # one of dbus-grid / dbus-mqtt (not yet supported)
VENUS_MQTT_GRID_TOPIC   = ENV.fetch('JANITZA_VENUS_MQTT_GRID_TOPIC', 'janitza-ruby/dbus-grid')

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def logger
  @logger ||= Logger.new(STDERR)
end

FloatReference = Struct.new(:value)

Definition = Struct.new(:metric, :unit, :type, :hass_klass, :topic, :dbus_grid) do
  def hass_name
    @hass_name ||= metric.split("_").map(&:capitalze).join
  end

  def increment(value=1.0)
    raise "type must be total_increasing but is #{type.inspect}" if type != "total_increasing"

    @fr ||= FloatReference.new(0.0)
    @fr.value += value
    Metric.new(self, @fr.value, Time.now) 
  end
end

Registers = [
  #  metric_name                    unit   type              hass_class       (VE) mqtt           VE dbus-grid
  %w(voltage_l1_n                      V   measurement       voltage          Ac/L1/Voltage       grid.L1.voltage grid.voltage),
  %w(voltage_l2_n                      V   measurement       voltage          Ac/L2/Voltage       grid.L2.voltage),
  %w(voltage_l3_n                      V   measurement       voltage          Ac/L3/Voltage       grid.L3.voltage),

  %w(voltage_l1_l2                     V   measurement       voltage          -Ac/L1-L2/Voltage),
  %w(voltage_l2_l3                     V   measurement       voltage          -Ac/L2-L3/Voltage),
  %w(voltage_l1_l3                     V   measurement       voltage          -Ac/L1-L3/Voltage),

  %w(current_l1                        A   measurement       current          Ac/L1/Current       grid.L1.current),
  %w(current_l2                        A   measurement       current          Ac/L2/Current       grid.L2.current),
  %w(current_l3                        A   measurement       current          Ac/L3/Current       grid.L3.current),
  %w(current_total                     A   measurement       current          Ac/Current          grid.current),

  %w(real_power_l1                     W   measurement       power            Ac/L1/Power         grid.L1.power),
  %w(real_power_l2                     W   measurement       power            Ac/L2/Power         grid.L2.power),
  %w(real_power_l3                     W   measurement       power            Ac/L3/Power         grid.L3.power),
  %w(real_power_total                  W   measurement       power            Ac/Power            grid.power),

  %w(apparent_power_l1                 VA  measurement       apparent_power   -Ac/L1/ApparentPower),
  %w(apparent_power_l2                 VA  measurement       apparent_power   -Ac/L2/ApparentPower),
  %w(apparent_power_l3                 VA  measurement       apparent_power   -Ac/L3/ApparentPower),
  %w(apparent_power_total              VA  measurement       apparent_power   -Ac/ApparentPower),

  %w(reactive_power_l1                 var measurement       reactive_power   -Ac/L1/ReactivePower),
  %w(reactive_power_l2                 var measurement       reactive_power   -Ac/L2/ReactivePower),
  %w(reactive_power_l3                 var measurement       reactive_power   -Ac/L3/ReactivePower),
  %w(reactive_power_total              var measurement       reactive_power   -Ac/ReactivePower),

  %w(power_factor_l1                   -   measurement       power_factor     -Ac/L1/PowerFactor),
  %w(power_factor_l2                   -   measurement       power_factor     -Ac/L2/PowerFactor),
  %w(power_factor_l3                   -   measurement       power_factor     -Ac/L3/PowerFactor),

  %w(frequency                         Hz  measurement       frequency        Ac/Frequency        grid.L1.frequency grid.L2.frequency grid.L3.frequency),

  %w(rotation_field                    -   measurement       -                -),

  %w(real_energy_l1_total              Wh  total_increasing  energy           -Ac/L1/Energy),
  %w(real_energy_l2_total              Wh  total_increasing  energy           -Ac/L2/Energy),
  %w(real_energy_l3_total              Wh  total_increasing  energy           -Ac/L3/Energy),
  %w(real_energy_total                 Wh  total_increasing  energy           -Ac/Energy),

  %w(real_energy_l1_consumed_total     Wh  total_increasing  energy           Ac/L1/Energy/Reverse    grid.L1.energy_reverse),
  %w(real_energy_l2_consumed_total     Wh  total_increasing  energy           Ac/L2/Energy/Reverse    grid.L2.energy_reverse),
  %w(real_energy_l3_consumed_total     Wh  total_increasing  energy           Ac/L3/Energy/Reverse    grid.L3.energy_reverse),
  %w(real_energy_consumed_total        Wh  total_increasing  energy           Ac/Energy/Reverse       grid.energy_reverse),

  %w(real_energy_l1_delivered_total    Wh  total_increasing  energy           Ac/L1/Energy/Forward    grid.L1.energy_forward),
  %w(real_energy_l2_delivered_total    Wh  total_increasing  energy           Ac/L2/Energy/Forward    grid.L2.energy_forward),
  %w(real_energy_l3_delivered_total    Wh  total_increasing  energy           Ac/L3/Energy/Forward    grid.L3.energy_forward),
  %w(real_energy_delivered_total       Wh  total_increasing  energy           Ac/Energy/Forward       grid.energy_forward),
].map { Definition.new(*_1[0..4], _1[5..-1].to_a) }

Metrics = [
  %w(collecting_registers_seconds_total  s  total_increasing  - -),
  %w(collecting_registers_count_total    -  total_increasing  - -),
].to_h{ [_1[0], Definition.new(*_1) ]}

VEUnitTransformer = { 
  %r(read_energy_.*) => ->(value) { value / 1000 }
}

$cache = Concurrent::Hash.new

Metric = Struct.new(:definition, :value, :timestamp) do
  def cache
    $cache[definition.metric] ||= {}
  end

  def update!
    cache.merge!("metric" => self)
  end

  def publish(mqttc, type: nil, venus_hash: nil)
    return if definition.topic == "-"

    if type == :venus
      return if definition.topic.to_s.start_with? "-"

      new_value = value
      transformer = VEUnitTransformer.find do |regex, _proc|
        regex.match?(definition.metric) 
      end&.last
      new_value = transformer.call(value) if transformer

      if VENUS_MQTT == "dbus-mqtt"
        mqttc.publish(definition.topic, new_value.to_s)
      end

      if venus_hash && VENUS_MQTT_MODE == "dbus-grid"
        definition.dbus_grid.to_a.each do |variable|
          variable.split(".").inject(venus_hash) do |ret, key|
            ret[key] = new_value if ret[key].nil?
            ret[key]
          end
        end
      end

      return
    end

    mqttc.publish(HASS_MQTT_PREFIX + definition.topic.gsub(/^-/,''), value.to_s)
  end

  def prometheus_type
    return :gauge if definition.type == "measurement"
    return :counter if definition.type == "total_increasing"

    raise "type #{definition.type.inspect} invalid, unable to convert to prometheus type"
  end

  def to_hass(mqtt)
    return if unit == "-"
    return if hass_klass == "-" 

    if cache.key?("hass_informed_at")
      mqtt.publish() # config
    end
  end
end

if HASS_MQTT
  @hass_mqtt = MQTT::Client.connect(HASS_MQTT)
end

if HASS_MQTT && (HASS_MQTT == VENUS_MQTT)
  @venus_mqtt = @hass_mqtt
elsif VENUS_MQTT
  @venus_mqtt = MQTT::Client.connect(VENUS_MQTT)
end


Thread.new do
  Fiber.set_scheduler(Async::Scheduler.new)

  duration = nil
  modbus_pre_open = now
  ModBus::TCPClient.new(MODBUS_HOST, MODBUS_PORT) do |client|
    client.with_slave(MODBUS_UNIT) do |unit|
      loop do
        venus_hash = Concurrent::Hash.new
        venus_hash["grid"] = Concurrent::Hash.new
        venus_hash["grid"]["L1"] = Concurrent::Hash.new
        venus_hash["grid"]["L2"] = Concurrent::Hash.new
        venus_hash["grid"]["L3"] = Concurrent::Hash.new

        read_registers = now
        registers = unit.query("\x3"+19000.to_word + (Registers.size*2).to_word).unpack("g*")
        real_time = Time.now
        registers = registers.map.with_index do |value, index|
          Metric.new(Registers[index], value, real_time).tap do |metric|
            Fiber.schedule { metric.update! }
            Fiber.schedule { metric.publish(@hass_mqtt) }
            metric.publish(@venus_mqtt, venus_hash: venus_hash, type: :venus)
          end
        end
        duration = now - read_registers
        Metrics["collecting_registers_seconds_total"].increment(duration).update!
        Metrics["collecting_registers_count_total"].increment.update!

        if duration > 0.1
          if VENUS_MQTT_MODE == "dbus-grid"
            Fiber.schedule { @venus_mqtt.publish(VENUS_MQTT_GRID_TOPIC, Oj.dump(venus_hash, mode: :compat)) }
          end
          next
        end

        sleep(0.1-duration)
        if VENUS_MQTT_MODE == "dbus-grid"
          Fiber.schedule { @venus_mqtt.publish(VENUS_MQTT_GRID_TOPIC, Oj.dump(venus_hash, mode: :compat)) }
        end
      end
    end
  end&.close

  logger.info "Fetching registers took: #{duration}s"
rescue StandardError => err
  logger.error "Terminating collector loop!"
  logger.error err
  exit 1
end
