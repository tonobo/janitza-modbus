require_relative 'janitza'

app = Rack::Builder.new do # rubocop:disable Metrics/BlockLength
  map '/metrics' do
    block = lambda do |_env|
      status = 200
      metrics = $cache.flat_map do |metric_name, value| 
        metric = value["metric"]
        [
          "# HELP janitza_#{metric_name} unit #{metric.definition.unit}",
          "# TYPE janitza_#{metric_name} #{metric.prometheus_type}",
          "janitza_#{metric_name} #{metric.value.to_f} #{(metric.timestamp.to_f * 1000).to_i}"
        ]
      end
      [
        status,
        { 'Content-Type' => 'text/plain' },
        StringIO.new(metrics.join("\n"))
      ]
    end

    run block
  end
end.to_app

run app
