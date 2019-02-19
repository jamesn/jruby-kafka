DEFAULT_CLIENT_OPTIONS = {
    :bootstrap_servers => '127.0.0.1:9092',
    :key_deserializer => 'org.apache.kafka.common.serialization.StringDeserializer',
    :value_deserializer => 'org.apache.kafka.common.serialization.StringDeserializer',
    :group_id => 'test',
    :topic => 'test',
    :auto_offset_reset => 'earliest'
}

def consumer_options(opt_override = nil)
  if opt_override.nil?
    DEFAULT_CLIENT_OPTIONS
  else
    DEFAULT_CLIENT_OPTIONS.merge(opt_override)
  end
end

def filter_consumer_options(opt_override = nil)
  options = consumer_options opt_override
  options.delete(:topic)
  options
end


def consumer_test_blk(stream, queue)
  it = stream.iterator
  begin
    while it.hasNext do
      queue << it.next.value.to_s
    end
  rescue Exception => e
    pp e
    sleep 1
    retry
  end
end
