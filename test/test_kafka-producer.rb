require 'test/unit'
require 'timeout'
require 'jruby-kafka'
require 'util/kafka-producer'
require 'util/consumer'

class TestKafkaProducer < Test::Unit::TestCase
  def test_01_send_message
    topic = 'test_send'
    future = send_kafka_producer_msg topic
    assert_not_nil(future)
    begin
      Timeout.timeout(30) do
        until future.isDone() do
          next
        end
      end
    end
    assert(future.isDone(), 'expected message to be done')
    assert(future.get().topic(), topic)
    assert_equal(future.get().partition(), 0)
  end

  def test_02_send_msg_with_cb
    metadata = exception = nil
    future = send_kafka_producer_msg_cb { |md,e| metadata = md; exception = e }
    assert_not_nil(future)    
    begin
      Timeout.timeout(30) do
        while metadata.nil? && exception.nil? do
          next
        end
      end
    end
    assert_not_nil(metadata)   
    assert_instance_of(Java::OrgApacheKafkaClientsProducer::RecordMetadata, metadata)
    assert_nil(exception)
    assert(future.isDone(), 'expected message to be done')
  end

  def test_03_get_sent_msg
    topic = 'get_sent_msg'
    send_kafka_producer_msg topic
    queue = Queue.new()
    consumer = Kafka::KafkaConsumer.new(consumer_options({:topic => topic }))
    consumer.subscribe([topic])
    begin
      Timeout.timeout(30) do
        until queue.length > 0 do
          stream = consumer.poll(java.time.Duration.ofSeconds(5))
          consumer_test_blk stream, queue
          sleep 1
          next
        end
      end
    end
    consumer.close
    found = []
    until queue.empty?
      found << queue.pop
    end
    assert(found.include?('test message'), 'expected to find message: test message')
  end

  def test_04_send_message_with_ts
    topic = 'test_send'
    future = send_kafka_producer_msg_ts topic, (Time.now.to_i * 1000)
    assert_not_nil(future)
    begin
      Timeout.timeout(30) do
        until future.isDone() do
          next
        end
      end
    end
    assert(future.isDone(), 'expected message to be done')
    assert(future.get().topic(), topic)
    assert_equal(future.get().partition(), 0)
  end

  def test_05_send_msg_with_ts_and_cb
    metadata = exception = nil
    future = send_kafka_producer_msg_ts_cb(Time.now.to_i * 1000) { |md,e| metadata = md; exception = e }
    assert_not_nil(future)    
    begin
      Timeout.timeout(30) do
        while metadata.nil? && exception.nil? do
          next
        end
      end
    end
    assert_not_nil(metadata)   
    assert_instance_of(Java::OrgApacheKafkaClientsProducer::RecordMetadata, metadata)
    assert_nil(exception)
    assert(future.isDone(), 'expected message to be done')
  end

end
