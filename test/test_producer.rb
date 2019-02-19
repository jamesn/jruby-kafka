require 'test/unit'
require 'jruby-kafka'
require 'timeout'
require 'util/producer'
require 'util/consumer'

require 'pp'

class TestKafka < Test::Unit::TestCase
  def test_01_run
    topic = 'test_run'
    send_test_messages topic
    queue = Queue.new()
    consumer = Kafka::KafkaConsumer.new(consumer_options({:topic => topic }))
    consumer.subscribe([topic])
    begin
      Timeout.timeout(30) do
        until queue.length > 3 do
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
    assert_equal([ "codec gzip test message",
                   "codec none test message",
                   "codec snappy test message",
                   "test message" ],
                 found.map(&:to_s).uniq.sort,)
  end

  def test_02_from_beginning
    topic = 'test_run'
    queue = Queue.new()
    options = {
      :topic => topic,
      :auto_offset_reset => 'earliest'
    }
    consumer = Kafka::KafkaConsumer.new(consumer_options(options))
    tp_list = consumer.partitionsFor(topic).map{|pi| org.apache.kafka.common.TopicPartition.new(topic, pi.partition())}
    consumer.assign(tp_list)
    consumer.seekToBeginning(tp_list)
    begin
      Timeout.timeout(30) do
        until queue.length > 3 do
          stream = consumer.poll(java.time.Duration.ofSeconds(5))
          consumer_test_blk stream, queue
          sleep 1
          pp queue.length
          next
        end
      end
    end
    consumer.close
    found = []
    until queue.empty?
      found << queue.pop
    end
    assert_equal([ "codec gzip test message",
                   "codec none test message",
                   "codec snappy test message",
                   "test message" ],
                 found.map(&:to_s).uniq.sort)
  end
end
