"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Unit tests for the Kafka Streams example.
"""

import unittest
from unittest.mock import patch, MagicMock, call
import json
import time

class TestSimpleKafkaStreams(unittest.TestCase):

    def setUp(self):
        self.input_topic = 'input-topic'
        self.output_topic = 'output-topic'
        self.bootstrap_servers = ['localhost:9092']

    @patch('main.KafkaConsumer')
    @patch('main.KafkaProducer')
    def test_streams_initialization(self, mock_producer_class, mock_consumer_class):
        """Test streams initialization with correct configs"""
        mock_consumer = MagicMock()
        mock_producer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer

        from main import SimpleKafkaStreams
        streams = SimpleKafkaStreams(self.input_topic, self.output_topic, self.bootstrap_servers)

        # Verify consumer creation
        mock_consumer_class.assert_called_once()
        consumer_args, consumer_kwargs = mock_consumer_class.call_args
        consumer_config = consumer_args[0]
        self.assertEqual(consumer_config['bootstrap_servers'], self.bootstrap_servers)
        self.assertEqual(consumer_config['group_id'], 'streams-group')

        # Verify producer creation
        mock_producer_class.assert_called_once()
        producer_args, producer_kwargs = mock_producer_class.call_args
        producer_config = producer_args[0]
        self.assertEqual(producer_config['bootstrap_servers'], self.bootstrap_servers)

    @patch('main.KafkaConsumer')
    @patch('main.KafkaProducer')
    def test_transform_message(self, mock_producer_class, mock_consumer_class):
        """Test message transformation"""
        mock_consumer = MagicMock()
        mock_producer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer

        from main import SimpleKafkaStreams
        streams = SimpleKafkaStreams(self.input_topic, self.output_topic)

        test_data = {'id': 1, 'data': 'hello world'}
        transformed = streams.transform_message(test_data)

        self.assertEqual(transformed['data'], 'HELLO WORLD')
        self.assertIn('processed_at', transformed)
        self.assertEqual(transformed['id'], 1)

    @patch('main.KafkaConsumer')
    @patch('main.KafkaProducer')
    def test_aggregate_words(self, mock_producer_class, mock_consumer_class):
        """Test word aggregation"""
        mock_consumer = MagicMock()
        mock_producer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer

        from main import SimpleKafkaStreams
        streams = SimpleKafkaStreams(self.input_topic, self.output_topic)

        # Aggregate some text
        streams.aggregate_words('hello world hello')
        streams.aggregate_words('world test')

        expected_counts = {'hello': 2, 'world': 2, 'test': 1}
        self.assertEqual(streams.word_counts, expected_counts)

    @patch('main.KafkaConsumer')
    @patch('main.KafkaProducer')
    @patch('main.threading.Thread')
    @patch('main.time.sleep')
    def test_start_and_process_stream(self, mock_sleep, mock_thread_class, mock_producer_class, mock_consumer_class):
        """Test starting the stream processing"""
        mock_consumer = MagicMock()
        mock_producer = MagicMock()
        mock_thread = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer
        mock_thread_class.return_value = mock_thread

        # Mock message
        mock_message = MagicMock()
        mock_message.value = {'id': 1, 'data': 'test message'}
        mock_consumer.__iter__.return_value = [mock_message]

        from main import SimpleKafkaStreams
        streams = SimpleKafkaStreams(self.input_topic, self.output_topic)

        # Mock the running flag to stop after one iteration
        streams.running = True

        # Since process_stream is infinite, we need to mock it to stop
        with patch.object(streams, 'process_stream') as mock_process:
            streams.start()

            # Verify thread was started for aggregations
            mock_thread_class.assert_called_once()
            mock_thread.start.assert_called_once()
            mock_thread.daemon = True

            # Verify process_stream was called
            mock_process.assert_called_once()

    @patch('main.KafkaConsumer')
    @patch('main.KafkaProducer')
    def test_publish_aggregations(self, mock_producer_class, mock_consumer_class):
        """Test publishing aggregations"""
        mock_consumer = MagicMock()
        mock_producer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer

        from main import SimpleKafkaStreams
        streams = SimpleKafkaStreams(self.input_topic, self.output_topic)

        # Set some word counts
        streams.word_counts = {'hello': 2, 'world': 1}
        streams.running = True

        # Mock time to stop after one iteration
        with patch('main.time.sleep', side_effect=[None, KeyboardInterrupt]):
            with patch('main.time.time', return_value=1234567890):
                streams.publish_aggregations()

                # Verify producer.send was called with aggregation
                expected_aggregation = {
                    'timestamp': 1234567890,
                    'word_counts': {'hello': 2, 'world': 1},
                    'total_words': 3
                }
                mock_producer.send.assert_called_once_with('word-counts-topic', expected_aggregation)

    @patch('main.KafkaConsumer')
    @patch('main.KafkaProducer')
    def test_stop(self, mock_producer_class, mock_consumer_class):
        """Test stopping the streams"""
        mock_consumer = MagicMock()
        mock_producer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer

        from main import SimpleKafkaStreams
        streams = SimpleKafkaStreams(self.input_topic, self.output_topic)

        streams.stop()

        # Verify running flag is set to False
        self.assertFalse(streams.running)

        # Verify consumer and producer close were called
        mock_consumer.close.assert_called_once()
        mock_producer.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()