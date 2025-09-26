"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Unit tests for the Kafka management example.
"""

import unittest
from unittest.mock import patch, MagicMock

class TestKafkaManagement(unittest.TestCase):

    @patch('main.KafkaAdminClient')
    @patch('main.NewTopic')
    def test_create_topic_success(self, mock_new_topic_class, mock_admin_class):
        """Test successful topic creation"""
        mock_admin = MagicMock()
        mock_topic = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_new_topic_class.return_value = mock_topic

        from main import create_topic

        bootstrap_servers = ['localhost:9092']
        topic_name = 'test-topic'
        num_partitions = 3
        replication_factor = 1

        create_topic(bootstrap_servers, topic_name, num_partitions, replication_factor)

        # Verify NewTopic was created with correct parameters
        mock_new_topic_class.assert_called_once_with(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )

        # Verify admin client was created
        mock_admin_class.assert_called_once_with(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )

        # Verify create_topics was called
        mock_admin.create_topics.assert_called_once_with([mock_topic])

        # Verify close was called
        mock_admin.close.assert_called_once()

    @patch('main.KafkaAdminClient')
    @patch('main.NewTopic')
    def test_create_topic_with_exception(self, mock_new_topic_class, mock_admin_class):
        """Test topic creation with KafkaError"""
        mock_admin = MagicMock()
        mock_topic = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_new_topic_class.return_value = mock_topic

        # Mock create_topics to raise an exception
        from kafka.errors import KafkaError
        mock_admin.create_topics.side_effect = KafkaError("Topic creation failed")

        from main import create_topic

        bootstrap_servers = ['localhost:9092']
        topic_name = 'test-topic'

        # Should not raise exception, but print error
        create_topic(bootstrap_servers, topic_name)

        # Verify close was still called
        mock_admin.close.assert_called_once()

    @patch('main.KafkaAdminClient')
    @patch('main.NewTopic')
    def test_create_topic_default_parameters(self, mock_new_topic_class, mock_admin_class):
        """Test create_topic with default parameters"""
        mock_admin = MagicMock()
        mock_topic = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_new_topic_class.return_value = mock_topic

        from main import create_topic

        bootstrap_servers = ['localhost:9092']
        topic_name = 'default-topic'

        create_topic(bootstrap_servers, topic_name)

        # Verify NewTopic called with defaults
        mock_new_topic_class.assert_called_once_with(
            name=topic_name,
            num_partitions=3,  # default
            replication_factor=1  # default
        )

if __name__ == '__main__':
    unittest.main()