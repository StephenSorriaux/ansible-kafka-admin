"""
This module provides classes and functions for tests
"""

import time

from kafka.client_async import KafkaClient
from kafka.protocol.types import (
    Array, Boolean, Int8, Int16, Int32, Schema, String
)
from kafka.protocol.api import Request, Response
from kafka.protocol.admin import DescribeAclsRequest_v0


class DescribeConfigsResponseV0(Response):
    """
    DescribeConfigs in Kafka Protocol
    Response serialization
    """
    API_KEY = 32
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('resources', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_entries', Array(
                ('config_name', String('utf-8')),
                ('config_value', String('utf-8')),
                ('read_only', Boolean),
                ('is_default', Boolean),
                ('is_sensitive', Boolean)))))
    )


class DescribeConfigsRequestV0(Request):
    """
    DescribeConfigs in Kafka Protocol
    Request serialization
    """
    API_KEY = 32
    API_VERSION = 0
    RESPONSE_TYPE = DescribeConfigsResponseV0
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_names', Array(String('utf-8')))))
    )


class KafkaManager(object):
    """
    Easier access to Kafka information
    """

    TOPIC_RESOURCE_ID = 2
    MAX_POLL_RETRIES = 3
    MAX_RETRY = 10
    SUCCESS_CODE = 0

    def __init__(self, **configs):
        self.client = KafkaClient(**configs)
        self.refresh()

    def refresh(self):
        """
        Refresh topics state
        """
        fut = self.client.cluster.request_update()
        self.client.poll(future=fut)
        if not fut.succeeded():
            self.close()
            self.module.fail_json(
                msg='Error while updating topic state from Kafka server: %s.'
                % fut.exception
            )

    def close(self):
        """
        Closes the client. Must be called once
        the client is not used anymore.
        """
        self.client.close()

    def get_controller(self):
        """
        Return the current controller for cluster.
        """
        node_id, _host, _port, _rack = self.client.cluster.controller
        return node_id

    def get_topics(self):
        """
        Returns the topics list
        """
        return self.client.cluster.topics()

    def get_total_partitions_for_topic(self, topic):
        """
        Returns the number of partitions for topic
        """
        return len(self.client.cluster.partitions_for_topic(topic))

    def get_partitions_metadata_for_topic(self, topic):
        """
        Returns set of partition for topic
        """
        return self.client.cluster._partitions[topic]

    def get_config_for_topic(self, topic_name, config_name):
        """
        Returns value for config_name topic option
        """
        request = DescribeConfigsRequestV0(
            resources=[(self.TOPIC_RESOURCE_ID, topic_name, [config_name])]
        )
        response = self.send_request_and_get_response(request)
        for err_code, err_message, _, _, config_entries in response.resources:
            if err_code != self.SUCCESS_CODE:
                raise Exception(err_message)
            for _, value, _, _, _ in config_entries:
                return value

    def describe_acls(self, acl_resource):
        """Describe a set of ACLs
        """

        request = DescribeAclsRequest_v0(
            resource_type=acl_resource['resource_type'],
            resource_name=acl_resource['name'],
            principal=acl_resource['principal'],
            host=acl_resource['host'],
            operation=acl_resource['operation'],
            permission_type=acl_resource['permission_type']
        )

        response = self.send_request_and_get_response(request)

        if response.error_code == self.SUCCESS_CODE:
            return response.resources

        return None

    def get_awaiting_request(self):
        """
        Returns the number of requests currently in the queue
        """
        return self.client.in_flight_request_count()

    def connection_check(self, node_id, connection_sleep=1):
        """
        Checks that connection with broker is OK and that it is possible
        to send requests
        Since the _maybe_connect() function used in ready() is 'async',
        we need to manually call it several time to make the connection
        """
        retries = 0
        if not self.client.ready(node_id):
            while retries < self.MAX_RETRY:
                self.client.poll()
                if self.client.ready(node_id):
                    return True
                time.sleep(connection_sleep)
                retries += 1
            return False
        return True

    def send_request_and_get_response(self, request):
        """
        Send requet and get associated response
        """
        try:
            node_id = self.get_controller()
        except Exception:
            raise
        if self.connection_check(node_id):
            future = self.client.send(node_id, request)
            self.client.poll(future=future)
            if future.succeeded():
                return future.value
            else:
                raise future.exception

        return None
