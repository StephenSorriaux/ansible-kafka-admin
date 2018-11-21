import time

from kafka.client import KafkaClient
from kafka.protocol.types import Array, Boolean, Int8, Int16, Int32, Schema, String
from kafka.protocol.api import Request, Response


class DescribeConfigsResponse_v0(Response):
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


class DescribeConfigsRequest_v0(Request):
    API_KEY = 32
    API_VERSION = 0
    RESPONSE_TYPE = DescribeConfigsResponse_v0
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_names', Array(String('utf-8')))))
    )


class KafkaManager(object):

    TOPIC_RESOURCE_ID = 2
    MAX_POLL_RETRIES = 3
    MAX_RETRY = 10
    SUCCESS_CODE = 0


    def __init__(self, **configs):
        self.client = KafkaClient(**configs)


    def close(self):
        self.client.close()


    def get_controller(self):
        nodeId, host, port, rack = self.client.cluster.controller
        return nodeId


    # Returns the topics list
    def get_topics(self):
        return self.client.cluster.topics()


    # Returns the number of partitions for topic
    def get_total_partitions_for_topic(self, topic):
        return len(self.client.cluster.partitions_for_topic(topic))


    def get_partitions_metadata_for_topic(self, topic):
        return self.client.cluster._partitions[topic]

    def get_config_for_topic(self, topic_name, config_name):
        request = DescribeConfigsRequest_v0(
            resources=[(self.TOPIC_RESOURCE_ID,topic_name,[config_name])]
        )
        resp = self.send_request_and_get_response(request)
        for r in resp:
            for err_code, err_message, _, _, config_entries in r.resources:
                if (err_code != self.SUCCESS_CODE):
                    raise Exception(err_message)
                for _, value, _, _, _ in config_entries:
                    return value


    # Returns the number of requests currently in the queue
    def get_awaiting_request(self):
        return self.client.in_flight_request_count()


    # Obtains response from server using poll()
    # It may need some times to get the response, so we had some retries
    def get_responses_from_client(self, connection_sleep = 1):
        retries = 0
        if self.get_awaiting_request() > 0:
            while retries < self.MAX_POLL_RETRIES:
                resp = self.client.poll()
                if len(resp) > 0:
                    return resp
                time.sleep(connection_sleep)
                retries += 1
            self.close()
        else:
            self.close()

    # Checks that connection with broker is OK and that it is possible
    # to send requests
    # Since the _maybe_connect() function used in ready() is 'async',
    # we need to manually call it several time to make the connection
    def connection_check(self, node_id, connection_sleep = 1):
        retries = 0
        if not self.client.ready(node_id):
            while retries < self.MAX_RETRY:
                if self.client.ready(node_id):
                    return True
                time.sleep(connection_sleep)
                retries += 1
            return False
        else:
            return True

    def send_request_and_get_response(self, request):
        try:
            node_id = self.get_controller()
        except Exception:
            raise
        if self.connection_check(node_id):
            update = self.client.send(node_id,request)
            if update.is_done and update.failed():
                self.close()
            return self.get_responses_from_client()
        else:
            self.close()