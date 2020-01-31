from kafka.protocol.commit import OffsetFetchRequest_v2, OffsetFetchResponse_v2, GroupCoordinatorRequest_v0, GroupCoordinatorResponse_v0
from kafka.protocol.offset import OffsetRequest_v1, OffsetResponse_v1

_OffsetRequest = OffsetRequest_v1
_OffsetResponse = OffsetResponse_v1

_OffsetFetchRequest = OffsetFetchRequest_v2
_OffsetFetchResponse = OffsetFetchResponse_v2

_GroupCoordinatorRequest = GroupCoordinatorRequest_v0
_GroupCoordinatorResponse = GroupCoordinatorResponse_v0

# Time value '-1' is to get the offset for next new message (=> last offset)
LATEST_TIMESTAMP = -1

# Fetch group as consumer 
AS_CONSUMER_ID = -1


class KafkaConsumerLag:

    def __init__(self, kafka_client):

        self.client = kafka_client
        self.client.check_version()

    def _send(self, broker_id, request, response_type=None):

        f = self.client.send(broker_id, request)
        response = self.client.poll(future=f)

        if response_type:
            if response and len(response) > 0:
                for r in response:
                    if isinstance(r, response_type):
                        return r
        else:
            if response and len(response) > 0:
                return response[0]

        return None

    def get_lag_stats(self, consumer_group=None):
        cluster = self.client.cluster
        brokers = cluster.brokers()

        # coordinating broker
        consumer_coordinator = {}
        
        # Current offset for each topic partition
        current_offsets = {}

        # Topic consumed by this consumer_group
        topics = []

        # Global lag 
        global_lag = 0

        # result object containing kafka statistics
        results = {}

        # Ensure connections to all brokers
        for broker in brokers:
            while not self.client.is_ready(broker.nodeId):
                self.client.ready(broker.nodeId)
        
        # Identify which broker is coordinating this consumer group
        response = self._send(next(iter(brokers)).nodeId, _GroupCoordinatorRequest(consumer_group), _GroupCoordinatorResponse)
        if response:
            consumer_coordinator = response.coordinator_id

        # Get current offset for each topic partitions
        response = self._send(consumer_coordinator, _OffsetFetchRequest(consumer_group, None), _OffsetFetchResponse)
        for topic_partition in response.topics:
            topic = topic_partition[0]
            if topic not in topics:
                current_offsets[topic] = []        
                topics.append(topic)
            for partition in topic_partition[1]:
                current_offsets[topic].append((partition[0], partition[1])) # partition[0]=> partition number, partition[1]=> partition current offset

        
        # Get last offset for each topic partition coordinated by each broker
        # Result object is set up also 
        for broker in brokers:
            # filter only topic consumed by consumer_group
            topics_partitions_by_broker = filter_by_topic(cluster.partitions_for_broker(broker.nodeId), topics)
            request_topic_partitions = build_offset_request_topics_partitions(topics_partitions_by_broker)            
            
            response = self._send(consumer_coordinator, _OffsetRequest(AS_CONSUMER_ID, request_topic_partitions), _OffsetResponse)
            if response:
                for topics_partitions in response.topics:
                    topic  = topics_partitions[0]
                    partitions = topics_partitions[1]
                    if topic not in results:
                        results[topic] = {}
                    for partition in partitions:
                        partition_id = partition[0]
                        last_offset = partition[3]
                        current_offset = current_offsets[topic][partition_id][1]
                        lag = last_offset - current_offset
                        global_lag +=  lag
                        # Set up result object
                        results[topic][partition_id] = {
                            'current_offset': current_offset,
                            'last_offset': last_offset,
                            'lag' : last_offset - current_offset
                        }   

        results["global_lag_count"] = global_lag
        return results


def filter_by_topic(topics_partitions, topics):
    result = []
    for topic_partition in topics_partitions:
        if topic_partition.topic in topics:
            result.append(topic_partition)
    
    return result


def build_offset_request_topics_partitions(topics_partitions):
    _topics_partitions = {}
    for topic_partition in topics_partitions:
        topic = topic_partition.topic
        partition = topic_partition.partition
        if topic not in _topics_partitions:
            _topics_partitions[topic_partition.topic] = []
        _topics_partitions[topic_partition.topic].append((partition, LATEST_TIMESTAMP))

    # convert to array for _OffsetRequest Struct
    request_topics_partitions = []
    for tp in _topics_partitions.items():
        request_topics_partitions.append(tp)

    return request_topics_partitions