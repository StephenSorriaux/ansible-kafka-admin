import json
import time
import uuid
import sys
import types

if sys.version_info >= (3, 12, 0):
    m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
    setattr(m, 'range', range)
    sys.modules['kafka.vendor.six.moves'] = m

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

testinfra_hosts = ["ansible://executors"]


# ---------------
# Common (TO BE SHARED)
# ---------------
def get_bootstrap_servers(host, listener_name='PLAINTEXT'):
    host_vars = host.ansible.get_variables()
    return host_vars['bootstrap_servers'][listener_name.lower()]


def make_args(args_dict):
    return json.dumps(args_dict)


def execute_kafka_info(host, resource, check=False):

    module_args = {
        'resource': resource,
    }

    module_args.update({
        'bootstrap_servers': get_bootstrap_servers(host)
    })

    return host.ansible('kafka_info', make_args(module_args),
                        check=check)


def produce_messages(topic, messages, **config):
    correlation_id = bytes(str(uuid.uuid4()), 'utf-8')

    producer = KafkaProducer(**config)

    record_metas = []
    record_headers = [('correlation_id', correlation_id)]

    for message in messages:
        fut = producer.send(
            topic, bytes(message, 'utf-8'), headers=record_headers)
        record_metas.append(fut.get())

    producer.flush()
    producer.close()

    return correlation_id, record_metas


def consume_messages(topic, topic_partitions=1,
                     max_messages=1, max_wait_timeout=120,
                     poll_timeout_ms=100, poll_max_records=10,
                     from_beginning=False, record_filter=None, **config):

    consumer = KafkaConsumer(**config)

    # manually assign topic partitions
    consumer.assign([
        TopicPartition(topic, partition)
        for partition in range(0, topic_partitions)
    ])

    if from_beginning:
        consumer.seek_to_beginning()

    messages = []
    start_ts = time.time()  # expressed in seconds
    while True:
        if len(messages) >= max_messages \
           or (time.time() - start_ts) > max_wait_timeout:
            break

        batch = consumer.poll(
            timeout_ms=poll_timeout_ms, max_records=poll_max_records)

        for subscription, records in batch.items():
            for record in filter(record_filter, records):
                messages.append(record.value.decode('utf-8'))

    return messages


# ---------------
# Actual user test
# ---------------
def test_user_info_should_return_created_users(host):

    kafka_user_info = execute_kafka_info(host, 'user')

    assert kafka_user_info['ansible_module_results'] is not None
    assert kafka_user_info['ansible_module_results']['users'] is not None

    users = kafka_user_info['ansible_module_results']['users']

    assert users['alice'] is not None
    assert users['alice'][0]['iterations'] == 4096
    assert users['alice'][0]['mechanism'] == "SCRAM-SHA-512"

    assert users['bob'] is not None
    assert users['bob'][0]['iterations'] == 5000
    assert users['bob'][0]['mechanism'] == "SCRAM-SHA-256"


def test_client_should_be_able_to_write_and_read_messages(host):
    topic = 'scram-test-topic'

    messages = [
        'Test Message #1',
        'Test Message #2',
        'Test Message #3'
    ]

    bootstrap_servers = get_bootstrap_servers(
        host, listener_name='OUTSIDE_SASL_PLAINTEXT')

    config = {
        'bootstrap_servers': bootstrap_servers,
        'security_protocol': 'SASL_PLAINTEXT',
        'sasl_mechanism': 'SCRAM-SHA-512',
        'sasl_plain_username': 'alice',
        'sasl_plain_password': 'changeit'
    }

    produce_messages(topic, messages, **config)
    actual_messages = consume_messages(topic, from_beginning=True,
                                       max_messages=3, **config)

    print(actual_messages)

    assert set(actual_messages) == set(messages)
