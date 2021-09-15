"""
Main tests for library
"""

import os
import time

from pkg_resources import parse_version

import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    get_consumer_group,
    get_topic_name,
    cg_defaut_configuration,
    topic_defaut_configuration,
    sasl_default_configuration,
    ensure_kafka_topic,
    check_unconsumed_topic,
    ensure_kafka_consumer_group,
    produce_and_consume_topic,
    ensure_idempotency,
    host_protocol_version
)

runner = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE'])
testinfra_hosts = runner.get_hosts('executors')

kafka_hosts = dict()
for host in testinfra.get_hosts(
    ['kafka1'],
    connection='ansible',
    ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
):
    kafka_hosts[host] = host.ansible.get_variables()


def test_delete_consumer_offset(host):
    delete_consumer_offset(host)


def test_delete_consumer_offset_partitions(host):
    delete_consumer_offset(host, [0])


def delete_consumer_offset(host, partitions=None):
    """
    Check if can delete consumer group for topic
    """
    # Given
    consumer_group = get_consumer_group()
    topic_name1 = get_topic_name()

    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name1,
        minimal_api_version="2.4.0"
    )
    time.sleep(0.3)

    produce_and_consume_topic(topic_name1, 1, consumer_group, True, "2.4.0")
    time.sleep(0.3)

    # When
    test_cg_configuration = cg_defaut_configuration.copy()

    test_cg_configuration.update({
        'consumer_group': consumer_group,
        'action': 'delete',
        'api_version': '2.4.0'
    })

    if partitions is None:
        test_cg_configuration.update({'topics': [{
            'name': topic_name1
        }]})
    else:
        test_cg_configuration.update({'topics': [{
            'name': topic_name1,
            'partitions': partitions
        }]})

    test_cg_configuration.update(sasl_default_configuration)
    ensure_idempotency(
        ensure_kafka_consumer_group,
        host,
        test_cg_configuration,
        minimal_api_version="2.4.0"
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        if (parse_version(
                host_protocol_version[host_vars['inventory_hostname']]) <
                parse_version("2.4.0")):
            continue

        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_unconsumed_topic(consumer_group, topic_name1, kfk_addr)
