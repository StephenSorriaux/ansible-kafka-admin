"""
Main tests for library
"""

import os
import time

from pkg_resources import parse_version
import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    get_topic_name,
    topic_defaut_configuration,
    ensure_kafka_topic,
    ensure_kafka_topic_with_zk,
    check_configured_topic,
    host_protocol_version, ensure_idempotency,
    ensure_kafka_topics
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


def test_update_replica_factor(host):
    """
    Check if can update replication factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'replica_factor': 2
    })
    ensure_idempotency(
        ensure_kafka_topic_with_zk,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_replica_factor_preserve_leader(host):
    """
    Check if can update replication factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'replica_factor': 2,
        'preserve_leader': True
    })
    ensure_idempotency(
        ensure_kafka_topic_with_zk,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_replica_factor_force_reassign(host):
    """
    Check if can update replication factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'kafka_sleep_time': 10,
        'kafka_max_retries': 10,
        'replica_factor': 2,
        'force_reassign': True,
        'preserve_leader': True
    })
    ensure_kafka_topic_with_zk(
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    test_topic_configuration.update({
        'replica_factor': 2,
        'force_reassign': True,
        'preserve_leader': True
    })
    results = ensure_kafka_topic_with_zk(
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for result in results:
        assert result['changed'], str(result)
        assert topic_name in result['changes']['topic_updated'], str(result)


def test_update_partitions(host):
    """
    Check if can update partitions numbers
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': 2
    })
    ensure_idempotency(
        ensure_kafka_topic_with_zk,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions_without_zk(host):
    """
    Check if can update partitions numbers without zk (only > 1.0.0)
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': 2
    })
    ensure_idempotency(
        ensure_kafka_topic,
        host,
        test_topic_configuration,
        topic_name,
        minimal_api_version="1.0.0"
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        if (parse_version(
                host_protocol_version[host_vars['inventory_hostname']]) <
                parse_version("1.0.0")):
            continue
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions_and_replica_factor(host):
    """
    Check if can update partitions numbers and replica factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': 4,
        'replica_factor': 2
    })
    ensure_idempotency(
        ensure_kafka_topic_with_zk,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions_and_replica_factor_default_value(host):
    """
    Check if can update partitions numbers
    make sure -1 values are considered, but warning + step is skipped
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': -1,
        'replica_factor': -1
    })
    ensure_kafka_topic(
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    expected_topic_configuration = topic_defaut_configuration.copy()
    expected_topic_configuration.update({
        'partitions': 1,
        'replica_factor': 1
    })
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, expected_topic_configuration,
                               topic_name, kfk_addr)


def test_add_options(host):
    """
    Check if can update topic options
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'options': {
            'retention.ms': 66574936,
            'flush.ms': 564939
        }
    })
    ensure_idempotency(
        ensure_kafka_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_delete_options(host):
    """
    Check if can remove topic options
    """
    # Given
    init_topic_configuration = topic_defaut_configuration.copy()
    init_topic_configuration.update({
        'options': {
            'retention.ms': 66574936,
            'flush.ms': 564939
        }
    })
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        init_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'options': {
            'flush.ms': 564939
        }
    })
    ensure_idempotency(
        ensure_kafka_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    deleted_options = {
        'retention.ms': 66574936,
    }
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr,
                               deleted_options=deleted_options)


def test_delete_topic(host):
    """
    Check if can delete topic
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'state': 'absent'
    })
    ensure_idempotency(
        ensure_kafka_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_check_mode(host):
    """
    Check if can check mode do nothing
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'state': 'absent'
    })
    ensure_kafka_topic_with_zk(
        host,
        test_topic_configuration,
        topic_name,
        check=True
    )
    time.sleep(0.3)
    test_topic_configuration.update({
        'state': 'present',
        'partitions': topic_defaut_configuration['partitions'] + 1,
        'replica_factor': topic_defaut_configuration['replica_factor'] + 1,
        'options': {
            'retention.ms': 1000
        }
    })
    ensure_kafka_topic_with_zk(
        host,
        test_topic_configuration,
        topic_name,
        check=True
    )
    time.sleep(0.3)
    new_topic_name = get_topic_name()
    ensure_kafka_topic_with_zk(
        host,
        test_topic_configuration,
        new_topic_name,
        check=True
    )
    time.sleep(0.3)
    # Then
    expected_topic_configuration = topic_defaut_configuration.copy()
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, expected_topic_configuration,
                               topic_name, kfk_addr)
        test_topic_configuration.update({
            'state': 'absent'
        })
        check_configured_topic(host, test_topic_configuration,
                               new_topic_name, kfk_addr)


def test_delete_options_topics(host):
    """
    Check if can remove topics options
    """
    # Given
    def get_topic_config():
        topic_configuration = topic_defaut_configuration.copy()
        topic_configuration.update({
            'name': get_topic_name(),
            'options': {
                'retention.ms': 66574936,
                'flush.ms': 564939
            }
        })
        return topic_configuration
    topic_configuration = {
        'topics': [
            get_topic_config(),
            get_topic_config()
        ]
    }
    ensure_kafka_topics(
        host,
        topic_configuration
    )
    time.sleep(0.3)
    # When
    for topic in topic_configuration['topics']:
        topic['options'] = {
            'flush.ms': 564939
        }
    ensure_idempotency(
        ensure_kafka_topics,
        host,
        topic_configuration
    )
    time.sleep(0.3)
    # Then
    deleted_options = {
        'retention.ms': 66574936,
    }
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        for topic in topic_configuration['topics']:
            check_configured_topic(host, topic,
                                   topic['name'], kfk_addr,
                                   deleted_options=deleted_options)


def test_duplicated_topics(host):
    """
    Check if can remove topics options
    """
    # Given
    duplicated_topic_name = get_topic_name()

    def get_topic_config():
        topic_configuration = topic_defaut_configuration.copy()
        topic_configuration.update({
            'name': duplicated_topic_name,
            'options': {
                'retention.ms': 66574936,
                'flush.ms': 564939
            }
        })
        return topic_configuration
    topic_configuration = {
        'topics': [
            get_topic_config(),
            get_topic_config()
        ]
    }
    # When
    results = ensure_kafka_topics(
        host,
        topic_configuration
    )
    time.sleep(0.3)
    # Then
    for result in results:
        assert not result['changed']
        assert 'duplicated topics' in result['msg']
