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
    host_protocol_version
)

runner = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE'])
testinfra_hosts = runner.get_hosts('kafka')

localhost = testinfra.get_host(
    'localhost',
    connection='ansible',
    ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
)

kafka_hosts = dict()
for host in testinfra.get_hosts(
    ['kafka1'],
    connection='ansible',
    ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
):
    kafka_hosts[host] = host.ansible.get_variables()


def test_update_replica_factor():
    """
    Check if can update replication factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'replica_factor': 2
    })
    ensure_kafka_topic_with_zk(
        localhost,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions():
    """
    Check if can update partitions numbers
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': 2
    })
    ensure_kafka_topic_with_zk(
        localhost,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions_without_zk():
    """
    Check if can update partitions numbers without zk (only > 1.0.0)
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': 2
    })
    ensure_kafka_topic(
        localhost,
        test_topic_configuration,
        topic_name,
        minimal_api_version="1.0.0"
    )
    time.sleep(0.5)
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


def test_update_partitions_and_replica_factor():
    """
    Check if can update partitions numbers and replica factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': 4,
        'replica_factor': 2
    })
    ensure_kafka_topic_with_zk(
        localhost,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions_and_replica_factor_default_value():
    """
    Check if can update partitions numbers
    make sure -1 values are considered, but warning + step is skipped
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'partitions': -1,
        'replica_factor': -1
    })
    ensure_kafka_topic(
        localhost,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.5)
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


def test_add_options():
    """
    Check if can update topic options
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'options': {
            'retention.ms': 66574936,
            'flush.ms': 564939
        }
    })
    ensure_kafka_topic(
        localhost,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_delete_topic():
    """
    Check if can delete topic
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'state': 'absent'
    })
    ensure_kafka_topic(
        localhost,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_check_mode():
    """
    Check if can check mode do nothing
    """
    # Given
    topic_name = get_topic_name()
    ensure_kafka_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'state': 'absent'
    })
    ensure_kafka_topic_with_zk(
        localhost,
        test_topic_configuration,
        topic_name,
        check=True
    )
    time.sleep(0.5)
    test_topic_configuration.update({
        'state': 'present',
        'partitions': topic_defaut_configuration['partitions'] + 1,
        'replica_factor': topic_defaut_configuration['replica_factor'] + 1,
        'options': {
            'retention.ms': 1000
        }
    })
    ensure_kafka_topic_with_zk(
        localhost,
        test_topic_configuration,
        topic_name,
        check=True
    )
    time.sleep(0.5)
    new_topic_name = get_topic_name()
    ensure_kafka_topic_with_zk(
        localhost,
        test_topic_configuration,
        new_topic_name,
        check=True
    )
    time.sleep(0.5)
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
