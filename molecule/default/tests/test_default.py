"""
Main tests for library
"""

import json
import os
import time

import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    get_topic_name,
    get_consumer_group,
    topic_defaut_configuration,
    acl_defaut_configuration,
    sasl_default_configuration,
    ensure_topic,
    ensure_acl,
    call_kafka_stat_lag,
    call_kafka_info,
    check_configured_topic,
    check_configured_acl,
    produce_and_consume_topic
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
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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


def test_update_partitions_and_replica_factor():
    """
    Check if can update partitions numbers and replica factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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
    ensure_topic(
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


def test_acl_create():
    """
    Check if can create acls
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
    test_acl_configuration.update({
        'state': 'absent',
        **sasl_default_configuration
    })
    ensure_acl(
        localhost,
        test_acl_configuration
    )
    time.sleep(0.5)
    # When
    test_acl_configuration.update({
        'state': 'present'
    })
    ensure_acl(
        localhost,
        test_acl_configuration
    )
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(host, test_acl_configuration, kfk_addr)


def test_acl_delete():
    """
    Check if can delete acls
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
    test_acl_configuration.update({
        'state': 'present',
        **sasl_default_configuration
    })
    ensure_acl(
        localhost,
        test_acl_configuration
    )
    time.sleep(0.5)
    # When
    test_acl_configuration.update({
        'state': 'absent'
    })
    ensure_acl(
        localhost,
        test_acl_configuration
    )
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(host, test_acl_configuration, kfk_addr)


def test_consumer_lag():
    """
    Check if can check global consumer lag
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    consumer_group = get_consumer_group()
    total_msg = 42
    # When
    produce_and_consume_topic(
        topic_name, total_msg, consumer_group)
    time.sleep(0.5)
    # Then
    for host, host_vars in kafka_hosts.items():
        lag = call_kafka_stat_lag(localhost, {
            'consummer_group': consumer_group
        })
        msg = json.loads(lag[0]['msg'])
        global_lag_count = msg['global_lag_count']
        assert global_lag_count == (total_msg - 1)


def test_check_mode():
    """
    Check if can check mode do nothing
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    test_acl_configuration = acl_defaut_configuration.copy()
    test_acl_configuration.update({
        'state': 'present',
        **sasl_default_configuration
    })
    ensure_acl(
        localhost,
        test_acl_configuration
    )
    time.sleep(0.5)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'state': 'absent'
    })
    ensure_topic(
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
    ensure_topic(
        localhost,
        test_topic_configuration,
        topic_name,
        check=True
    )
    time.sleep(0.5)
    new_topic_name = get_topic_name()
    ensure_topic(
        localhost,
        test_topic_configuration,
        new_topic_name,
        check=True
    )
    time.sleep(0.5)
    check_acl_configuration = test_acl_configuration.copy()
    check_acl_configuration.update({
        'state': 'absent'
    })
    ensure_acl(
        localhost,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.5)
    check_acl_configuration.update({
        'state': 'present',
        'name': get_topic_name()
    })
    ensure_acl(
        localhost,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.5)
    # Then
    expected_topic_configuration = topic_defaut_configuration.copy()
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        kfk_sasl_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(host, expected_topic_configuration,
                               topic_name, kfk_addr)
        check_configured_acl(host, test_acl_configuration, kfk_sasl_addr)
        test_topic_configuration.update({
            'state': 'absent'
        })
        check_configured_topic(host, test_topic_configuration,
                               new_topic_name, kfk_addr)
        check_acl_configuration.update({
            'state': 'absent'
        })
        check_configured_acl(host, test_acl_configuration, kfk_sasl_addr)


def test_kafka_info_topic():
    """
    Check if can get info on topic
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    results = call_kafka_info(
        localhost,
        {
            'resource': 'topic'
        }
    )
    # Then
    for r in results:
        assert topic_name in r['ansible_module_results']


def test_kafka_info_brokers():
    """
    Check if can get info on brokers
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    # When
    results = call_kafka_info(
        localhost,
        {
            'resource': 'broker'
        }
    )
    # Then
    for r in results:
        assert len(r['ansible_module_results']) == 2


def test_kafka_info_consumer_group():
    """
    Check if can get info on consumer groups
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        localhost,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.5)
    consumer_group = get_consumer_group()
    total_msg = 42
    # When
    produce_and_consume_topic(
        topic_name, total_msg, consumer_group)
    time.sleep(0.5)
    # Then
    results = call_kafka_info(
        localhost,
        {
            'resource': 'consumer_group'
        }
    )
    for r in results:
        assert consumer_group in r['ansible_module_results']
