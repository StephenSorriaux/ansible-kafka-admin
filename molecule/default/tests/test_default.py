"""
Main tests for library
"""

import json
import os
import time

import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    get_topic_name,
    get_acl_name,
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
    produce_and_consume_topic,
    ensure_idempotency
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
    ensure_topic(
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
        ensure_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions(host):
    """
    Check if can update partitions numbers
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
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
        ensure_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions_and_replica_factor(host):
    """
    Check if can update partitions numbers and replica factor
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
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
        ensure_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_update_partitions_and_replica_factor_default_value(host):
    """
    Check if can update partitions numbers
    make sure -1 values are considered, but warning + step is skipped
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
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
    ensure_topic(
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
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, expected_topic_configuration,
                               topic_name, kfk_addr)


def test_add_options(host):
    """
    Check if can update topic options
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
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
        ensure_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, test_topic_configuration,
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
    ensure_topic(
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
        ensure_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    deleted_options = {
        'retention.ms': 66574936,
    }
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, test_topic_configuration,
                               topic_name, kfk_addr,
                               deleted_options=deleted_options)


def test_delete_topic(host):
    """
    Check if can delete topic
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
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
        ensure_topic,
        host,
        test_topic_configuration,
        topic_name
    )
    time.sleep(0.3)
    # Then
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, test_topic_configuration,
                               topic_name, kfk_addr)


def test_acl_create(host):
    """
    Check if can create acls
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
    test_acl_configuration.update({
        'name': get_acl_name(),
        'state': 'absent',
        **sasl_default_configuration
    })
    ensure_acl(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    test_acl_configuration.update({
        'state': 'present'
    })
    ensure_idempotency(
        ensure_acl,
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # Then
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(kafka_host, test_acl_configuration, kfk_addr)


def test_acl_delete(host):
    """
    Check if can delete acls
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
    test_acl_configuration.update({
        'name': get_acl_name(),
        'state': 'present',
        **sasl_default_configuration
    })
    ensure_acl(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    test_acl_configuration.update({
        'state': 'absent'
    })
    ensure_idempotency(
        ensure_acl,
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # Then
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(kafka_host, test_acl_configuration, kfk_addr)


def test_consumer_lag(host):
    """
    Check if can check global consumer lag
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    consumer_group = get_consumer_group()
    total_msg = 42
    # When
    produce_and_consume_topic(
        topic_name, total_msg, consumer_group)
    time.sleep(0.3)
    # Then
    lags = call_kafka_stat_lag(host, {
        'consummer_group': consumer_group
    })
    for lag in lags:
        msg = json.loads(lag['msg'])
        global_lag_count = msg['global_lag_count']
        assert global_lag_count == (total_msg - 1)


def test_check_mode(host):
    """
    Check if can check mode do nothing
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    test_acl_configuration = acl_defaut_configuration.copy()
    test_acl_configuration.update({
        'name': get_acl_name(),
        'state': 'present',
        **sasl_default_configuration
    })
    ensure_acl(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    test_topic_configuration = topic_defaut_configuration.copy()
    test_topic_configuration.update({
        'state': 'absent'
    })
    ensure_topic(
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
    ensure_topic(
        host,
        test_topic_configuration,
        topic_name,
        check=True
    )
    time.sleep(0.3)
    new_topic_name = get_topic_name()
    ensure_topic(
        host,
        test_topic_configuration,
        new_topic_name,
        check=True
    )
    time.sleep(0.3)
    check_acl_configuration = test_acl_configuration.copy()
    check_acl_configuration.update({
        'state': 'absent'
    })
    ensure_acl(
        host,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.3)
    check_acl_configuration.update({
        'state': 'present',
        'name': get_topic_name()
    })
    ensure_acl(
        host,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.3)
    # Then
    expected_topic_configuration = topic_defaut_configuration.copy()
    for kafka_host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9092" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        kfk_sasl_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_topic(kafka_host, expected_topic_configuration,
                               topic_name, kfk_addr)
        check_configured_acl(kafka_host, test_acl_configuration, kfk_sasl_addr)
        test_topic_configuration.update({
            'state': 'absent'
        })
        check_configured_topic(kafka_host, test_topic_configuration,
                               new_topic_name, kfk_addr)
        check_acl_configuration.update({
            'state': 'absent'
        })
        check_configured_acl(kafka_host, test_acl_configuration, kfk_sasl_addr)


def test_kafka_info_topic(host):
    """
    Check if can get info on topic
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    results = call_kafka_info(
        host,
        {
            'resource': 'topic'
        }
    )
    # Then
    for r in results:
        assert topic_name in r['ansible_module_results']


def test_kafka_info_brokers(host):
    """
    Check if can get info on brokers
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    # When
    results = call_kafka_info(
        host,
        {
            'resource': 'broker'
        }
    )
    # Then
    for r in results:
        assert len(r['ansible_module_results']) == 2


def test_kafka_info_consumer_group(host):
    """
    Check if can get info on consumer groups
    """
    # Given
    topic_name = get_topic_name()
    ensure_topic(
        host,
        topic_defaut_configuration,
        topic_name
    )
    time.sleep(0.3)
    consumer_group = get_consumer_group()
    total_msg = 42
    # When
    produce_and_consume_topic(
        topic_name, total_msg, consumer_group)
    time.sleep(0.3)
    # Then
    results = call_kafka_info(
        host,
        {
            'resource': 'consumer_group'
        }
    )
    for r in results:
        assert consumer_group in r['ansible_module_results']


def test_kafka_info_acl(host):
    """
    Check if can get info on acl
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
    resource_name = get_acl_name()
    test_acl_configuration.update({
        'name': resource_name,
        'state': 'present',
        **sasl_default_configuration
    })
    ensure_acl(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    results = call_kafka_info(
        host,
        {
            'resource': 'acl'
        }
    )
    expected = {
        'resource_type': 'topic',
        'operation': 'write',
        'permission_type': 'allow',
        'resource_name': resource_name,
        'principal': 'User:common',
        'host': '*',
        'pattern_type': 'literal'
    }
    # Then
    for r in results:
        assert expected in r['ansible_module_results']['topic'][resource_name]
