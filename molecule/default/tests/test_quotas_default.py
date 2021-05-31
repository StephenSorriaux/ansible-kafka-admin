"""
Main tests for library
"""

import os
import time

import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    quotas_default_configuration,
    sasl_default_configuration,
    ensure_kafka_quotas, get_entity_name,
    check_configured_quotas_kafka, ensure_idempotency,
    check_configured_quotas_zookeeper
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

zookeeper_hosts = dict()
for host in testinfra.get_hosts(
    ['zookeeper'],
    connection='ansible',
    ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
):
    zookeeper_hosts[host] = host.ansible.get_variables()


def test_quotas_create(host):
    """
    Check if can create quotas
    """
    # Given
    test_quotas_configuration = quotas_default_configuration.copy()
    test_quotas_configuration.update({
        'entries': [{
            'entity': [{
                'entity_type': 'client-id',
                'entity_name': get_entity_name()
            },
                       {
                'entity_type': 'user',
                'entity_name': get_entity_name()
            }],
            'quotas': {
                'producer_byte_rate': 104101
            }
        }]
    })
    test_quotas_configuration.update(sasl_default_configuration)
    ensure_kafka_quotas(
        host,
        test_quotas_configuration
    )
    time.sleep(0.3)
    # When
    test_quotas_configuration['entries'][0].update({
        'quotas': {
            'producer_byte_rate': 104101,
            'consumer_byte_rate': 104101
        }
    })
    ensure_idempotency(
        ensure_kafka_quotas,
        host,
        test_quotas_configuration
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_quotas_kafka(host,
                                      test_quotas_configuration,
                                      kfk_addr)
    for host, host_vars in zookeeper_hosts.items():
        zk_addr = "%s:2181" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_quotas_zookeeper(host,
                                          test_quotas_configuration,
                                          zk_addr)


def test_quotas_alter(host):
    """
    Check if can create quotas
    """
    # Given
    test_quotas_configuration = quotas_default_configuration.copy()
    test_quotas_configuration.update({
        'entries': [{
            'entity': [{
                'entity_type': 'user',
                'entity_name': get_entity_name()
            }],
            'quotas': {
                'producer_byte_rate': 10410,
                'consumer_byte_rate': 10410
            }
        }]
    })
    test_quotas_configuration.update(sasl_default_configuration)
    ensure_kafka_quotas(
        host,
        test_quotas_configuration
    )
    time.sleep(0.3)
    # When
    test_quotas_configuration['entries'][0].update({
        'quotas': {
            'producer_byte_rate': 12,
            'consumer_byte_rate': 10
        }
    })
    ensure_idempotency(
        ensure_kafka_quotas,
        host,
        test_quotas_configuration
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_quotas_kafka(host,
                                      test_quotas_configuration,
                                      kfk_addr)
    for host, host_vars in zookeeper_hosts.items():
        zk_addr = "%s:2181" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_quotas_zookeeper(host,
                                          test_quotas_configuration,
                                          zk_addr)


def test_quotas_delete(host):
    """
    Check if can create quotas
    """
    # Given
    test_quotas_configuration = quotas_default_configuration.copy()
    test_quotas_configuration.update({
        'entries': [{
            'entity': [{
                'entity_type': 'user',
                'entity_name': get_entity_name()
            }],
            'quotas': {
                'producer_byte_rate': 104101,
                'consumer_byte_rate': 104101
            }
        }]
    })
    test_quotas_configuration.update(sasl_default_configuration)
    ensure_kafka_quotas(
        host,
        test_quotas_configuration
    )
    time.sleep(0.3)
    # When
    test_quotas_configuration['entries'][0].update({
        'quotas': {
            'producer_byte_rate': 104101
        }
    })
    ensure_idempotency(
        ensure_kafka_quotas,
        host,
        test_quotas_configuration
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_quotas_kafka(host,
                                      test_quotas_configuration,
                                      kfk_addr)
    for host, host_vars in zookeeper_hosts.items():
        zk_addr = "%s:2181" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_quotas_zookeeper(host,
                                          test_quotas_configuration,
                                          zk_addr)
