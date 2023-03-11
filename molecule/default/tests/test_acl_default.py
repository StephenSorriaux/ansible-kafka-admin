"""
Main tests for library
"""
import os
import time

import pytest

import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    acl_defaut_configuration,
    acl_multi_ops_configuration,
    sasl_default_configuration,
    ensure_kafka_acl, get_acl_name,
    check_configured_acl, ensure_idempotency,
    ensure_kafka_acls
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


acl_configurations_testdata = [
    pytest.param(
        acl_defaut_configuration, id="default_acl"
    ),
    pytest.param(
        acl_multi_ops_configuration, id="multi_ops_acl"
    ),
]


@pytest.mark.parametrize("acl_configuration", acl_configurations_testdata)
def test_acl_create(host, acl_configuration):
    """
    Check if can create acls
    """
    # Given
    test_acl_configuration = acl_configuration.copy()
    test_acl_configuration.update({
        'name': get_acl_name(),
        'state': 'absent'
    })
    test_acl_configuration.update(sasl_default_configuration)
    ensure_kafka_acl(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    test_acl_configuration.update({
        'state': 'present'
    })
    ensure_idempotency(
        ensure_kafka_acl,
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(host, test_acl_configuration, kfk_addr)


@pytest.mark.parametrize("acl_configuration", acl_configurations_testdata)
def test_acl_delete(host, acl_configuration):
    """
    Check if can delete acls
    """
    # Given
    test_acl_configuration = acl_configuration.copy()
    test_acl_configuration.update({
        'name': get_acl_name(),
        'state': 'present'
    })
    test_acl_configuration.update(sasl_default_configuration)
    ensure_kafka_acl(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    test_acl_configuration.update({
        'state': 'absent'
    })
    ensure_idempotency(
        ensure_kafka_acl,
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(host, test_acl_configuration, kfk_addr)


@pytest.mark.parametrize("acl_configuration", acl_configurations_testdata)
def test_check_mode(host, acl_configuration):
    """
    Check if can check mode do nothing
    """
    # Given
    results = []
    test_acl_configuration = acl_configuration.copy()
    test_acl_configuration.update({
        'name': get_acl_name(),
        'state': 'present'
    })
    test_acl_configuration.update(sasl_default_configuration)
    ensure_kafka_acl(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    check_acl_configuration = test_acl_configuration.copy()
    check_acl_configuration.update({
        'state': 'absent'
    })
    results += ensure_kafka_acl(
        host,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.3)
    check_acl_configuration.update({
        'state': 'present',
        'name': "test_" + str(time.time())
    })
    results += ensure_kafka_acl(
        host,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.3)
    # Then
    for result in results:
        assert result['changed']
    for host, host_vars in kafka_hosts.items():
        kfk_sasl_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(host, test_acl_configuration, kfk_sasl_addr)
        check_acl_configuration.update({
            'state': 'absent'
        })
        check_configured_acl(host, test_acl_configuration, kfk_sasl_addr)


@pytest.mark.parametrize("acl_configuration", acl_configurations_testdata)
def test_acls_create(host, acl_configuration):
    """
    Check if can create acls
    """
    # Given
    def get_acl_config():
        copy = acl_configuration.copy()
        copy.update({
            'name': get_acl_name(),
            'state': 'absent'
        })
        return copy

    test_acl_configuration = {
        'acls': [
            get_acl_config(),
            get_acl_config()
        ]
    }
    test_acl_configuration.update(sasl_default_configuration)
    ensure_kafka_acls(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # When
    for acl in test_acl_configuration['acls']:
        acl['state'] = 'present'

    ensure_idempotency(
        ensure_kafka_acls,
        host,
        test_acl_configuration
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        for acl in test_acl_configuration['acls']:
            check_configured_acl(host, acl, kfk_addr)


@pytest.mark.parametrize("acl_configuration", acl_configurations_testdata)
def test_duplicated_acls(host, acl_configuration):
    """
    Check if can create acls
    """
    # Given
    duplicated_acl_name = get_acl_name()

    def get_acl_config():
        copy = acl_configuration.copy()
        copy.update({
            'name': duplicated_acl_name,
            'state': 'present'
        })
        return copy

    test_acl_configuration = {
        'acls': [
            get_acl_config(),
            get_acl_config()
        ]
    }
    test_acl_configuration.update(sasl_default_configuration)
    # When
    results = ensure_kafka_acls(
        host,
        test_acl_configuration
    )
    time.sleep(0.3)

    # Then
    for result in results:
        assert not result['changed']
        assert 'duplicated acls' in result['msg']


# Disable to not delete other tests acls
# def test_acls_delete_others(host):
#     """
#     Check if can delete others acls
#     """
#     # Given
#     def get_acl_config(name=get_acl_name(), state='present'):
#         acl_configuration = acl_defaut_configuration.copy()
#         acl_configuration.update({
#             'name': name,
#             'state': state
#         })
#         return acl_configuration
#     others_acls_configuration = {
#         'acls': [
#             get_acl_config(),
#             get_acl_config()
#         ]
#     }
#     others_acls_configuration.update(sasl_default_configuration)
#     ensure_kafka_acls(
#         host,
#         others_acls_configuration
#     )
#     time.sleep(0.3)
#     # When
#     test_acl_configuration = {
#         'mark_others_as_absent': True,
#         'acls': [
#             get_acl_config(),
#             get_acl_config(state='absent')
#         ]
#     }
#     test_acl_configuration.update(sasl_default_configuration)
#     ensure_idempotency(
#         ensure_kafka_acls,
#         host,
#         test_acl_configuration
#     )
#     time.sleep(0.3)
#     # Then
#     for acl in others_acls_configuration['acls']:
#         acl['state'] = 'absent'
#
#     for host, host_vars in kafka_hosts.items():
#         kfk_addr = "%s:9094" % \
#             host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
#         for acl in others_acls_configuration['acls']:
#             check_configured_acl(host, acl, kfk_addr)
