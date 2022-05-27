"""
Main tests for library
"""

import os
import time

import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    acl_defaut_configuration,
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


def test_acl_create(host):
    """
    Check if can create acls
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
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


def test_acl_delete(host):
    """
    Check if can delete acls
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
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


def test_check_mode(host):
    """
    Check if can check mode do nothing
    """
    # Given
    test_acl_configuration = acl_defaut_configuration.copy()
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
    ensure_kafka_acl(
        host,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.3)
    check_acl_configuration.update({
        'state': 'present',
        'name': "test_" + str(time.time())
    })
    ensure_kafka_acl(
        host,
        check_acl_configuration,
        check=True
    )
    time.sleep(0.3)
    # Then
    for host, host_vars in kafka_hosts.items():
        kfk_sasl_addr = "%s:9094" % \
            host_vars['ansible_eth0']['ipv4']['address']['__ansible_unsafe']
        check_configured_acl(host, test_acl_configuration, kfk_sasl_addr)
        check_acl_configuration.update({
            'state': 'absent'
        })
        check_configured_acl(host, test_acl_configuration, kfk_sasl_addr)


def test_acls_create(host):
    """
    Check if can create acls
    """
    # Given
    def get_acl_config():
        acl_configuration = acl_defaut_configuration.copy()
        acl_configuration.update({
            'name': get_acl_name(),
            'state': 'absent'
        })
        return acl_configuration
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


def test_duplicated_acls(host):
    """
    Check if can create acls
    """
    # Given
    duplicated_acl_name = get_acl_name()

    def get_acl_config():
        acl_configuration = acl_defaut_configuration.copy()
        acl_configuration.update({
            'name': duplicated_acl_name,
            'state': 'present'
        })
        return acl_configuration
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


# Disable to not delete other tests acls
# def test_single_element_acls_with_mark_others_as_absent_works(host):
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
#             get_acl_config()
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
