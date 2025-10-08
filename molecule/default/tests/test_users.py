import os
import time

import pytest

import testinfra.utils.ansible_runner
from tests.ansible_utils import (
    ensure_kafka_user, ensure_idempotency,
    get_user_name, call_kafka_info
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

user_configurations_testdata = [
    pytest.param(
        {'password': 'changeit', 'state': 'present'}, id="user_defaults"
    ),
    pytest.param(
        {
            'password': 'changeit',
            'mechanism': 'SCRAM-SHA-256',
            'iterations': 5000,
            'state': 'present'
        },
        id="user_overrides"
    ),
]


@pytest.mark.parametrize("user_configuration", user_configurations_testdata)
def test_user_create(host, user_configuration):
    """
    Check if can create acls
    """
    # Given
    test_user_configuration = user_configuration.copy()
    test_user_configuration.update({
        'name': get_user_name(),
        'state': 'absent'
    })
    ensure_kafka_user(
        host,
        test_user_configuration
    )
    time.sleep(0.3)
    # When
    test_user_configuration.update({
        'state': 'present'
    })
    ensure_idempotency(
        ensure_kafka_user,
        host,
        test_user_configuration
    )
    time.sleep(0.3)
    # Then
    kafka_user_infos = call_kafka_info(
        host, {'resource': 'user'}, minimal_api_version='2.7.0'
    )
    for kafka_user_info in kafka_user_infos:
        assert kafka_user_info['ansible_module_results'] is not None
        assert kafka_user_info['ansible_module_results']['users'] is not None
        users = kafka_user_info['ansible_module_results']['users']
        username = test_user_configuration['name']
        assert users[username] is not None
        assert users[username][0]['iterations'] == test_user_configuration.get(
            'iterations', 4096
        )
        assert users[username][0]['mechanism'] == test_user_configuration.get(
            'mechanism', 'SCRAM-SHA-512'
        )
