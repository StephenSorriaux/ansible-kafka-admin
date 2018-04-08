import os

import testinfra.utils.ansible_runner

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts(['kafka1', 'kafka2'])


def test_error_kafka_logs(host):
    controller_log = host.file('/opt/kafka/logs/controller.log')

    assert not controller_log.contains('ERROR')
