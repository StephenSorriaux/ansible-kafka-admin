import os
import yaml
import json
import six
import testinfra

from pkg_resources import parse_version
from datetime import datetime

from tests.utils import KafkaManager

from kafka import KafkaConsumer, KafkaProducer


def get_molecule_configuration():
    molecule_file = os.environ['MOLECULE_FILE']
    with open(molecule_file, 'r') as f:
        return yaml.safe_load(f)


def get_topic_name():
    # current date and time
    now = datetime.now()

    timestamp = datetime.timestamp(now)
    return "test_" + str(timestamp)


def get_consumer_group():
    # current date and time
    now = datetime.now()

    timestamp = datetime.timestamp(now)
    return "AWESOME_consumer_group_" + str(timestamp)


molecule_configuration = get_molecule_configuration()

topic_defaut_configuration = {
    'state': 'present',
    'replica_factor': 1,
    'partitions': 1,
    'options': {}
}

acl_defaut_configuration = {
    'acl_resource_type': 'topic',
    'name': 'test-acl',
    'state': 'absent',
    'acl_principal': 'User:common',
    'acl_operation': 'write',
    'acl_permission': 'allow',
    'acl_pattern_type': 'literal'
}

sasl_default_configuration = {
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_plain_username': 'admin',
    'sasl_plain_password': 'admin-secret'
}

env_no_sasl = []
env_sasl = []

host_protocol_version = {}

ansible_kafka_supported_versions = \
    (molecule_configuration['provisioner']
     ['inventory']
     ['group_vars']
     ['all']['ansible_kafka_supported_versions'])
for supported_version in ansible_kafka_supported_versions:
    protocol_version = supported_version['protocol_version']
    instance_suffix = supported_version['instance_suffix']
    zk = testinfra.get_host(
        'zookeeper-' + instance_suffix,
        connection='ansible',
        ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
    )
    zk_addr = "%s:2181" % (zk.ansible.get_variables()
                           ['ansible_eth0']['ipv4']
                           ['address']['__ansible_unsafe'])
    kfk1 = testinfra.get_host(
        'kafka1-' + instance_suffix,
        connection='ansible',
        ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
    )
    kfk1_addr = (kfk1.ansible.get_variables()['ansible_eth0']
                 ['ipv4']['address']['__ansible_unsafe'])
    host_protocol_version['kafka1-' + instance_suffix] = protocol_version
    kfk2 = testinfra.get_host(
        'kafka2-' + instance_suffix,
        connection='ansible',
        ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
    )
    kfk2_addr = (kfk2.ansible.get_variables()['ansible_eth0']
                 ['ipv4']['address']['__ansible_unsafe'])

    host_protocol_version['kafka2-' + instance_suffix] = protocol_version
    env_sasl.append({
        'protocol_version': protocol_version,
        'zk_addr': zk_addr,
        'kfk_addr': '%s:%d,%s:%d' % (kfk1_addr, 9094, kfk2_addr, 9094)
    })
    env_no_sasl.append({
        'protocol_version': protocol_version,
        'zk_addr': zk_addr,
        'kfk_addr': '%s:%d,%s:%d' % (kfk1_addr, 9092, kfk2_addr, 9092)
    })


def call_kafka_stat_lag(
        localhost,
        args=dict()
):
    results = []
    if 'sasl_plain_username' in args:
        envs = env_sasl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']
        module_args = {
            'api_version': protocol_version,
            'bootstrap_servers': env['kfk_addr'],
        }
        module_args.update(args)
        module_args = "{{ %s }}" % json.dumps(module_args)
        results.append(localhost.ansible('kafka_stat_lag',
                                         module_args, check=False))
    return results


def call_kafka_info(
        localhost,
        args=dict()
):
    results = []
    if 'sasl_plain_username' in args:
        envs = env_sasl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']
        module_args = {
            'api_version': protocol_version,
            'bootstrap_servers': env['kfk_addr'],
        }
        module_args.update(args)
        module_args = "{{ %s }}" % json.dumps(module_args)
        results.append(localhost.ansible('kafka_info',
                                         module_args, check=False))
    return results


def call_kafka_lib(
        localhost,
        args=dict(),
        check=False
):
    results = []
    if 'sasl_plain_username' in args:
        envs = env_sasl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']

        module_args = {
            'api_version': protocol_version,
            'zookeeper': env['zk_addr'],
            'bootstrap_servers': env['kfk_addr'],
        }
        module_args.update(args)
        module_args = "{{ %s }}" % json.dumps(module_args)
        results.append(localhost.ansible('kafka_lib',
                                         module_args, check=check))
    return results


def call_kafka_topic_with_zk(
        localhost,
        args=dict(),
        check=False
):
    results = []
    if 'sasl_plain_username' in args:
        envs = env_sasl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']

        module_args = {
            'api_version': protocol_version,
            'zookeeper': env['zk_addr'],
            'bootstrap_servers': env['kfk_addr'],
        }
        module_args.update(args)
        module_args = "{{ %s }}" % json.dumps(module_args)
        results.append(localhost.ansible('kafka_topic',
                                         module_args, check=check))
    return results


def call_kafka_topic(
        localhost,
        args=dict(),
        check=False,
        minimal_api_version="0.0.0"
):
    results = []
    if 'sasl_plain_username' in args:
        envs = env_sasl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']
        if (parse_version(minimal_api_version) >
                parse_version(protocol_version)):
            continue

        module_args = {
            'api_version': protocol_version,
            'bootstrap_servers': env['kfk_addr'],
        }
        module_args.update(args)
        module_args = "{{ %s }}" % json.dumps(module_args)
        results.append(localhost.ansible('kafka_topic',
                                         module_args, check=check))
    return results


def call_kafka_acl(
        localhost,
        args=dict(),
        check=False
):
    results = []
    if 'sasl_plain_username' in args:
        envs = env_sasl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']

        module_args = {
            'api_version': protocol_version,
            'bootstrap_servers': env['kfk_addr'],
        }
        module_args.update(args)
        module_args = "{{ %s }}" % json.dumps(module_args)
        results.append(localhost.ansible('kafka_acl',
                                         module_args, check=check))
    return results


def ensure_topic(localhost, topic_defaut_configuration,
                 topic_name, check=False):
    return call_kafka_lib(localhost, {
        'resource': 'topic',
        'name': topic_name,
        **topic_defaut_configuration
    }, check)


def ensure_acl(localhost, test_acl_configuration, check=False):
    return call_kafka_lib(localhost, {
        'resource': 'acl',
        **test_acl_configuration
    }, check)


def ensure_kafka_topic(localhost, topic_defaut_configuration,
                       topic_name, check=False, minimal_api_version="0.0.0"):
    call_config = topic_defaut_configuration.copy()
    call_config.update({
        'name': topic_name
    })
    return call_kafka_topic(localhost, call_config, check)


def ensure_kafka_topic_with_zk(localhost, topic_defaut_configuration,
                               topic_name, check=False):
    call_config = topic_defaut_configuration.copy()
    call_config.update({
        'name': topic_name
    })
    return call_kafka_topic_with_zk(localhost, call_config, check)


def ensure_kafka_acl(localhost, test_acl_configuration, check=False):
    return call_kafka_acl(localhost, test_acl_configuration, check)


def check_configured_topic(host, topic_configuration,
                           topic_name, kafka_servers, deleted_options=None):
    """
    Test if topic configuration is what was defined
    """
    # Forcing api_version to 0.11.0 in order to be sure that a
    # Metadata_v1 is sent (so that we get the controller info)
    kafka_client = KafkaManager(
        bootstrap_servers=kafka_servers,
        api_version=(0, 11, 0)
    )

    if deleted_options is None:
        deleted_options = {}

    try:
        if topic_configuration['state'] == 'present':
            assert topic_name in kafka_client.get_topics()

            partitions = \
                kafka_client.get_total_partitions_for_topic(topic_name)
            assert partitions == topic_configuration['partitions']

            ite = kafka_client.get_partitions_metadata_for_topic(topic_name)
            for _, metadata in six.iteritems(ite):
                tot_replica = len(metadata.replicas)
                assert tot_replica == topic_configuration['replica_factor']

            for key, value in six.iteritems(topic_configuration['options']):
                config = kafka_client.get_config_for_topic(topic_name, [key])
                assert str(config) == str(value)

            for key, value in six.iteritems(deleted_options):
                config = kafka_client.get_config_for_topic(topic_name, key)
                assert str(config) != str(value)

        else:
            assert topic_name not in kafka_client.get_topics()
    finally:
        kafka_client.close()


def check_configured_acl(host, acl_configuration, kafka_servers):
    """
    Test if acl configuration is what was defined
    """
    # Forcing api_version to 0.11.0 in order to be sure that a
    # Metadata_v1 is sent (so that we get the controller info)
    kafka_client = KafkaManager(
        bootstrap_servers=kafka_servers,
        api_version=(0, 11, 0),
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='admin',
        sasl_plain_password='admin-secret'
    )

    acl_resource = dict(
        resource_type=2,    # topic
        operation=4,        # write
        permission_type=3,  # allow
        name=acl_configuration['name'],
        principal='User:common',
        host='*')

    try:
        acls = kafka_client.describe_acls(acl_resource)

        if acl_configuration['state'] == 'present':
            assert acls
        else:
            assert not acls
    finally:
        kafka_client.close()


def produce_and_consume_topic(topic_name, total_msg, consumer_group):
    for env in env_no_sasl:
        server = env['kfk_addr']

        producer = KafkaProducer(
            bootstrap_servers=server
        )
        for i in range(total_msg):
            producer.send(topic_name, b'msg %d' % i)
        producer.flush()
        producer.close()

        consumer = KafkaConsumer(
            topic_name,
            max_poll_records=1,
            api_version=(0, 11, 0),
            auto_offset_reset='earliest',
            bootstrap_servers=server,
            group_id=consumer_group,
            session_timeout_ms=60000,  # keep the group alive
            heartbeat_interval_ms=30000  # keep the group alive
        )

        # ensure we consume only 1 msg
        msg = None
        while not msg:
            msg = consumer.poll(timeout_ms=100, max_records=1)

        # will commit offset to 1
        consumer.commit()
        # voluntary dont close the client to keep the consumer group alive
