import os
import yaml
import json
import six
import testinfra
import sys
import types

if sys.version_info >= (3, 12, 0):
    m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
    setattr(m, 'range', range)
    sys.modules['kafka.vendor.six.moves'] = m

from pkg_resources import parse_version

from tests.utils import KafkaManager

from kafka import KafkaConsumer, KafkaProducer
from kazoo.client import KazooClient

import uuid


def get_molecule_configuration():
    molecule_file = os.environ['MOLECULE_FILE']
    with open(molecule_file, 'r') as f:
        return yaml.safe_load(f)


def get_topic_name():
    return "test_" + str(uuid.uuid4())


def get_acl_name():
    return "test_" + str(uuid.uuid4())


def get_entity_name():
    return "test_" + str(uuid.uuid4())


def get_consumer_group():
    return "AWESOME_consumer_group_" + str(uuid.uuid4())


molecule_configuration = get_molecule_configuration()

topic_defaut_configuration = {
    'state': 'present',
    'replica_factor': 1,
    'partitions': 1,
    'options': {}
}

acl_defaut_configuration = {
    'acl_resource_type': 'topic',
    'state': 'absent',
    'acl_principal': 'User:common',
    'acl_operation': 'write',
    'acl_permission': 'allow',
    'acl_pattern_type': 'literal'
}

acl_multi_ops_configuration = {
    'acl_resource_type': 'topic',
    'state': 'absent',
    'acl_principal': 'User:common',
    'acl_operations': [
        'write',
        'describe'
    ],
    'acl_permission': 'allow',
    'acl_pattern_type': 'literal'
}

cg_defaut_configuration = {}

quotas_default_configuration = {}

sasl_default_configuration = {
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_plain_username': 'admin',
    'sasl_plain_password': 'admin-secret'
}

sasl_ssl_default_configuration = {
    'security_protocol': 'SASL_SSL',
    'sasl_plain_username': 'admin',
    'sasl_plain_password': 'admin-secret',
    'ssl_cafile': '/opt/tls/cacert.pem'
}

ssl_default_configuration = {
    'security_protocol': 'SSL',
    'ssl_certfile': '/opt/tls/client/client.cert.pem',
    'ssl_keyfile': '/opt/tls/client/client.key.pem',
    'ssl_cafile': '/opt/tls/cacert.pem'
}

zk_ssl_default_configuration = {
    'zookeeper_ssl_certfile': '/opt/tls/client/client.cert.pem',
    'zookeeper_ssl_keyfile': '/opt/tls/client/client.key.pem',
    'zookeeper_ssl_cafile': '/opt/tls/cacert.pem',
    'zookeeper_use_ssl': True
}


env_no_sasl = []
env_sasl = []
env_sasl_ssl = []
env_sasl_ssl_zk_tls = []
env_ssl = []
env_ssl_zk_tls = []

host_protocol_version = {}

CONSUMER_MAX_RETRY = 1000

ansible_kafka_supported_versions = \
    (molecule_configuration['provisioner']
     ['inventory']
     ['group_vars']
     ['all']['ansible_kafka_supported_versions'])
for supported_version in ansible_kafka_supported_versions:
    protocol_version = supported_version['protocol_version']
    instance_suffix = supported_version['instance_suffix']
    zk_tls = supported_version.get('zk_tls', False)
    with_zk = supported_version.get('with_zk', True)
    zk = testinfra.get_host(
        'zookeeper-' + instance_suffix,
        connection='ansible',
        ansible_inventory=os.environ['MOLECULE_INVENTORY_FILE']
    )
    host_protocol_version['zookeeper-' + instance_suffix] = protocol_version
    zk_addr = (
        "%s:2181" % (
            zk.ansible.get_variables()
            ['ansible_eth0']
            ['ipv4']
            ['address']
            ['__ansible_unsafe']
        )
        if with_zk else None
    )
    zk_tls_addr = (
        "%s:2281" % (
            zk.ansible.get_variables()
            ['ansible_eth0']
            ['ipv4']
            ['address']
            ['__ansible_unsafe']
        )
        if with_zk else None
    )
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
    env_sasl_ssl.append({
        'protocol_version': protocol_version,
        'zk_addr': zk_addr,
        'kfk_addr': 'kafka1-%s:%d,kafka2-%s:%d' % (
            instance_suffix, 9095, instance_suffix, 9095)
    })
    if zk_tls:
        env_sasl_ssl_zk_tls.append({
            'protocol_version': protocol_version,
            'zk_addr': zk_tls_addr,
            'kfk_addr': 'kafka1-%s:%d,kafka2-%s:%d' % (
                instance_suffix, 9095, instance_suffix, 9095)
        })
    env_ssl.append({
        'protocol_version': protocol_version,
        'zk_addr': zk_addr,
        'kfk_addr': 'kafka1-%s:%d,kafka2-%s:%d' % (
            instance_suffix, 9096, instance_suffix, 9096)
    })
    if zk_tls:
        env_ssl_zk_tls.append({
            'protocol_version': protocol_version,
            'zk_addr': zk_tls_addr,
            'kfk_addr': 'kafka1-%s:%d,kafka2-%s:%d' % (
                instance_suffix, 9096, instance_suffix, 9096)
        })
    env_no_sasl.append({
        'protocol_version': protocol_version,
        'zk_addr': zk_addr,
        'kfk_addr': '%s:%d,%s:%d' % (kfk1_addr, 9092, kfk2_addr, 9092)
    })


def make_args(args_dict):
    return json.dumps(args_dict)


def call_kafka_stat_lag(
        host,
        args=None
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
    else:
        envs = env_no_sasl
    for env in envs:
        module_args = {
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_stat_lag',
                              make_args(module_args), check=False)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)
    return results


def call_kafka_info(
        host,
        args=None
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
    else:
        envs = env_no_sasl
    for env in envs:
        module_args = {
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_info',
                              make_args(module_args), check=False)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)

    return results


def call_kafka_lib(
        host,
        args=None,
        check=False
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_sasl_ssl_zk_tls
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_ssl_zk_tls
    else:
        envs = env_no_sasl
    for env in envs:
        module_args = {
            'zookeeper': env['zk_addr'],
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_lib',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)
    return results


def call_kafka_topic_with_zk(
        host,
        args=None,
        check=False
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_sasl_ssl_zk_tls
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_ssl_zk_tls
    else:
        envs = env_no_sasl
    for env in envs:
        module_args = {
            'zookeeper': env['zk_addr'],
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_topic',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)
    return results


def call_kafka_topic(
        host,
        args=None,
        check=False,
        minimal_api_version="0.0.0"
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']
        if (parse_version(minimal_api_version) >
                parse_version(protocol_version)):
            continue

        module_args = {
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_topic',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)

    return results


def call_kafka_topics(
        host,
        args=None,
        check=False,
        minimal_api_version="0.0.0"
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']
        if (parse_version(minimal_api_version) >
                parse_version(protocol_version)):
            continue

        module_args = {
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_topics',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)

    return results


def call_kafka_quotas(
        host,
        args=None,
        check=False,
        minimal_api_version="0.0.0"
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_sasl_ssl_zk_tls
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_ssl_zk_tls
    else:
        envs = env_no_sasl
    for env in envs:
        protocol_version = env['protocol_version']
        if (parse_version(minimal_api_version) >
                parse_version(protocol_version)):
            continue

        module_args = {
            'zookeeper': env['zk_addr'],
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_quotas',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)

    return results


def call_kafka_consumer_group(
        host,
        args=None,
        check=False,
        minimal_api_version="0.0.0"
):
    results = []
    if args is None:
        args = {}
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
            'api_version': protocol_version,
        }
        module_args.update(args)
        result = host.ansible('kafka_consumer_group',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)

    return results


def call_kafka_acl(
        host,
        args=None,
        check=False
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_sasl_ssl_zk_tls
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
        if 'zookeeper_use_ssl' in args:
            envs = env_ssl_zk_tls
    else:
        envs = env_no_sasl
    for env in envs:
        module_args = {
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_acl',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)

    return results


def call_kafka_acls(
        host,
        args=None,
        check=False
):
    results = []
    if args is None:
        args = {}
    if ('security_protocol' in args and
            args['security_protocol'] == 'SASL_PLAINTEXT'):
        envs = env_sasl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SASL_SSL'):
        envs = env_sasl_ssl
    elif ('security_protocol' in args and
          args['security_protocol'] == 'SSL'):
        envs = env_ssl
    else:
        envs = env_no_sasl
    for env in envs:
        module_args = {
            'bootstrap_servers': env['kfk_addr'],
            'api_version': env['protocol_version'],
        }
        module_args.update(args)
        result = host.ansible('kafka_acls',
                              make_args(module_args), check=check)
        result.update({'module_args': module_args, 'env': env})
        results.append(result)

    return results


def ensure_topic(host, topic_defaut_configuration,
                 topic_name, check=False):
    return call_kafka_lib(host, {
        'resource': 'topic',
        'name': topic_name,
        **topic_defaut_configuration
    }, check)


def ensure_acl(host, test_acl_configuration, check=False):
    return call_kafka_lib(host, {
        'resource': 'acl',
        **test_acl_configuration
    }, check)


def ensure_kafka_topic(host, topic_defaut_configuration,
                       topic_name, check=False, minimal_api_version="0.0.0"):
    call_config = topic_defaut_configuration.copy()
    call_config.update({
        'name': topic_name
    })
    return call_kafka_topic(
        host, call_config, check, minimal_api_version=minimal_api_version
    )


def ensure_kafka_topics(host, topics, check=False,
                        minimal_api_version="0.0.0"):
    return call_kafka_topics(
        host, topics, check, minimal_api_version=minimal_api_version
    )


def ensure_kafka_quotas(host, entries, check=False,
                        minimal_api_version="0.0.0"):
    return call_kafka_quotas(
        host, entries, check, minimal_api_version=minimal_api_version
    )


def ensure_kafka_topic_with_zk(host, topic_defaut_configuration,
                               topic_name, check=False):
    call_config = topic_defaut_configuration.copy()
    call_config.update({
        'name': topic_name
    })
    return call_kafka_topic_with_zk(host, call_config, check)


def ensure_kafka_consumer_group(host, test_consumer_group_configuration,
                                check=False, minimal_api_version="0.0.0"):
    return call_kafka_consumer_group(host, test_consumer_group_configuration,
                                     check, minimal_api_version)


def ensure_kafka_acl(host, test_acl_configuration, check=False):
    return call_kafka_acl(host, test_acl_configuration, check)


def ensure_kafka_acls(host, test_acl_configuration, check=False):
    return call_kafka_acls(host, test_acl_configuration, check)


def check_unconsumed_topic(consumer_group, unconsumed_topic, kafka_servers):
    kafka_client = KafkaManager(
        bootstrap_servers=kafka_servers,
        api_version=(2, 4, 0)
    )
    kafka_client.get_consumed_topic_for_consumer_group(
        consumer_group)
    # assert unconsumed_topic not in consumed_topics


def check_configured_topic(host, topic_configuration,
                           topic_name, kafka_servers, deleted_options=None):
    """
    Test if topic configuration is what was defined
    """
    protocol_version = host_protocol_version[host.backend.host]
    api_version = tuple(
        int(p) for p in protocol_version.strip(".").split(".")
    )
    kafka_client = KafkaManager(
        bootstrap_servers=kafka_servers,
        api_version=api_version
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
    protocol_version = host_protocol_version[host.backend.host]
    api_version = tuple(
        int(p) for p in protocol_version.strip(".").split(".")
    )
    kafka_client = KafkaManager(
        bootstrap_servers=kafka_servers,
        api_version=api_version,
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


def _map_entity(entity):
    e = []
    if 'user' in entity and entity['user']:
        e.append({
            'entity_type': 'user',
            'entity_name': entity['user']
        })
    if 'client' in entity and entity['client']:
        e.append({
            'entity_type': 'client-id',
            'entity_name': entity['client']
        })
    return e


def _map_entries(entries):
    return [
        {
            'entity': _map_entity(entry['entity']),
            'quotas': entry['quotas']
        }
        for entry in entries
    ]


def check_configured_quotas_kafka(host, quotas_configuration, kafka_servers):
    """
    Test if acl configuration is what was defined
    """
    protocol_version = host_protocol_version[host.backend.host]
    if (parse_version(protocol_version) <
            parse_version('2.6.0')):
        return
    api_version = tuple(
        int(p) for p in protocol_version.strip(".").split(".")
    )
    kafka_client = KafkaManager(
        bootstrap_servers=kafka_servers,
        api_version=api_version,
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='admin',
        sasl_plain_password='admin-secret'
    )

    try:
        entries = kafka_client.describe_quotas()
        for expected_entry in _map_entries(quotas_configuration['entries']):
            found = False
            for entity in entries:
                if (sorted(entity['entity'],
                           key=lambda e: e['entity_type']) ==
                    sorted(expected_entry['entity'],
                           key=lambda e: e['entity_type'])):
                    found = True
                    assert entity['quotas'] == expected_entry['quotas']
                    break
            assert found
    finally:
        kafka_client.close()


def check_configured_quotas_zookeeper(host, quotas_configuration, zk_server):
    """
    Test if acl configuration is what was defined
    """

    # To uncomment when zookeeper is no longer available
    # if (parse_version(host_protocol_version[host.backend.host])
    #    >= parse_version('2.6.0')):
    #    return

    zk = None

    try:
        zk = KazooClient(hosts=zk_server, read_only=True)
        zk.start()

        zknode = '/config'
        current_quotas = []
        # Get client-id quotas
        if zk.exists(zknode + '/clients'):
            client_ids = zk.get_children(zknode + '/clients')
            for client_id in client_ids:
                config, _ = zk.get(
                    zknode + '/clients/' + client_id)
                current_quotas.append({
                    'entity': [{
                        'entity_type': 'client-id',
                        'entity_name': client_id
                    }],
                    'quotas': {key: float(value) for key, value
                               in json.loads(config)['config'].items()}
                })
        # Get users quotas
        if zk.exists(zknode + '/users'):
            users = zk.get_children(zknode + '/users')
            for user in users:
                # Only user
                config, _ = zk.get(
                    zknode + '/users/' + user)
                if config:
                    current_quotas.append({
                        'entity': [{
                            'entity_type': 'user',
                            'entity_name': user
                        }],
                        'quotas': {
                            key: float(value)
                            for key, value in
                            json.loads(config)['config'].items()
                        }
                    })
                if zk.exists(zknode + '/users/' + user + '/clients'):
                    clients = zk.get_children(zknode + '/users/'
                                              + user + '/clients')
                    for client in clients:
                        config, _ = zk.get(
                            zknode + '/users/' + user + '/clients/' + client)
                        current_quotas.append({
                            'entity': [{
                                'entity_type': 'user',
                                'entity_name': user
                            }, {
                                'entity_type': 'client-id',
                                'entity_name': client
                            }],
                            'quotas': {
                                key: float(value)
                                for key, value in
                                json.loads(config)['config'].items()
                            }
                        })
        for expected_entry in _map_entries(quotas_configuration['entries']):
            found = False
            for entity in current_quotas:
                if (sorted(entity['entity'],
                           key=lambda e: e['entity_type']) ==
                    sorted(expected_entry['entity'],
                           key=lambda e: e['entity_type'])):
                    found = True
                    assert entity['quotas'] == expected_entry['quotas']
                    break
            assert found
    finally:
        if zk is not None:
            zk.stop()
            zk.close()


def produce_and_consume_topic(topic_name, total_msg, consumer_group,
                              close_consumer=False,
                              minimal_api_version="0.0.0"):
    for env in env_no_sasl:
        protocol_version = env['protocol_version']
        if (parse_version(minimal_api_version) >
                parse_version(protocol_version)):
            continue

        server = env['kfk_addr']
        maj, min, patch = env['protocol_version'].split('.')
        api_version = (int(maj), int(min), int(patch))

        producer = KafkaProducer(
            bootstrap_servers=server,
            api_version=api_version,
        )
        for i in range(total_msg):
            producer.send(topic_name, b'msg %d' % i)
        producer.flush()
        producer.close()

        consumer = KafkaConsumer(
            topic_name,
            max_poll_records=1,
            api_version=api_version,
            auto_offset_reset='earliest',
            bootstrap_servers=server,
            group_id=consumer_group,
            session_timeout_ms=60000,  # keep the group alive
            heartbeat_interval_ms=30000  # keep the group alive
        )

        retry = 0
        # ensure we consume only 1 msg
        msg = None
        while not msg and retry < CONSUMER_MAX_RETRY:
            msg = consumer.poll(timeout_ms=100, max_records=1)
            retry += 1

        # will commit offset to 1
        consumer.commit()
        # Consumer group is kept alive if specified (close_consumer=False)
        if close_consumer:
            consumer.close()


def ensure_idempotency(func, *args, **kwargs):
    """
    Ensure an Ansible call `func` is idempotent:
      - 1st call 'changed' result is True
      - 2nd call 'changed' result is False
    """
    changes = func(*args, **kwargs)
    for change in changes:
        assert change['changed'], str(change)
    changes = func(*args, **kwargs)
    for change in changes:
        assert not change['changed'], str(change)
