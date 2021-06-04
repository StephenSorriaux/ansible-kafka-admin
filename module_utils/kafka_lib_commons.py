import os
from pkg_resources import parse_version

from ansible.module_utils.kafka_lib_errors import IncompatibleVersion
from ansible.module_utils.kafka_manager import KafkaManager
from ansible.module_utils.ssl_utils import (
    generate_ssl_object, generate_ssl_context
)

DOCUMENTATION_COMMON = '''
  bootstrap_servers:
    description:
      - 'kafka broker connection.'
      - 'format: host1:port,host2:port'
    required: True
  api_version:
    description:
      - 'kafka version'
      - 'format: major.minor.patch. Examples: 0.11.0 or 1.0.1'
      - 'if not set, will launch an automatic version discovery but can '
      - 'trigger stackstraces on Kafka server.'
    default: auto
  sasl_mechanism:
    description:
      - 'when using sasl, whether use PLAIN or GSSAPI ' \
        'or SCRAM-SHA-256 or SCRAM-SHA-512 '
    default: PLAIN
    choices: [PLAIN, 'SCRAM-SHA-256', 'SCRAM-SHA-512', GSSAPI]
  security_protocol:
    description:
      - 'how to connect to Kafka.'
     default: PLAINTEXT
     choices: [PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL]
  sasl_plain_username:
    description:
      - 'when using security_protocol = ssl, username to use.'
  sasl_plain_password:
    description:
      - 'when using security_protocol = ssl, password for '
      - 'sasl_plain_username.'
  ssl_check_hostname:
    description:
      - 'when using ssl for Kafka, check if certificate for hostname is '
      - 'correct.'
    default: True
  ssl_cafile:
    description:
      - 'when using ssl for Kafka, content of ca cert file or path to ca '
      - 'cert file.'
  sasl_kerberos_service_name:
    description:
      - 'when using kerberos, service name.'
  ssl_certfile:
    description:
      - 'when using ssl for Kafka, content of cert file or path to server '
      - 'cert file.'
  ssl_keyfile:
    description:
      - 'when using ssl for kafka, content of keyfile or path to server '
      - 'cert key file.'
  ssl_password:
    description:
      - 'when using ssl for Kafka, password for ssl_keyfile.'
  ssl_crlfile:
    description:
      - 'when using ssl for Kafka, content of crl file or path to cert '
      - 'crl file.'
  request_timeout_ms:
    description:
      - 'timeout for kafka client requests'
    default: 30000
  connections_max_idle_ms:
    description:
      - 'close idle connections after'
    default: 540000
'''

module_topic_commons = dict(
    partitions=dict(type='int', required=False, default=0),

    replica_factor=dict(type='int', required=False, default=0),

    options=dict(required=False, type='dict', default={}),

    kafka_sleep_time=dict(type='int', required=False, default=5),

    kafka_max_retries=dict(type='int', required=False, default=5),
)

module_acl_commons = dict(
    acl_resource_type=dict(choices=['topic', 'broker', 'cluster',
                                    'delegation_token', 'group',
                                    'transactional_id'],
                           default='topic'),

    acl_principal=dict(type='str', required=False),

    acl_operation=dict(choices=['all', 'alter', 'alter_configs',
                                'cluster_action', 'create', 'delete',
                                'describe', 'describe_configs',
                                'idempotent_write', 'read', 'write'],
                       required=False),
    acl_pattern_type=dict(choice=['any', 'match', 'literal',
                                  'prefixed'],
                          required=False, default='literal'),

    acl_permission=dict(choices=['allow', 'deny'], default='allow'),

    acl_host=dict(type='str', required=False, default="*"),
)

module_zookeeper_commons = dict(
    zookeeper=dict(type='str', required=False),

    zookeeper_auth_scheme=dict(
        choices=['digest', 'sasl'],
        default='digest'
    ),

    zookeeper_auth_value=dict(
        type='str',
        no_log=True,
        required=False,
        default=''
    ),

    zookeeper_ssl_check_hostname=dict(
        default=True,
        type='bool',
        required=False
    ),

    zookeeper_ssl_cafile=dict(
        required=False,
        default=None,
        type='path'
    ),

    zookeeper_ssl_certfile=dict(
        required=False,
        default=None,
        type='path'
    ),

    zookeeper_ssl_keyfile=dict(
        required=False,
        default=None,
        no_log=True,
        type='path'
    ),

    zookeeper_ssl_password=dict(
        type='str',
        no_log=True,
        required=False
    ),

    zookeeper_sleep_time=dict(type='int', required=False, default=5),

    zookeeper_max_retries=dict(type='int', required=False, default=5),
)

module_commons = dict(
    bootstrap_servers=dict(type='str', required=True),

    security_protocol=dict(
        choices=['PLAINTEXT', 'SSL', 'SASL_SSL', 'SASL_PLAINTEXT'],
        default='PLAINTEXT'
    ),

    api_version=dict(type='str', required=True, default=None),

    ssl_check_hostname=dict(
        default=True,
        type='bool',
        required=False
    ),

    ssl_cafile=dict(required=False, default=None, type='path'),

    ssl_certfile=dict(required=False, default=None, type='path'),

    ssl_keyfile=dict(
        required=False,
        default=None,
        no_log=True,
        type='path'
    ),

    ssl_password=dict(type='str', no_log=True, required=False),

    ssl_crlfile=dict(required=False, default=None, type='path'),

    ssl_supported_protocols=dict(
        required=False, default=None, type='list',
        choices=['TLSv1', 'TLSv1.1', 'TLSv1.2']
    ),

    ssl_ciphers=dict(required=False, default=None, type='str'),

    sasl_mechanism=dict(
        choices=['PLAIN', 'GSSAPI', 'SCRAM-SHA-256', 'SCRAM-SHA-512'],
        default='PLAIN'
    ),

    sasl_plain_username=dict(type='str', required=False),

    sasl_plain_password=dict(type='str', no_log=True, required=False),

    sasl_kerberos_service_name=dict(type='str', required=False),

    request_timeout_ms=dict(type='int', default=30000),

    connections_max_idle_ms=dict(type='int', default=540000)
)


def get_manager_from_params(params):

    bootstrap_servers = params['bootstrap_servers']
    security_protocol = params['security_protocol']
    ssl_check_hostname = params['ssl_check_hostname']
    ssl_cafile = params['ssl_cafile']
    ssl_certfile = params['ssl_certfile']
    ssl_keyfile = params['ssl_keyfile']
    ssl_password = params['ssl_password']
    ssl_crlfile = params['ssl_crlfile']
    ssl_supported_protocols = params['ssl_supported_protocols']
    ssl_ciphers = params['ssl_ciphers']
    sasl_mechanism = params['sasl_mechanism']
    sasl_plain_username = params['sasl_plain_username']
    sasl_plain_password = params['sasl_plain_password']
    sasl_kerberos_service_name = params['sasl_kerberos_service_name']
    request_timeout_ms = params['request_timeout_ms']
    connections_max_idle_ms = params['connections_max_idle_ms']

    api_version = tuple(
        int(p) for p in params['api_version'].strip(".").split(".")
    )

    kafka_ssl_files = generate_ssl_object(
        ssl_cafile, ssl_certfile, ssl_keyfile, ssl_crlfile
    )

    # Generate ssl context to support limit ssl protocols & ciphers
    ssl_context = None
    if security_protocol in ('SSL', 'SASL_SSL'):
        ssl_context = generate_ssl_context(
            ssl_check_hostname=ssl_check_hostname,
            ssl_cafile=kafka_ssl_files['cafile']['path'],
            ssl_certfile=kafka_ssl_files['certfile']['path'],
            ssl_keyfile=kafka_ssl_files['keyfile']['path'],
            ssl_password=ssl_password,
            ssl_crlfile=kafka_ssl_files['crlfile']['path'],
            ssl_supported_protocols=ssl_supported_protocols,
            ssl_ciphers=ssl_ciphers
        )

    manager = KafkaManager(
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol, api_version=api_version,
        ssl_context=ssl_context,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        sasl_kerberos_service_name=sasl_kerberos_service_name,
        request_timeout_ms=request_timeout_ms,
        connections_max_idle_ms=connections_max_idle_ms
    )

    if parse_version(manager.get_api_version()) < parse_version('0.11.0'):
        raise IncompatibleVersion(
            'Current version of library is not compatible with '
            'Kafka < 0.11.0.'
        )

    if 'kafka_sleep_time' in params:
        manager.kafka_sleep_time = params['kafka_sleep_time']
    if 'kafka_max_retries' in params:
        manager.kafka_max_retries = params['kafka_max_retries']

    if 'zookeeper' in params:
        manager.zk_configuration = get_zookeeper_configuration(params)
    if 'zookeeper_sleep_time' in params:
        manager.zookeeper_sleep_time = params['zookeeper_sleep_time']
    if 'zookeeper_max_retries' in params:
        manager.zookeeper_max_retries = params['zookeeper_max_retries']

    return manager


def maybe_clean_kafka_ssl_files(params):

    ssl_cafile = params['ssl_cafile']
    ssl_certfile = params['ssl_certfile']
    ssl_keyfile = params['ssl_keyfile']
    ssl_crlfile = params['ssl_crlfile']

    kafka_ssl_files = generate_ssl_object(
        ssl_cafile, ssl_certfile, ssl_keyfile, ssl_crlfile
    )

    for _key, value in kafka_ssl_files.items():
        if (
                value['path'] is not None and value['is_temp'] and
                os.path.exists(os.path.dirname(value['path']))
        ):
            os.remove(value['path'])


def get_zookeeper_configuration(params):
    zookeeper = params['zookeeper']
    if zookeeper is not None:
        zookeeper_auth_scheme = params['zookeeper_auth_scheme']
        zookeeper_auth_value = params['zookeeper_auth_value']
        zookeeper_ssl_check_hostname = params['zookeeper_ssl_check_hostname']
        zookeeper_ssl_cafile = params['zookeeper_ssl_cafile']
        zookeeper_ssl_certfile = params['zookeeper_ssl_certfile']
        zookeeper_ssl_keyfile = params['zookeeper_ssl_keyfile']
        zookeeper_ssl_password = params['zookeeper_ssl_password']

        zookeeper_ssl_files = generate_ssl_object(
          zookeeper_ssl_cafile, zookeeper_ssl_certfile,
          zookeeper_ssl_keyfile
        )
        zookeeper_use_ssl = bool(
            zookeeper_ssl_files['keyfile']['path'] is not None and
            zookeeper_ssl_files['certfile']['path'] is not None
        )

        zookeeper_auth = []
        if zookeeper_auth_value != '':
            auth = (zookeeper_auth_scheme, zookeeper_auth_value)
            zookeeper_auth.append(auth)

        return dict(
            hosts=zookeeper,
            auth_data=zookeeper_auth,
            keyfile=zookeeper_ssl_files['keyfile']['path'],
            use_ssl=zookeeper_use_ssl,
            keyfile_password=zookeeper_ssl_password,
            certfile=zookeeper_ssl_files['certfile']['path'],
            ca=zookeeper_ssl_files['cafile']['path'],
            verify_certs=zookeeper_ssl_check_hostname
        )
    return None


def maybe_clean_zk_ssl_files(params):

    if ('zookeeper_ssl_cafile' in params and
            'zookeeper_ssl_certfile' in params and
            'zookeeper_ssl_keyfile' in params):
        zookeeper_ssl_cafile = params['zookeeper_ssl_cafile']
        zookeeper_ssl_certfile = params['zookeeper_ssl_certfile']
        zookeeper_ssl_keyfile = params['zookeeper_ssl_keyfile']

        zookeeper_ssl_files = generate_ssl_object(
            zookeeper_ssl_cafile, zookeeper_ssl_certfile,
            zookeeper_ssl_keyfile
        )

        for _key, value in zookeeper_ssl_files.items():
            if (
                    value['path'] is not None and value['is_temp'] and
                    os.path.exists(os.path.dirname(value['path']))
            ):
                os.remove(value['path'])
