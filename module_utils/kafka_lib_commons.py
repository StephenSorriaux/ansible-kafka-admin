import os
from pkg_resources import parse_version

from ansible.module_utils.pycompat24 import get_exception

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
        - 'when using sasl, whether use PLAIN or GSSAPI.'
    default: PLAIN
    choices: [PLAIN, GSSAPI]
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
'''

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

    # only PLAIN is currently available
    sasl_mechanism=dict(choices=['PLAIN', 'GSSAPI'], default='PLAIN'),

    sasl_plain_username=dict(type='str', required=False),

    sasl_plain_password=dict(type='str', no_log=True, required=False),

    sasl_kerberos_service_name=dict(type='str', required=False),
)


def get_manager_from_params(module, params):

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

    api_version = tuple(
        int(p) for p in params['api_version'].strip(".").split(".")
    )

    kafka_ssl_files = generate_ssl_object(
        module, ssl_cafile, ssl_certfile, ssl_keyfile, ssl_crlfile
    )

    try:
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
            module=module, bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol, api_version=api_version,
            ssl_context=ssl_context,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name)

        if parse_version(manager.get_api_version()) < parse_version('0.11.0'):
            module.fail_json(
                msg='Current version of library is not compatible with '
                'Kafka < 0.11.0.'
            )

    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Error while initializing Kafka client: %s ' % str(e)
        )

    return manager


def maybe_clean_kafka_ssl_files(module, params):

    ssl_cafile = params['ssl_cafile']
    ssl_certfile = params['ssl_certfile']
    ssl_keyfile = params['ssl_keyfile']
    ssl_crlfile = params['ssl_crlfile']

    kafka_ssl_files = generate_ssl_object(
        module, ssl_cafile, ssl_certfile, ssl_keyfile, ssl_crlfile
    )

    for _key, value in kafka_ssl_files.items():
        if (
                value['path'] is not None and value['is_temp'] and
                os.path.exists(os.path.dirname(value['path']))
        ):
            os.remove(value['path'])
