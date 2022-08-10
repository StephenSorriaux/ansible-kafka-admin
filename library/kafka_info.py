#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for Kafka information
"""
from __future__ import absolute_import, division, print_function

__metaclass__ = type

from pkg_resources import parse_version

from ansible.module_utils.ansible_release import __version__ as ansible_version

# import module snippets
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.pycompat24 import get_exception
from kafka.errors import KafkaError

from ansible.module_utils.kafka_lib_commons import (
    module_commons, DOCUMENTATION_COMMON, get_manager_from_params,
    maybe_clean_kafka_ssl_files
)


ANSIBLE_METADATA = {'metadata_version': '1.0'}

DOCUMENTATION = '''
---
module: kafka_info
short_description: Gather Kafka information
description:
     - Gather Kafka information.
     - Not compatible with Kafka version < 0.11.0.
author:
    - Stephen SORRIAUX
options:
  resource:
    description:
      - 'the type of resource to get information about'
    required: True
    choices: [topic, broker, consumer_group, acl,
         topic-config]
  include_defaults:
    description:
      - 'Include defaults configuration when using topic-config resource'
    required: False
    default: True
  include_internal:
    description:
      - 'Include internal topics when using topic or topic-config resource'
    required: False
    default: False
''' + DOCUMENTATION_COMMON

EXAMPLES = '''
    - name: Get topics from Kafka cluster
      kafka_info:
        resource: "topic"
        bootstrap_servers: "{{ ansible_ssh_host }}:9094"
        api_version: "{{ kafka_api_version }}"
        sasl_mechanism: "PLAIN"
        security_protocol: "SASL_SSL"
        sasl_plain_username: "admin"
        sasl_plain_password: "{{ kafka_admin_password }}"
        ssl_check_hostname: False
        ssl_cafile: "{{ kafka_cacert | default('/etc/ssl/certs/cacert.crt') }}"
        ignore_empty_partition: True
    register: my_topics
'''


def main():
    module = AnsibleModule(
        argument_spec=dict(
            resource=dict(
                choices=[
                    'topic',
                    'broker',
                    'consumer_group',
                    'acl',
                    'topic-config',
                    'user'
                ],
                required=True
            ),
            include_defaults=dict(type='bool', default=True),
            include_internal=dict(type='bool', default=False),
            **module_commons
        )
    )

    params = module.params
    resource = params['resource']
    manager = None
    results = None

    try:
        manager = get_manager_from_params(params)
        results = manager.get_resource(resource, params)
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Error while getting %s from Kafka: %s ' % (resource, e)
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Something went wrong: %s ' % e
        )
    finally:
        if manager:
            manager.close()
        maybe_clean_kafka_ssl_files(params)

    # Ansible deprecate module 'results' key
    if parse_version(ansible_version) < parse_version('2.8.0'):
        module.exit_json(changed=True, results=results)
    else:
        module.exit_json(changed=True, ansible_module_results=results)


if __name__ == '__main__':
    main()
