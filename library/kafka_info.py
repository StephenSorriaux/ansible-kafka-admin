#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for Kafka information
"""
from __future__ import absolute_import, division, print_function

__metaclass__ = type

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
    choices: [topic, broker, consumer_group, acl]
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
                choices=['topic', 'broker', 'consumer_group', 'acl'],
                required=True
            ),
            **module_commons
        )
    )

    params = module.params
    resource = params['resource']

    try:
        manager = get_manager_from_params(params)
        results = manager.get_resource(resource)
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Error while getting %s from Kafka: %s ' % (resource, e)
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Seomthing went wrong: %s ' % e
        )
    finally:
        manager.close()
        maybe_clean_kafka_ssl_files(params)

    module.exit_json(changed=True, results=results)


if __name__ == '__main__':
    main()
