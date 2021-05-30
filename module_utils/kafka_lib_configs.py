# -*- coding: utf-8 -*-
from kafka.errors import KafkaError

from ansible.module_utils.pycompat24 import get_exception

from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files,
    maybe_clean_zk_ssl_files
)


def process_module_configs(module, params=None):
    params = params if params is not None else module.params

    configs = params['configs']

    changed = False
    msg = ''

    try:
        manager = get_manager_from_params(params)
        current_configs = manager.get_configs(configs)

        configuration_configs = {
            (config['resource_type'], config['resource_name']):
            config['options'] for config in configs
        }

        configs_to_add = {}

        # Add inexistant configs
        for resource, options in configuration_configs:
            current_options = current_configs[resource]

        configs_to_delete = {}

    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Unable to initialize Kafka manager: %s' % e
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Something went wrong: %s' % e
        )
    finally:
        manager.close()
        maybe_clean_kafka_ssl_files(params)
        maybe_clean_zk_ssl_files(params)

    if not changed:
        msg += 'nothing to do.'

    module.exit_json(changed=changed, msg=msg)
