# -*- coding: utf-8 -*-
from kafka.errors import KafkaError

from ansible.module_utils.pycompat24 import get_exception

from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files, get_zookeeper_configuration,
    maybe_clean_zk_ssl_files
)


def process_module_topic(module):
    params = module.params

    name = params['name']
    partitions = params['partitions']
    replica_factor = params['replica_factor']
    state = params['state']
    zookeeper_sleep_time = params['zookeeper_sleep_time']
    zookeeper_max_retries = params['zookeeper_max_retries']

    options = []
    if params['options'] is not None:
        options = params['options'].items()

    zk_configuration = get_zookeeper_configuration(params)

    changed = False
    msg = 'topic \'%s\': ' % (name)
    warn = None

    try:
        manager = get_manager_from_params(params)

        if state == 'present':
            if name in manager.get_topics():
                if not module.check_mode:
                    changed, warn = manager.ensure_topic(
                        name, zk_configuration, options, partitions,
                        replica_factor, zookeeper_sleep_time,
                        zookeeper_max_retries
                    )
                    if changed:
                        msg += 'successfully updated.'
            else:
                if not module.check_mode:
                    manager.create_topic(
                        name=name, partitions=partitions,
                        replica_factor=replica_factor,
                        config_entries=options
                    )
                changed = True
                msg += 'successfully created.'

        elif state == 'absent':
            if name in manager.get_topics():
                if not module.check_mode:
                    manager.delete_topic(name)
                changed = True
                msg += 'successfully deleted.'

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

    if warn is not None:
        module.warn(warn)

    module.exit_json(changed=changed, msg=msg)
