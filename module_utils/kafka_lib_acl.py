# -*- coding: utf-8 -*-
from pkg_resources import parse_version

from kafka.errors import KafkaError

from ansible.module_utils.pycompat24 import get_exception

from ansible.module_utils.kafka_acl import (
    ACLOperation, ACLPermissionType, ACLResourceType, ACLPatternType,
    ACLResource
)

from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files,
    maybe_clean_zk_ssl_files
)


def process_module_acl(module):
    params = module.params

    name = params['name']
    state = params['state']

    acl_resource_type = params['acl_resource_type']
    acl_principal = params['acl_principal']
    acl_operation = params['acl_operation']
    acl_permission = params['acl_permission']
    acl_pattern_type = params['acl_pattern_type']
    acl_host = params['acl_host']

    changed = False
    msg = 'acl \'%s\': ' % (name)
    warn = None

    try:
        manager = get_manager_from_params(params)
        if not acl_operation:
            module.fail_json(msg="acl_operation is required")

        api_version = parse_version(manager.get_api_version())

        if acl_resource_type.lower() == 'broker':
            module.deprecate(
                'Usage of "broker" is deprecated, please use "cluster" '
                'instead'
            )

        acl_resource = ACLResource(
            resource_type=ACLResourceType.from_name(acl_resource_type),
            operation=ACLOperation.from_name(acl_operation),
            permission_type=ACLPermissionType.from_name(
              acl_permission
            ),
            pattern_type=ACLPatternType.from_name(acl_pattern_type),
            name=name,
            principal=acl_principal,
            host=acl_host
        )

        acl_resource_found = manager.describe_acls(
            acl_resource, api_version
        )

        if state == 'present':
            if not acl_resource_found:
                if not module.check_mode:
                    manager.create_acls([acl_resource], api_version)
                changed = True
                msg += 'successfully created.'
        elif state == 'absent':
            if acl_resource_found:
                if not module.check_mode:
                    manager.delete_acls([acl_resource], api_version)
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
