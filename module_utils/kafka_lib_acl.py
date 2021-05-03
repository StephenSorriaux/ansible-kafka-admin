# -*- coding: utf-8 -*-
import collections

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
    params = module.params.copy()
    params['acls'] = [{
        'name': params['name'],
        'acl_operation': params['acl_operation'],
        'acl_permission': params['acl_permission'],
        'acl_pattern_type': params['acl_pattern_type'],
        'acl_host': params['acl_host'],
        'acl_principal': params['acl_principal'],
        'acl_resource_type': params['acl_resource_type'],
        'state': params['state']
    }]

    process_module_acls(module, params)


def process_module_acls(module, params=None):
    params = params if params is not None else module.params

    acls = params['acls']
    mark_others_as_absent = params.get('mark_others_as_absent', False)

    changed = False
    msg = ''
    warn = None

    try:
        manager = get_manager_from_params(params)
        api_version = parse_version(manager.get_api_version())

        for acl in acls:
            if not acl['acl_operation']:
                module.fail_json(msg="acl_operation is required")

            if acl['acl_resource_type'].lower() == 'broker':
                module.deprecate(
                    'Usage of "broker" is deprecated, please use "cluster" '
                    'instead'
                )

        if len(acls) > 1:
            acl_resource = ACLResource(
                resource_type=ACLResourceType.ANY,
                operation=ACLOperation.ANY,
                permission_type=ACLPermissionType.ANY,
                pattern_type=ACLPatternType.ANY,
                name=None,
                principal=None,
                host="*"
            )
        else:
            acl = acls[0]

            acl_name = acl['name']
            acl_resource_type = acl['acl_resource_type']
            acl_principal = acl['acl_principal']
            acl_operation = acl['acl_operation']
            acl_permission = acl['acl_permission']
            acl_pattern_type = acl['acl_pattern_type']
            acl_host = acl['acl_host']

            acl_resource = ACLResource(
                resource_type=ACLResourceType.from_name(acl_resource_type),
                operation=ACLOperation.from_name(acl_operation),
                permission_type=ACLPermissionType.from_name(
                    acl_permission
                ),
                pattern_type=ACLPatternType.from_name(acl_pattern_type),
                name=acl_name,
                principal=acl_principal,
                host=acl_host
            )
        acl_resource_found = manager.describe_acls(
            acl_resource, api_version
        )

        acls_marked_present = [ACLResource(
            resource_type=ACLResourceType.from_name(acl['acl_resource_type']),
            operation=ACLOperation.from_name(acl['acl_operation']),
            permission_type=ACLPermissionType.from_name(
                acl['acl_permission']
            ),
            pattern_type=ACLPatternType.from_name(acl['acl_pattern_type']),
            name=acl['name'],
            principal=acl['acl_principal'],
            host=acl['acl_host']
        ) for acl in acls if acl['state'] == 'present']
        acls_marked_absent = [ACLResource(
            resource_type=ACLResourceType.from_name(acl['acl_resource_type']),
            operation=ACLOperation.from_name(acl['acl_operation']),
            permission_type=ACLPermissionType.from_name(
                acl['acl_permission']
            ),
            pattern_type=ACLPatternType.from_name(acl['acl_pattern_type']),
            name=acl['name'],
            principal=acl['acl_principal'],
            host=acl['acl_host']
        ) for acl in acls if acl['state'] == 'absent']

        # Check for duplicated acls
        duplicated_acls = [acl for acl, count in collections.Counter(
            acls_marked_absent + acls_marked_present
        ).items() if count > 1]
        if len(duplicated_acls) > 0:
            module.fail_json(
                msg='Got duplicated acls in \'acls\': %s' % duplicated_acls
            )
            return

        acls_to_add = [acl for acl in acls_marked_present
                       if acl not in acl_resource_found]
        acls_to_delete = [acl for acl in acls_marked_absent
                          if acl in acl_resource_found]

        # Cleanup others acls
        if mark_others_as_absent:
            acls_to_delete.extend(
                [acl for acl in acl_resource_found
                 if acl not in acls_marked_present + acls_marked_absent]
            )
        if len(acls_to_add) > 0:
            if not module.check_mode:
                manager.create_acls(acls_to_add, api_version)
            changed = True
            msg += ''.join(['acl %s successfully created. ' %
                            acl for acl in acls_to_add])
        if len(acls_to_delete) > 0:
            if not module.check_mode:
                manager.delete_acls(acls_to_delete, api_version)
            changed = True
            msg += ''.join(['acl %s successfully deleted. ' %
                            acl for acl in acls_to_delete])
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
