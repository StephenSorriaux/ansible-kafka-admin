# -*- coding: utf-8 -*-
import collections
import traceback

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

MATCH_ANY_RESOURCE = ACLResource(
    resource_type=ACLResourceType.ANY,
    operation=ACLOperation.ANY,
    permission_type=ACLPermissionType.ANY,
    pattern_type=ACLPatternType.ANY,
    name=None,
    principal=None,
    host=None
)


def build_acl(acl, operation):
    return ACLResource(
        resource_type=ACLResourceType.from_name(
            acl['acl_resource_type']
        ),
        operation=operation,
        permission_type=ACLPermissionType.from_name(
            acl['acl_permission']
        ),
        pattern_type=ACLPatternType.from_name(
            acl['acl_pattern_type']
        ),
        name=acl['name'],
        principal=acl['acl_principal'],
        host=acl['acl_host']
    )


def process_module_acl(module):
    params = module.params.copy()
    params['acls'] = [{
        'name': params['name'],
        'acl_operation': params['acl_operation'],
        'acl_operations': params['acl_operations'],
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
    changes = {}
    manager = None

    try:
        manager = get_manager_from_params(params)
        api_version = parse_version(manager.get_api_version())

        for acl in acls:

            if 'acl_operation' in acl and acl['acl_operation'] is not None:
                acl['acl_operations'] = [acl['acl_operation']]

            if acl['acl_operations'] is None:
                acl['acl_operations'] = ['any']

            if acl['acl_resource_type'].lower() == 'broker':
                module.deprecate(
                    'Usage of "broker" is deprecated, please use "cluster" '
                    'instead'
                )

        acls_marked_present = [
            build_acl(acl, ACLOperation.from_name(operation))
            for acl in acls for operation in acl['acl_operations']
            if acl['state'] == 'present'
        ]

        acls_marked_absent = [
            build_acl(acl, ACLOperation.from_name(operation))
            for acl in acls for operation in acl['acl_operations']
            if acl['state'] == 'absent'
        ]

        # Check for duplicated acls
        duplicated_acls = [
            acl for acl, count in collections.Counter(
                acls_marked_absent + acls_marked_present).items() if count > 1]

        if len(duplicated_acls) > 0:
            module.fail_json(
                msg='Got duplicated acls in \'acls\': %s' % duplicated_acls
            )
            return

        acl_resource = build_acl(acls[0], ACLOperation.ANY) \
            if len(acls) == 1 else MATCH_ANY_RESOURCE

        acl_resource_found = manager.describe_acls(
            acl_resource, api_version
        )

        acls_to_add = [acl for acl in acls_marked_present
                       if acl not in acl_resource_found]

        # loop over acl_resource_found instead of acls_marked_absent
        # this allow to delete correct and specific operations
        #   in case of ANY matching
        acls_to_delete = [acl for acl in acl_resource_found
                          if acl in acls_marked_absent]

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
            changes.update({
                'acls_added': [acl.to_dict() for acl in acls_to_add]
            })
        if len(acls_to_delete) > 0:
            if not module.check_mode:
                manager.delete_acls(acls_to_delete, api_version)
            changed = True
            msg += ''.join(['acl %s successfully deleted. ' %
                            acl for acl in acls_to_delete])
            changes.update({
                'acls_deleted': [acl.to_dict() for acl in acls_to_delete]
            })
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Unable to initialize Kafka manager: %s' % e
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Something went wrong: (%s) %s' % (e, traceback.format_exc(e)),
            changes=changes
        )
    finally:
        if manager:
            manager.close()
        maybe_clean_kafka_ssl_files(params)
        maybe_clean_zk_ssl_files(params)

    if not changed:
        msg += 'nothing to do.'

    if warn is not None:
        module.warn(warn)

    module.exit_json(changed=changed, msg=msg, changes=changes)
