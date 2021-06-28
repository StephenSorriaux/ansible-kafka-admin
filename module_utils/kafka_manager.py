import itertools
import json
from pkg_resources import parse_version
import time

from ansible.module_utils.kafka_protocol import (
    AlterPartitionReassignmentsRequest_v0,
    ListPartitionReassignmentsRequest_v0,
    DescribeConfigsRequest_v1,
    DescribeClientQuotasRequest_v0,
    AlterClientQuotasRequest_v0
)
from kafka.client_async import KafkaClient
from kazoo.client import KazooClient
from kafka.protocol.admin import (
    CreateTopicsRequest_v0,
    DeleteTopicsRequest_v0,
    CreateAclsRequest_v0,
    CreateAclsRequest_v1,
    DeleteAclsRequest_v0,
    DeleteAclsRequest_v1,
    DescribeAclsRequest_v0,
    DescribeAclsRequest_v1,
    ListGroupsRequest_v2,
    DescribeGroupsRequest_v0,
    DescribeConfigsRequest_v0,
    CreatePartitionsRequest_v0,
    AlterConfigsRequest_v0
)
from kafka.protocol.offset import (
    OffsetRequest_v1
)
from kafka.protocol.group import MemberAssignment, ProtocolMetadata
import kafka.errors
from kafka.errors import IllegalArgumentError

from ansible.module_utils.kafka_acl import (
    ACLOperation, ACLPermissionType, ACLResourceType, ACLPatternType,
    ACLResource
)
from ansible.module_utils.kafka_lib_errors import (
    KafkaManagerError, UndefinedController, ReassignPartitionsTimeout,
    UnableToRefreshState, MissingConfiguration, ZookeeperBroken
)


class KafkaManager:
    """
    A class used to interact with Kafka and Zookeeper
    and easily retrive useful information
    """

    MAX_RETRY = 10
    MAX_POLL_RETRIES = 3
    MAX_ZK_RETRIES = 5
    TOPIC_RESOURCE_ID = 2
    DEFAULT_TIMEOUT = 15000
    SUCCESS_CODE = 0
    ZK_REASSIGN_NODE = '/admin/reassign_partitions'
    ZK_TOPIC_PARTITION_NODE = '/brokers/topics/'
    ZK_TOPIC_CONFIGURATION_NODE = '/config/topics/'

    KFK_LATEST_OFFSET = -1
    KFK_EARLIEST_OFFSET = -2

    # Not used yet.
    ZK_TOPIC_DELETION_NODE = '/admin/delete_topics/'

    def __init__(self, **configs):
        self.zk_client = None
        self.zk_configuration = None
        self.zookeeper_sleep_time = 5
        self.zookeeper_max_retries = 5
        self.kafka_sleep_time = 5
        self.kafka_max_retries = 5
        self.client = KafkaClient(**configs)
        self.refresh()

    def init_zk_client(self):
        """
        Zookeeper client initialization
        """
        if (self.zk_configuration is None or
                self.zk_configuration['hosts'] == ''):
            raise MissingConfiguration(
                '\'zookeeper\', parameter is needed when '
                'parameter \'state\' is \'present\' for resource '
                '\'topic\'.'
            )
        try:
            self.zk_client = KazooClient(**self.zk_configuration)
            self.zk_client.start()
        except Exception as e:
            raise ZookeeperBroken(
                msg='Error while initializing Zookeeper client : '
                '%s. Is your Zookeeper server available and '
                'running on \'%s\'?' % (e, self.zk_configuration['hosts'])
            )

    def close_zk_client(self):
        """
        Closes Zookeeper client
        """
        self.zk_client.stop()

    def close_kafka_client(self):
        """
        Closes Kafka client
        """
        self.client.close()

    def close(self):
        """
        Closes any available client
        """
        self.close_kafka_client()
        if self.zk_client is not None:
            self.close_zk_client()

    def refresh(self):
        """
        Refresh topics state
        """
        fut = self.client.cluster.request_update()
        self.client.poll(future=fut)
        if not fut.succeeded():
            raise UnableToRefreshState(
                'Error while updating topic state from Kafka server: %s.'
                % fut.exception
            )

    def create_topics(self, topics, timeout=None):
        """
        Creates a topic
        Usable for Kafka version >= 0.10.1
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        request = CreateTopicsRequest_v0(
            create_topic_requests=[(
                topic['name'],
                topic['partitions'],
                topic['replica_factor'],
                topic['replica_assignment']
                if 'replica_assignment' in topic else [],
                topic['options'].items() if 'options' in topic else []
            ) for topic in topics],
            timeout=timeout
        )
        response = self.send_request_and_get_response(request)

        for topic, error_code in response.topic_errors:
            if error_code != self.SUCCESS_CODE:
                raise KafkaManagerError(
                    'Error while creating topic %s. '
                    'Error key is %s, %s.' % (
                        topic, kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description
                    )
                )

    def delete_topics(self, topics, timeout=None):
        """
        Deletes a topic
        Usable for Kafka version >= 0.10.1
        Need to know which broker is controller for topic
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        request = DeleteTopicsRequest_v0(topics=[topic['name']
                                                 for topic in topics],
                                         timeout=timeout)
        response = self.send_request_and_get_response(request)

        for topic, error_code in response.topic_error_codes:
            if error_code != self.SUCCESS_CODE:
                raise KafkaManagerError(
                    'Error while deleting topic %s. Error key is: %s, %s. '
                    'Is option \'delete.topic.enable\' set to true on '
                    ' your Kafka server?' % (
                        topic, kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description
                    )
                )

    @staticmethod
    def _convert_create_acls_resource_request_v0(acl_resource):
        if acl_resource.operation == ACLOperation.ANY:
            raise IllegalArgumentError("operation must not be ANY")
        if acl_resource.permission_type == ACLPermissionType.ANY:
            raise IllegalArgumentError("permission_type must not be ANY")

        return (
            acl_resource.resource_type,
            acl_resource.name,
            acl_resource.principal,
            acl_resource.host,
            acl_resource.operation,
            acl_resource.permission_type
        )

    @staticmethod
    def _convert_create_acls_resource_request_v1(acl_resource):
        if acl_resource.operation == ACLOperation.ANY:
            raise IllegalArgumentError("operation must not be ANY")
        if acl_resource.permission_type == ACLPermissionType.ANY:
            raise IllegalArgumentError("permission_type must not be ANY")

        return (
            acl_resource.resource_type,
            acl_resource.name,
            acl_resource.pattern_type,
            acl_resource.principal,
            acl_resource.host,
            acl_resource.operation,
            acl_resource.permission_type
        )

    @staticmethod
    def _convert_delete_acls_resource_request_v0(acl_resource):
        return (
            acl_resource.resource_type,
            acl_resource.name,
            acl_resource.principal,
            acl_resource.host,
            acl_resource.operation,
            acl_resource.permission_type
        )

    @staticmethod
    def _convert_delete_acls_resource_request_v1(acl_resource):
        return (
            acl_resource.resource_type,
            acl_resource.name,
            acl_resource.pattern_type,
            acl_resource.principal,
            acl_resource.host,
            acl_resource.operation,
            acl_resource.permission_type
        )

    def describe_acls(self, acl_resource, api_version):
        """Describe a set of ACLs
        """

        if api_version < parse_version('2.0.0'):
            request = DescribeAclsRequest_v0(
                resource_type=acl_resource.resource_type,
                resource_name=acl_resource.name,
                principal=acl_resource.principal,
                host=acl_resource.host,
                operation=acl_resource.operation,
                permission_type=acl_resource.permission_type
            )
        else:
            request = DescribeAclsRequest_v1(
                resource_type=acl_resource.resource_type,
                resource_name=acl_resource.name,
                resource_pattern_type_filter=acl_resource.pattern_type,
                principal=acl_resource.principal,
                host=acl_resource.host,
                operation=acl_resource.operation,
                permission_type=acl_resource.permission_type
            )

        response = self.send_request_and_get_response(request)

        if response.error_code != self.SUCCESS_CODE:
            raise KafkaManagerError(
                'Error while describing ACL %s. Error %s: %s.' % (
                    acl_resource, response.error_code,
                    response.error_message
                )
            )

        acl_list = []
        for resources in response.resources:
            if api_version < parse_version('2.0.0'):
                resource_type, resource_name, acls = resources
                resource_pattern_type = ACLPatternType.LITERAL.value
            else:
                resource_type, resource_name, resource_pattern_type, acls = \
                    resources
            for acl in acls:
                principal, host, operation, permission_type = acl
                conv_acl = ACLResource(
                    principal=principal,
                    host=host,
                    operation=ACLOperation(operation),
                    permission_type=ACLPermissionType(permission_type),
                    name=resource_name,
                    pattern_type=ACLPatternType(resource_pattern_type),
                    resource_type=ACLResourceType(resource_type),
                )
                acl_list.append(conv_acl)

        return acl_list

    def create_acls(self, acl_resources, api_version):
        """Create a set of ACLs"""

        if api_version < parse_version('2.0.0'):
            request = CreateAclsRequest_v0(
                creations=[self._convert_create_acls_resource_request_v0(
                    acl_resource) for acl_resource in acl_resources]
            )
        else:
            request = CreateAclsRequest_v1(
                creations=[self._convert_create_acls_resource_request_v1(
                    acl_resource) for acl_resource in acl_resources]
            )
        response = self.send_request_and_get_response(request)

        for error_code, error_message in response.creation_responses:
            if error_code != self.SUCCESS_CODE:
                raise KafkaManagerError(
                    'Error while creating ACL %s. Error %s: %s.' % (
                        acl_resources, error_code, error_message
                    )
                )

    def delete_acls(self, acl_resources, api_version):
        """Delete a set of ACLSs"""

        if api_version < parse_version('2.0.0'):
            request = DeleteAclsRequest_v0(
                filters=[self._convert_delete_acls_resource_request_v0(
                    acl_resource) for acl_resource in acl_resources]
            )
        else:
            request = DeleteAclsRequest_v1(
                filters=[self._convert_delete_acls_resource_request_v1(
                    acl_resource) for acl_resource in acl_resources]
            )

        response = self.send_request_and_get_response(request)

        for error_code, error_message, _ in response.filter_responses:
            if error_code != self.SUCCESS_CODE:
                raise KafkaManagerError(
                    'Error while deleting ACL %s. Error %s: %s.' % (
                        acl_resources, error_code, error_message
                    )
                )

    def send_request_and_get_response(self, request, node_id=None):
        """
        Sends a Kafka protocol request and returns
        the associated response
        """
        if node_id is None:
            try:
                node_id = self.get_controller()

            except UndefinedController:
                raise

            except Exception as e:
                raise KafkaManagerError(
                    'Cannot determine a controller for your current Kafka '
                    'server. Is your Kafka server running and available on '
                    '\'%s\' with security protocol \'%s\'? Are you using the '
                    'library versions from given \'requirements.txt\'? '
                    'Exception was: %s' % (
                        self.client.config['bootstrap_servers'],
                        self.client.config['security_protocol'],
                        e
                    )
                )

        if self.connection_check(node_id):
            future = self.client.send(node_id, request)
            while not future.succeeded():
                self.client.poll(future=future)

                if future.failed():
                    raise KafkaManagerError(
                        'Error while sending request %s to Kafka server: %s.'
                        % (request, future.exception)
                    )
            return future.value
        else:
            raise KafkaManagerError(
                'Connection is not ready, please check your client '
                'and server configurations.'
            )

    def get_controller(self):
        """
        Returns the current controller
        """
        if self.client.cluster.controller is not None:
            node_id, _host, _port, _rack = self.client.cluster.controller
            return node_id
        else:
            raise UndefinedController(
                'Cannot determine a controller for your current Kafka '
                'server. Is your Kafka server running and available on '
                '\'%s\' with security protocol \'%s\'?' % (
                    self.client.config['bootstrap_servers'],
                    self.client.config['security_protocol']
                )
            )

    def get_config_for_topics(self, topics):
        """
        Returns responses with configuration
        Usable with Kafka version >= 0.11.0
        """
        topics_configs = {}
        if parse_version(self.get_api_version()) < parse_version('1.1.0'):
            request = DescribeConfigsRequest_v0(
                resources=[
                    (
                        self.TOPIC_RESOURCE_ID, topic_name, None
                    ) for topic_name, _ in topics.items()]
            )
            kafka_config = self.send_request_and_get_response(request)
            for (error_code,
                 _,
                 _,
                 resource_name,
                 config_entries) in kafka_config.resources:
                current_config = {}
                for (config_names,
                     config_values,
                     _,
                     is_default,
                     _) in config_entries:
                    if not is_default:
                        current_config[config_names] = config_values

                topics_configs[resource_name] = current_config
        else:
            request = DescribeConfigsRequest_v1(
                resources=[
                    (
                        self.TOPIC_RESOURCE_ID, topic_name, None
                    ) for topic_name, _ in topics.items()]
            )
            kafka_config = self.send_request_and_get_response(request)
            for (error_code,
                 _,
                 _,
                 resource_name,
                 config_entries) in kafka_config.resources:
                current_config = {}
                for (config_names,
                     config_values,
                     _,
                     config_source,
                     _,
                     _) in config_entries:
                    # Dynamic topic config
                    if config_source == 1:
                        current_config[config_names] = config_values
                topics_configs[resource_name] = current_config
        return topics_configs

    def get_topics(self):
        """
        Returns the topics list
        """
        return self.client.cluster.topics()

    def get_total_partitions_for_topic(self, topic):
        """
        Returns the number of partitions for topic
        """
        return len(self.client.cluster.partitions_for_topic(topic))

    def get_partitions_for_topic(self, topic):
        """
        Returns all partitions for topic, with information
        TODO do not use private property anymore
        """
        return self.client.cluster._partitions[topic]

    def get_total_brokers(self):
        """
        Returns number of brokers available
        """
        return len(self.client.cluster.brokers())

    def get_brokers(self):
        """
        Returns all brokers
        """
        return self.client.cluster.brokers()

    def get_api_version(self):
        """
        Returns Kafka server version
        """
        major, minor, patch = self.client.config['api_version']
        return '%s.%s.%s' % (major, minor, patch)

    def connection_check(self, node_id, connection_sleep=0.1):
        """
        Checks that connection with broker is OK and that it is possible to
        send requests
        Since the maybe_connect() function used in ready() is 'async', we
        need to manually call the poll() function to establish the connection
        to the node
        """
        retries = 0
        if not self.client.ready(node_id):
            while retries < self.MAX_RETRY:
                self.client.poll()
                if self.client.ready(node_id):
                    return True
                time.sleep(connection_sleep)
                retries += 1
            return False
        return True

    def is_topics_configuration_need_update(self, topics):
        """
        Checks whether topic's options need to be updated or not.
        Since the DescribeConfigsRequest does not give all current
        configuration entries for a topic, we need to use Zookeeper.
        Requires zk connection.
        """
        current_topics_config = self.get_config_for_topics(topics)
        topics_need_update = []

        for topic_name, current_config in current_topics_config.items():
            if len(topics[topic_name]) != len(current_config.keys()):
                topics_need_update.append(topic_name)
            else:
                for conf_name, conf_value in topics[topic_name]:
                    if (
                            conf_name not in current_config.keys() or
                            str(conf_value) != str(current_config[conf_name])
                    ):
                        topics_need_update.append(topic_name)
        return topics_need_update

    def is_topics_partitions_need_update(self, topics):
        """
        Checks whether topic's partitions need to be updated or not.
        """
        topics_to_update = []
        for topic_name, partitions in topics.items():
            total_partitions = self.get_total_partitions_for_topic(topic_name)

            if partitions != total_partitions:
                if partitions > total_partitions:
                    # increasing partition number
                    topics_to_update.append(topic_name)
                else:
                    # decreasing partition number, which is not possible
                    raise KafkaManagerError(
                        'Can\'t update \'%s\' topic partition from %s to %s :'
                        'only increase is possible.' % (
                            topic_name, total_partitions, partitions
                        )
                    )

        return topics_to_update

    def is_topics_replication_need_update(self, topics):
        """
        Checks whether a topic replica needs to be updated or not.
        """
        topics_need_update = []

        for topic_name, replica_factor in topics.items():
            for _id, part in self.get_partitions_for_topic(topic_name).items():
                _topic, _partition, _leader, replicas, _isr, _error = part
                if len(replicas) != replica_factor:
                    topics_need_update.append(topic_name)

        return topics_need_update

    def update_topics_partitions(self, topics):
        """
        Updates the topic partitions
        Usable for Kafka version >= 1.0.0
        Requires to be the sended to the current controller of the Kafka
        cluster.
        The request requires to precise the total number of partitions and
        broker assignment for each new partition without forgeting replica.
        See NewPartitions class for explanations
        apache/kafka/clients/admin/NewPartitions.java#L53
        """
        brokers = []
        for node_id, _, _, _ in self.get_brokers():
            brokers.append(int(node_id))
        brokers_iterator = itertools.cycle(brokers)

        topics_assignments = []
        for topic_name, partitions in topics.items():
            topic, _, _, replicas, _, _ = (
                self.get_partitions_for_topic(topic_name)[0]
            )
            total_replica = len(replicas)
            old_partition = self.get_total_partitions_for_topic(topic_name)
            assignments = []
            for _new_partition in range(partitions - old_partition):
                assignment = []
                for _replica in range(total_replica):
                    assignment.append(next(brokers_iterator))
                assignments.append(assignment)
            topics_assignments.append(
                (topic_name, (partitions, assignments))
            )

        request = CreatePartitionsRequest_v0(
            topic_partitions=topics_assignments,
            timeout=self.DEFAULT_TIMEOUT,
            validate_only=False
        )
        response = self.send_request_and_get_response(request)
        for topic, error_code, _error_message in response.topic_errors:
            if error_code != self.SUCCESS_CODE:
                raise KafkaManagerError(
                    'Error while updating topic \'%s\' partitions. '
                    'Error key is %s, %s. Request was %s.' % (
                        topic, kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description,
                        request
                    )
                )
        self.refresh()

    def update_topics_configuration(self, topics):
        """
        Updates the topic configuration
        Usable for Kafka version >= 0.11.0
        Requires to be the sended to the current controller of the Kafka
        cluster.
        """
        request = AlterConfigsRequest_v0(
            resources=[
                (
                    self.TOPIC_RESOURCE_ID, topic_name, topic_conf
                )
                for topic_name, topic_conf in topics.items()],
            validate_only=False
        )
        response = self.send_request_and_get_response(request)
        for error_code, _, _, resource_name in response.resources:
            if error_code != self.SUCCESS_CODE:
                raise KafkaManagerError(
                    'Error while updating topic \'%s\' configuration. '
                    'Error key is %s, %s' % (
                        resource_name,
                        kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description
                    )
                )
        self.refresh()

    def get_assignment_for_replica_factor_update(self, topic_name,
                                                 replica_factor):
        """
        Generates a json assignment based on replica_factor given to update
        replicas for a topic.
        Uses all brokers available and distributes them as replicas using
        a round robin method.
        """
        all_replicas = []
        partitions = []

        if replica_factor > self.get_total_brokers():
            raise KafkaManagerError(
                'Error while updating topic \'%s\' replication factor : '
                'replication factor \'%s\' is more than available brokers '
                '\'%s\'' % (
                    topic_name,
                    replica_factor,
                    self.get_total_brokers()
                )
            )
        else:
            for node_id, _, _, _ in self.get_brokers():
                all_replicas.append(node_id)
            brokers_iterator = itertools.cycle(all_replicas)
            for _, part in self.get_partitions_for_topic(topic_name).items():
                _, partition, _, _, _, _ = part
                replicas = []
                for _i in range(replica_factor):
                    replicas.append(next(brokers_iterator))
                assign_tmp = (
                    partition,
                    replicas,
                    {}
                )
                partitions.append(assign_tmp)

            return [(topic_name, partitions, {})]

    def get_assignment_for_replica_factor_update_with_zk(self, topics):
        """
        Generates a json assignment based on replica_factor given to update
        replicas for a topic.
        Uses all brokers available and distributes them as replicas using
        a round robin method.
        """
        all_replicas = []
        assign = {'partitions': [], 'version': 1}

        for topic_name, replica_factor in topics.items():
            if replica_factor > self.get_total_brokers():
                raise KafkaManagerError(
                    'Error while updating topic \'%s\' replication factor : '
                    'replication factor \'%s\' is more than available brokers '
                    '\'%s\'' % (
                        topic_name,
                        replica_factor,
                        self.get_total_brokers()
                    )
                )
            else:
                for node_id, _, _, _ in self.get_brokers():
                    all_replicas.append(node_id)
                brokers_iterator = itertools.cycle(all_replicas)
                for _, part in self.get_partitions_for_topic(
                        topic_name).items():
                    _, partition, _, _, _, _ = part
                    assign_tmp = {
                        'topic': topic_name,
                        'partition': partition,
                        'replicas': []
                    }
                    for _i in range(replica_factor):
                        assign_tmp['replicas'].append(next(brokers_iterator))
                    assign['partitions'].append(assign_tmp)

            return json.dumps(assign, ensure_ascii=False).encode('utf-8')

    def get_assignment_for_partition_update(self, topic_name, partitions):
        """
        Generates a json assignment based on number of partitions given to
        update partitions for a topic.
        Uses all brokers available and distributes them among partitions
        using a round robin method.
        """
        all_brokers = []
        assign = {'partitions': {}, 'version': 1}

        _, _, _, replicas, _, _ = self.get_partitions_for_topic(
            topic_name)[0]
        total_replica = len(replicas)

        for node_id, _host, _port, _rack in self.get_brokers():
            all_brokers.append(node_id)
        brokers_iterator = itertools.cycle(all_brokers)

        for i in range(partitions):
            assign_tmp = []
            for _j in range(total_replica):
                assign_tmp.append(next(brokers_iterator))
            assign['partitions'][str(i)] = assign_tmp

        return json.dumps(assign, ensure_ascii=False).encode('utf-8')

    def wait_for_partition_assignement(self):
        """
        wait until all assignements is done.
        """
        retries = 0
        assignement_done = False
        while (
                not assignement_done and
                retries < self.kafka_max_retries
        ):
            request = ListPartitionReassignmentsRequest_v0(
                timeout_ms=60000,
                topics=None,
                tags={}
            )
            response = self.send_request_and_get_response(request)
            if len(response.topics) == 0:
                break
            retries += 1
            time.sleep(self.kafka_sleep_time)

        if retries >= self.kafka_max_retries:
            raise ReassignPartitionsTimeout(
                'Reassignement, is still in progress after %s tries,'
                'giving up. Consider increasing your `kafka_max_retries`'
                'and/or `kafka_sleep_time` parameters and check your'
                'cluster.',
                retries
            )

    def wait_for_znode_assignment(self):
        """
        Wait for the reassignment znode to be consumed by Kafka.

        Raises `ReassignPartitionsTimeout` if `zk_max_retries` is reached.
        """
        retries = 0
        while (
                self.zk_client.exists(self.ZK_REASSIGN_NODE) and
                retries < self.zookeeper_max_retries
        ):
            retries += 1
            time.sleep(self.zookeeper_sleep_time)

        if retries >= self.zookeeper_max_retries:
            raise ReassignPartitionsTimeout(
                'The znode %s, is still present after %s tries, giving up.'
                'Consider increasing your `zookeeper_max_retries` and/or '
                '`zookeeper_sleep_time` parameters and check your cluster.',
                self.ZK_REASSIGN_NODE,
                retries
            )

    def update_admin_assignments(self, topics):
        """
Updates the topic replica factor using a json assignment
Cf core/src/main/scala/kafka/admin/ReassignPartitionsCommand.scala#L580
 1 - Send AlterReplicaLogDirsRequest to allow broker to create replica in
     the right log dir later if the replica has not been created yet.

  2 - Create reassignment znode so that controller will send
      LeaderAndIsrRequest to create replica in the broker
      def path = "/admin/reassign_partitions" ->
      zk.create("/admin/reassign_partitions", b"a value")
  case class ReplicaAssignment(
    @BeanProperty @JsonProperty("topic") topic: String,
    @BeanProperty @JsonProperty("partition") partition: Int,
    @BeanProperty @JsonProperty("replicas") replicas: java.util.List[Int])
  3 - Send AlterReplicaLogDirsRequest again to make sure broker will start
      to move replica to the specified log directory.
     It may take some time for controller to create replica in the broker
     Retry if the replica has not been created.
 It may be possible that the node '/admin/reassign_partitions' is already
 there for another topic. That's why we need to check for its existence
 and wait for its consumption if it is already present.
 Requires zk connection.
        """
        if (parse_version(self.get_api_version()) >= parse_version('2.4.0')):
            assign = []
            for name, replica_factor in topics.items():
                assign += self.get_assignment_for_replica_factor_update(
                    name, replica_factor
                )
            request = AlterPartitionReassignmentsRequest_v0(
                timeout_ms=60000,
                topics=assign,
                tags={}
            )
            self.wait_for_partition_assignement()
            self.send_request_and_get_response(request)
            self.wait_for_partition_assignement()
        elif self.zk_configuration is not None:
            try:
                json_assignment = (
                    self.get_assignment_for_replica_factor_update_with_zk(
                        topics
                    )
                )
                self.init_zk_client()
                self.wait_for_znode_assignment()
                self.zk_client.create(self.ZK_REASSIGN_NODE, json_assignment)
                self.wait_for_znode_assignment()
            finally:
                self.close_zk_client()
        else:
            raise KafkaManagerError('Zookeeper is mandatory for partition assignment when \
            using Kafka <= 2.4.0.')
        self.refresh()

    def update_topic_assignment(self, json_assignment, zknode):
        """
 Updates the topic partition assignment using a json assignment
 Used when Kafka version < 1.0.0
 Requires zk connection.
        """
        try:
            self.init_zk_client()
            if not self.zk_client.exists(zknode):
                raise KafkaManagerError(
                    'Error while updating assignment: zk node %s missing. '
                    'Is the topic name correct?' % (zknode)
                )
            self.zk_client.set(zknode, json_assignment)
            self.refresh()
        finally:
            self.close_zk_client()

    @staticmethod
    def generate_consumer_groups_for_broker(broker, response):
        """
    From a `broker` and `response` generate a list of consumer groups
        """
        consumer_groups = {}
        for err, gid, gstate, prot_type, prot, _members in response.groups:
            members = {}
            for mid, cid, chost, mdata, assign in _members:
                mdata = ProtocolMetadata.decode(mdata)
                assign = MemberAssignment.decode(assign)
                assignment = {}
                for t, p in assign.assignment:
                    assignment[t] = p
                members[mid] = {
                    'client_id': cid,
                    'client_host': chost,
                    'member_metadata': {
                        'version': mdata.version,
                        'subscription': mdata.subscription,
                        'user_data': mdata.user_data.decode('utf-8')
                    },
                    'member_assignment': {
                        'version': assign.version,
                        'assignment': assignment,
                        'user_data': assign.user_data.decode('utf-8')
                    }
                }
            group = {
                'error_code': err,
                'group_state': gstate,
                'members': members,
                'protocol_type': prot_type,
                'protocol': prot,
                'coordinator': {
                    'host': broker.host,
                    'nodeId': broker.nodeId,
                    'port': broker.port,
                    'rack': broker.rack
                }
            }
            consumer_groups[gid] = group
        return consumer_groups

    def get_consumer_groups_resource(self):
        """
Return a dict object containing information about consumer groups and
following this structure:
{
    "AWESOME_consumer_group_1607465801": {
        "coordinator": {
            "host": "172.17.0.9",
            "nodeId": 1001,
            "port": 9092,
            "rack": null
        },
        "error_code": 0,
        "group_state": "Empty",
        "members": {},
        "protocol": "",
        "protocol_type": "consumer"
    },
    "AWESOME_consumer_group_1607466258": {
        "coordinator": {
            "host": "172.17.0.10",
            "nodeId": 1002,
            "port": 9092,
            "rack": null
        },
        "error_code": 0,
        "group_state": "Stable",
        "members": {
            "kafka-python-2.0.1-e5500fee-8df9-4f37-bcd7-788522a1c382": {
                "client_host": "/172.17.0.1",
                "client_id": "kafka-python-2.0.1",
                "member_assignment": {
                    "assignment": {
                        "test_1607465755": [
                            0
                        ]
                    },
                    "user_data": "",
                    "version": 0
                },
                "member_metadata": {
                    "subscription": [
                        "test_1607465755"
                    ],
                    "user_data": "",
                    "version": 0
                }
            }
        },
        "protocol": "range",
        "protocol_type": "consumer"
    }
}
        """
        consumer_groups = {}
        for broker in self.get_brokers():
            request = ListGroupsRequest_v2()
            response = self.send_request_and_get_response(
                request, node_id=broker.nodeId
            )
            if response.error_code != self.SUCCESS_CODE:
                raise KafkaManagerError(
                    'Error while list consumer groups of %s. '
                    'Error key is %s, %s.' % (
                        broker.nodeId,
                        kafka.errors.for_code(response.error_code).message,
                        kafka.errors.for_code(response.error_code).description
                    )
                )
            if response.groups:
                request = DescribeGroupsRequest_v0(
                    groups=tuple(
                        [group for group, protocol in response.groups]
                    )
                )
                response = self.send_request_and_get_response(
                    request, node_id=broker.nodeId
                )
                consumer_groups.update(
                    self.generate_consumer_groups_for_broker(broker, response)
                )

        return consumer_groups

    def get_brokers_resource(self):
        """
Return a dict object containing information about brokers and
following this structure:
{
    "1001": {
        "host": "172.17.0.9",
        "nodeId": 1001,
        "port": 9092,
        "rack": null
    },
    "1002": {
        "host": "172.17.0.10",
        "nodeId": 1002,
        "port": 9092,
        "rack": null
    }
}
        """
        brokers = {}
        for broker in self.get_brokers():
            brokers[broker.nodeId] = broker._asdict()
        return brokers

    def get_topics_resource(self):
        """
Return a dict object containing information about topics and partitions,
and following this structure:
{
    "test_1600378061": {
        "0": {
            "isr": [
                1002
            ],
            "leader": 1002,
            "replicas": [
                1002
            ],
            "earliest_offset": 12,
            "latest_offset": 15
        }
    }
}
        """
        topics = {}
        for topic in self.get_topics():
            topics[topic] = {}
            partitions = self.get_partitions_for_topic(topic)
            for partition, metadata in partitions.items():
                _, _, leader, replicas, isr, _ = metadata
                topics[topic][partition] = {
                    'leader': leader,
                    'replicas': replicas,
                    'isr': isr
                }

        for node in self.get_brokers():
            topics_partitions_leader = {
                k: v
                for k, v in {
                        topic_name: [
                            partition
                            for partition, partition_info in partitions.items()
                            if partition_info['leader'] == node.nodeId
                        ]
                        for topic_name, partitions in topics.items()
                }.items() if len(v) > 0
            }

            request = OffsetRequest_v1(replica_id=-1, topics=[
                (
                    topic_name,
                    [
                        (
                            partition,
                            self.KFK_EARLIEST_OFFSET
                         )
                        for partition in partitions
                    ]
                )
                for topic_name, partitions in topics_partitions_leader.items()
            ])
            response = self.send_request_and_get_response(
                request, node_id=node.nodeId).to_object()
            for topic in response['topics']:
                for partition in topic['partitions']:
                    if partition['error_code'] == 0:
                        topics[topic['topic']][
                            partition['partition']
                        ]['earliest_offset'] = partition['offset']
            request = OffsetRequest_v1(replica_id=-1, topics=[
                (
                    topic_name,
                    [
                        (
                            partition,
                            self.KFK_LATEST_OFFSET
                         )
                        for partition in partitions
                    ]
                )
                for topic_name, partitions in topics_partitions_leader.items()
            ])
            response = self.send_request_and_get_response(
                request, node_id=node.nodeId).to_object()
            for topic in response['topics']:
                for partition in topic['partitions']:
                    if partition['error_code'] == 0:
                        topics[topic['topic']][
                            partition['partition']
                        ]['latest_offset'] = partition['offset']
        return topics

    def get_acls_resource(self):
        """
Return a dict object containing information about acls, following this
structure:
{
    "<resource_type>": {
        "<resource_name>": [
            {
                "resource_type": <resource_type>,
                "operation": <operation>,
                "permission_type": <permission_type>,
                "resource_name": <resource_name>,
                "principal": <principal>,
                "host': <host>,
                "pattern_type": <pattern_type>
            }
        ]
    }
}
        """
        # match all ACLs
        acl_resource = ACLResource(
            ACLResourceType.ANY,
            ACLOperation.ANY,
            ACLPermissionType.ANY,
            pattern_type=ACLPatternType.ANY
        )
        api_version = parse_version(self.get_api_version())
        if api_version < parse_version('2.0.0'):
            request = DescribeAclsRequest_v0(
                resource_type=acl_resource.resource_type,
                resource_name=acl_resource.name,
                principal=acl_resource.principal,
                host=acl_resource.host,
                operation=acl_resource.operation,
                permission_type=acl_resource.permission_type
            )
        else:
            request = DescribeAclsRequest_v1(
                resource_type=acl_resource.resource_type,
                resource_name=acl_resource.name,
                resource_pattern_type_filter=acl_resource.pattern_type,
                principal=acl_resource.principal,
                host=acl_resource.host,
                operation=acl_resource.operation,
                permission_type=acl_resource.permission_type
            )

        response = self.send_request_and_get_response(request)
        if response.error_code != self.SUCCESS_CODE:
            raise KafkaManagerError(
                'Error while describing ACL %s. Error %s: %s.' % (
                    acl_resource, response.error_code,
                    response.error_message
                )
            )

        acl_results = {}
        for resources in response.resources:
            if api_version < parse_version('2.0.0'):
                resource_type, resource_name, acls = resources
                resource_pattern_type = ACLPatternType.LITERAL.value
            else:
                resource_type, resource_name, resource_pattern_type, acls = \
                    resources

            for acl in acls:
                principal, host, operation, permission_type = acl
                conv_acl = ACLResource(
                    principal=principal,
                    host=host,
                    operation=ACLOperation(operation),
                    permission_type=ACLPermissionType(permission_type),
                    name=resource_name,
                    pattern_type=ACLPatternType(resource_pattern_type),
                    resource_type=ACLResourceType(resource_type),
                )
                acl_results.setdefault(
                    conv_acl.resource_type.name.lower(), {}
                ).setdefault(
                    resource_name, []
                ).append(
                    conv_acl.to_dict()
                )

        return acl_results

    @property
    def resource_to_func(self):
        return {
            'topic': self.get_topics_resource,
            'broker': self.get_brokers_resource,
            'consumer_group': self.get_consumer_groups_resource,
            'acl': self.get_acls_resource
        }

    def get_resource(self, resource):
        if resource not in self.resource_to_func:
            raise ValueError('Unexpected resource "%s"' % resource)

        return self.resource_to_func[resource]()

    def ensure_topics(self, topics):
        topics_changed = set()
        warn = ''

        for topic in topics:
            partitions = topic['partitions']
            replica_factor = topic['replica_factor']
            if not (partitions > 0 and replica_factor > 0):
                # 0 or "default" (-1)
                warn += (
                    "Current values of 'partitions' (%s) and "
                    "'replica_factor' (%s) for %s does not let this lib to "
                    "perform any action related to partitions and "
                    "replication. SKIPPING." % (
                        partitions,
                        replica_factor,
                        topic['name']
                    )
                )

        topics = [
            topic for topic in topics if (
                topic['partitions'] > 0 and topic['replica_factor'] > 0)
        ]

        topics_config_need_update = self.is_topics_configuration_need_update({
            topic['name']: topic['options'].items()
            for topic in topics
        })
        if len(topics_config_need_update) > 0:
            self.update_topics_configuration({
                topic['name']: topic['options'].items()
                for topic in topics if (topic['name']
                                        in topics_config_need_update)
            })
            topics_changed.update(topics_config_need_update)

        topics_replication_need_update = \
            self.is_topics_replication_need_update({
                topic['name']: topic['replica_factor']
                for topic in topics
            })
        if len(topics_replication_need_update) > 0:
            self.update_admin_assignments({
                topic['name']: topic['replica_factor']
                for topic in topics if (topic['name']
                                        in topics_replication_need_update)
            })
            topics_changed.update(topics_replication_need_update)

        topics_partition_need_update = self.is_topics_partitions_need_update({
            topic['name']: topic['partitions']
            for topic in topics
        })
        if len(topics_partition_need_update) > 0:
            cur_version = parse_version(self.get_api_version())
            if cur_version < parse_version('1.0.0'):
                topics_partition_need_update = {
                    topic['name']: topic['partitions']
                    for topic in topics if (topic['name']
                                            in topics_partition_need_update)
                }
                for name, partitions in topics_partition_need_update.items():
                    json_assignment = (
                        self.get_assignment_for_partition_update
                        (name, partitions)
                    )
                    zknode = '/brokers/topics/%s' % name
                    self.update_topic_assignment(
                        json_assignment,
                        zknode
                    )
            else:
                self.update_topics_partitions({
                    topic['name']: topic['partitions']
                    for topic in topics
                })
            topics_changed.update(topics_partition_need_update)

        return topics_changed, warn

    @staticmethod
    def _map_to_quota_resources(entries):
        return [
            {
                'entity': [
                    {
                        'entity_type': entity['entity_type'],
                        'entity_name': entity['entity_name']
                    } for entity in entry['entity']
                ],
                'quotas': {
                    quota['name']: quota['value']
                    for quota in entry['values']
                }
            } for entry in entries]

    @staticmethod
    def _map_to_quota_request(entries):
        return AlterClientQuotasRequest_v0(entries=[
            (
                [(
                    entity['entity_type'],
                    entity['entity_name']
                ) for entity in entry['entity']],
                [(
                    key,
                    value,
                    True
                ) for key, value in entry['quotas_to_delete'].items()] +
                [(
                    key,
                    value,
                    False
                ) for key, value in entry['quotas_to_alter'].items()] +
                [(
                    key,
                    value,
                    False
                ) for key, value in entry['quotas_to_add'].items()]
            )
            for entry in entries
        ], validate_only=False)

    def describe_quotas(self):
        if parse_version(self.get_api_version()) >= parse_version('2.6.0'):
            request = DescribeClientQuotasRequest_v0(components=[],
                                                     strict=False)
            response = self.send_request_and_get_response(request)
            if response.error_code != 0:
                raise KafkaManagerError(response.error_message)
            current_quotas = response.to_object()['entries']
            return self._map_to_quota_resources(current_quotas)
        else:
            # Use zookeeper when kafka < 2.6.0
            try:
                self.init_zk_client()
                zknode = '/config'
                current_quotas = []
                if not self.zk_client.exists(zknode):
                    raise KafkaManagerError(
                        'Error while updating assignment: zk node %s missing. '
                        'Is the topic name correct?' % (zknode)
                    )
                # Get client-id quotas
                if self.zk_client.exists(zknode + '/clients'):
                    client_ids = self.zk_client.get_children(
                        zknode + '/clients')
                    for client_id in client_ids:
                        config, _ = self.zk_client.get(
                            zknode + '/clients/' + client_id)
                        current_quotas.append({
                            'entity': [{
                                'entity_type': 'client-id',
                                'entity_name': client_id
                            }],
                            'quotas': {key: float(value) for key, value
                                       in json.loads(
                                           config.decode('utf-8')
                                       )['config'].items()}
                        })
                # Get users quotas
                if self.zk_client.exists(zknode + '/users'):
                    users = self.zk_client.get_children(zknode + '/users')
                    for user in users:
                        # Only user
                        config, _ = self.zk_client.get(
                            zknode + '/users/' + user)
                        if config:
                            current_quotas.append({
                                'entity': [{
                                    'entity_type': 'user',
                                    'entity_name': user
                                }],
                                'quotas': {key: float(value) for key, value
                                           in json.loads(
                                               config.decode('utf-8')
                                           )['config'].items()}
                            })
                        if self.zk_client.exists(zknode + '/users/'
                                                 + user + '/clients'):
                            clients = self.zk_client.get_children(zknode
                                                                  + '/users/'
                                                                  + user
                                                                  + '/clients')
                            for client in clients:
                                config, _ = self.zk_client.get(
                                    zknode + '/users/' + user
                                    + '/clients/' + client)
                                current_quotas.append({
                                    'entity': [{
                                        'entity_type': 'user',
                                        'entity_name': user
                                    }, {
                                        'entity_type': 'client-id',
                                        'entity_name': client
                                    }],
                                    'quotas': {key: float(value) for
                                               key, value in
                                               json.loads(
                                                   config.decode('utf-8')
                                               )['config'].items()}
                                })
                return current_quotas
            finally:
                self.close_zk_client()

    def alter_quotas(self, quotas):
        if parse_version(self.get_api_version()) >= parse_version('2.6.0'):
            request = self._map_to_quota_request(quotas)
            response = self.send_request_and_get_response(request)
            response_entries = response.to_object()['entries']
            for response_entry in response_entries:
                if response_entry['error_code'] != 0:
                    raise KafkaManagerError(response_entry['error_message'])
        else:
            # Use zookeeper when kafka < 2.6.0
            try:
                self.init_zk_client()
                base_znode = '/config'
                for quota in quotas:
                    znode = ''
                    entity_description = {
                        entity['entity_type']: entity['entity_name']
                        for entity in quota['entity']
                    }
                    if 'user' in entity_description:
                        znode += '/users/' + entity_description['user']
                    if 'client-id' in entity_description:
                        znode += '/clients/' + entity_description['client-id']
                    if self.zk_client.exists(base_znode + znode):
                        node, _ = self.zk_client.get(base_znode + znode)
                        existing_node = json.loads(node.decode('utf-8'))
                        existing_node['config'].update(
                            quota['quotas_to_add'])
                        existing_node['config'].update(
                            quota['quotas_to_alter'])
                        existing_node['config'].update({
                            key: str(value)
                            for key, value in existing_node['config'].items()
                        })

                        for key, _ in quota['quotas_to_delete'].items():
                            del existing_node['config'][key]
                        self.zk_client.set(base_znode + znode,
                                           json.dumps(existing_node,
                                                      ensure_ascii=False)
                                           .encode('utf-8'))
                    else:
                        configs = dict()
                        configs.update(quota['quotas_to_add'])
                        configs.update(quota['quotas_to_alter'])
                        configs.update({
                            key: str(value)
                            for key, value in configs.items()
                        })
                        self.zk_client.create(base_znode + znode,
                                              json.dumps({
                                                  'version': 1,
                                                  'config': configs
                                              }, ensure_ascii=False)
                                              .encode('utf-8'),
                                              makepath=True)
                    # remove base path
                    self._zk_notify_config_update(znode[1:])
            finally:
                self.close_zk_client()

    def _zk_notify_config_update(self, entity_path):
        """
        Create a znode to notify brokers of configuration changes.
        """
        self.zk_client.create('/config/changes/config_change_',
                              json.dumps({
                                  'version': 2,
                                  'entity_path': entity_path
                              }, ensure_ascii=False).encode('utf-8'),
                              sequence=True)
