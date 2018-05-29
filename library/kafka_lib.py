#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2018, Stephen SORRIAUX
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type

ANSIBLE_METADATA = {'metadata_version': '1.0'}


DOCUMENTATION = '''
---
module: kafka_lib
short_description: Manage Kafka topic
description:
     - Configure Kafka topic.
     - Not compatible avec Kafka version < 0.11.0.
author:
    - Stephen SORRIAUX
options:
  resource:
    description:
      - 'managed resource type.'
    default: topic
    choices: [topic] (more to come)
  name:
    description:
      - 'name of the managed resource.'
    required: True
  partition:
    description:
      - 'when resource = topic, number of partitions for this resource.'
  replica_factor:
    description:
      - 'when resource = topic, number of replica for the partitions of this resource.'
  state:
    description:
      - 'state of the managed resource.'
    default: present
    choices: [present, absent]
  options:
    description:
      - 'a dict with all options wanted for the managed resource'
      - 'Example: retention.ms: 7594038'
    type: dict
  zookeeper:
    description:
      - 'the zookeeper connection.'
  zookeeper_auth_scheme:
    description:
      - 'when zookeeper is configured to use authentication, schema used to connect to zookeeper.'
      default: 'digest'
      choices: [digest, sasl]
  zookeeper_auth_value:
    description:
      - 'when zookeeper is configured to use authentication, value used to connect.'
  bootstrap_servers:
    description:
      - 'kafka broker connection.'
      - 'format: host1:port,host2:port'
    required: True
  security_protocol:
    description:
      - 'how to connect to Kafka.'
     default: PLAINTEXT
     choices: [PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL]
  api_version:
    description:
      - 'kafka version'
      - 'format: major.minor.patch. Examples: 0.11.0 or 1.0.1'
      - 'if not set, will launch an automatic version discovery but can trigger stackstraces on Kafka server.'
    default: auto
  ssl_check_hostname:
    description:
      - 'when using ssl, check if certificate for hostname is correct.'
    default: True
  ssl_cafile:
    description:
      - 'when using ssl, content of ca cert file or path to ca cert file.'
  ssl_certfile:
    description:
      - 'when using ssl, content of cert file or path to server cert file.'
  ssl_keyfile:
    description:
      - 'when using ssl, content of keyfile or path to server cert key file.'
  ssl_password:
    description:
      - 'when using ssl, password for ssl_keyfile.'
  ssl_crlfile:
    description:
      - 'when using ssl, content of crl file or path to cert crl file.'
  sasl_mechanism:
    description:
      - 'when using sasl, whether use PLAIN or GSSAPI.'
    default: PLAIN
    choices: [PLAIN, GSSAPI]
  sasl_plain_username:
    description:
      - 'when using security_protocol = ssl, username to use.'
  sasl_plain_password:
    description:
      - 'when using security_protocol = ssl, password for sasl_plain_username.'
  sasl_kerberos_service_name:
    description:
      - 'when using kerberos, service name.'
'''

EXAMPLES = '''

    # creates a topic 'test' with provided configuation for plaintext configured Kafka and Zookeeper
    - name: create topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        partitions: 2
        replica_factor: 1
        options:
          retention.ms: 574930
          flush.ms: 12345
        state: 'present'
        zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # creates a topic for a sasl_ssl configured Kafka and plaintext Zookeeper
    - name: create topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        partitions: 2
        replica_factor: 1
        options:
          retention.ms: 574930
          flush.ms: 12345
        state: 'present'
        zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"
        security_protocol: 'SASL_SSL'
        sasl_plain_username: 'username'
        sasl_plain_password: 'password'
        ssl_cafile: '{{ content_of_ca_cert_file_or_path_to_ca_cert_file }}'

    # creates a topic for a plaintext configured Kafka and a digest authentication Zookeeper
    - name: create topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        partitions: 2
        replica_factor: 1
        options:
          retention.ms: 574930
          flush.ms: 12345
        state: 'present'
        zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
        zookeeper_auth_scheme: "digest"
        zookeeper_auth_value: "username:password"
        bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # deletes a topic
    - name: delete topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        state: 'absent'
        zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # deletes a topic using automatic api_version discovery
    - name: delete topic
      kafka_lib:
        resource: 'topic'
        name: 'test'
        state: 'absent'
        zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

'''

# import module snippets
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.six import b
from ansible.module_utils._text import to_bytes, to_native
from ansible.module_utils.pycompat24 import get_exception

import os
import time
import itertools
import json
import tempfile
from tempfile import mkstemp, mkdtemp
from os import close
from pkg_resources import parse_version

from kafka.client import KafkaClient
from kafka.protocol.api import Request, Response
from kafka.protocol.metadata import *
from kafka.protocol.admin import *
from kafka.protocol.types import Array, Boolean, Int8, Int16, Int32, Schema, String

from kazoo.client import KazooClient

## KAFKA PROTOCOL RESPONSES DEFINITION

class AlterConfigsResponse_v0(Response):
    API_KEY = 33
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('resources', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('resource_type', Int8),
            ('resource_name', String('utf-8'))))
    )

class DescribeConfigsResponse_v0(Response):
    API_KEY = 32
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('resources', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_entries', Array(
                ('config_name', String('utf-8')),
                ('config_value', String('utf-8')),
                ('read_only', Boolean),
                ('is_default', Boolean),
                ('is_sensitive', Boolean)))))
    )

class DescribeConfigsResponse_v1(Response):
    API_KEY = 32
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('resources', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_entries', Array(
                ('config_name', String('utf-8')),
                ('config_value', String('utf-8')),
                ('read_only', Boolean),
                ('config_source', Int8),
                ('is_sensitive', Boolean),
                ('config_synonyms', Array(
                    ('config_name', String('utf-8')),
                    ('config_value', String('utf-8')),
                    ('config_source', Int8)
                ))))))
    )

class LeaderAndIsrResponse_v0(Response):
    API_KEY = 4
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('partitions', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
            ('error_code', Int16)))
    )

class LeaderAndIsrResponse_v1(Response):
    API_KEY = 4
    API_VERSION = 1
    SCHEMA = LeaderAndIsrResponse_v0.SCHEMA

## KAFKA PROTOCOL REQUESTS DEFINITION

class DescribeConfigsRequest_v0(Request):
    API_KEY = 32
    API_VERSION = 0
    RESPONSE_TYPE = DescribeConfigsResponse_v0
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_names', Array(String('utf-8')))))
    )

class DescribeConfigsRequest_v1(Request):
    API_KEY = 32
    API_VERSION = 1
    RESPONSE_TYPE = DescribeConfigsResponse_v1
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_names', Array(String('utf-8'))))),
        ('include_synonyms', Boolean)
    )

class AlterConfigsRequest_v0(Request):
    API_KEY = 33
    API_VERSION = 0
    RESPONSE_TYPE = AlterConfigsResponse_v0
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_entries', Array(
                ('config_name', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('validate_only', Boolean)
    )

class LeaderAndIsrRequest_v0(Request):
    API_KEY = 4
    API_VERSION = 0
    RESPONSE_TYPE = LeaderAndIsrResponse_v0
    SCHEMA = Schema(
        ('controller_id', Int32),
        ('controller_epoch', Int32),
        ('partition_states', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
            ('controller_epoch', Int32),
            ('leader', Int32),
            ('leader_epoch', Int32),
            ('isr', Int32),
            ('zk_version', Int32),
            ('replicas', Int32))),
        ('live_leaders', Array(
            ('id', Int32),
            ('host', String('utf-8')),
            ('port', Int32)))
    )

class LeaderAndIsrRequest_v1(Request):
    API_KEY = 4
    API_VERSION = 1
    RESPONSE_TYPE = LeaderAndIsrResponse_v1
    SCHEMA = Schema(
        ('controller_id', Int32),
        ('controller_epoch', Int32),
        ('partition_states', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
            ('controller_epoch', Int32),
            ('leader', Int32),
            ('leader_epoch', Int32),
            ('isr', Int32),
            ('zk_version', Int32),
            ('replicas', Int32),
            ('is_new', Boolean))),
        ('live_leaders', Array(
            ('id', Int32),
            ('host', String('utf-8')),
            ('port', Int32)))
    )

class CreatePartitionsRequest_v0(Request):
    API_KEY = 37
    API_VERSION = 0
    RESPONSE_TYPE = CreatePartitionsResponse_v0
    SCHEMA = Schema(
        ('topic_partitions', Array(
            ('topic', String('utf-8')),
            ('new_partitions', Schema(
                ('count', Int32),
                ('assignment', Array(Array(Int32))))))),
        ('timeout', Int32),
        ('validate_only', Boolean)
    )

## A CLASS USED TO INTERACT WITH SERVER USING KAFKA PROTOCOL

class KafkaManager:

    MAX_RETRY = 10
    MAX_POLL_RETRIES = 3
    MAX_ZK_RETRIES = 5
    TOPIC_RESOURCE_ID = 2
    DEFAULT_TIMEOUT = 15000
    SUCCESS_CODE = 0
    ZK_REASSIGN_NODE = '/admin/reassign_partitions'
    ZK_TOPIC_PARTITION_NODE = '/brokers/topics/'
    ZK_TOPIC_CONFIGURATION_NODE = '/config/topics/'

    # Not used yet.
    ZK_TOPIC_DELETION_NODE = '/admin/delete_topics/'

    def __init__(self, module, **configs):
        self.module = module
        self.client = KafkaClient(**configs)


    def init_zk_client(self, zookeeper, zookeeper_auth=[]):
        self.zk_client = KazooClient(hosts=zookeeper, auth_data=zookeeper_auth, read_only=True)
        self.zk_client.start()

    def close_zk_client(self):
        self.zk_client.stop()

    def close(self):
        self.client.close()

    # Creates a topic
    # Usable for Kafka version >= 0.10.1
    def create_topic(self, name, partitions, replica_factor, replica_assignment = [], config_entries = [], timeout = None):
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        request = CreateTopicsRequest_v0(create_topic_requests=[(name,partitions,replica_factor,replica_assignment,config_entries)], timeout=timeout)
        resp = self.send_request_and_get_response(request)
        for request in resp:
            for topic, error_code in request.topic_error_codes:
                if (error_code != self.SUCCESS_CODE):
                    self.close()
                    self.module.fail_json(msg='Error while creating topic %s. Error key is %s' % (topic,error_code))

    # Deletes a topic
    # Usable for Kafka version >= 0.10.1
    # Need to know which broker is controller for topic
    def delete_topic(self, name, timeout = None):
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        request = DeleteTopicsRequest_v0(topics=[name], timeout=timeout)
        resp = self.send_request_and_get_response(request)
        for request in resp:
            for topic, error_code in request.topic_error_codes:
                if (error_code != self.SUCCESS_CODE):
                    self.close()
                    self.module.fail_json(msg='Error while deleting topic %s. Error key is %s. Is option \'delete.topic.enable\' set to true on your server?' % (topic,error_code))

    def send_request_and_get_response(self, request):
        node_id = self.get_controller()
        if self.connection_check(node_id):
            update = self.client.send(node_id,request)
            if update.is_done and update.failed():
                self.close()
                self.module.fail_json(msg='Error while sending request %s : %s.' % (request, update.exception))
            return self.get_responses_from_client()
        else:
            self.close()
            self.module.fail_json(msg='Connection is not ready, please check your client and server configurations.')


    def get_controller(self):
        nodeId, host, port, rack = self.client.cluster.controller
        return nodeId

    def get_controller_id_for_topic(self,topic_name):
        request = MetadataRequest_v1(topics=[topic_name])
        resp = self.send_request_and_get_response(request)
        for request in resp:
            controller_id = request.controller_id
        return controller_id


    # Returns responses with configuration
    # Usable with Kafka version >= 0.11.0
    def get_config_for_topic(self, topic_name, config_names):
        request = DescribeConfigsRequest_v0(resources=[(self.TOPIC_RESOURCE_ID,topic_name,config_names)])
        return self.send_request_and_get_response(request)

    # Obtains response from server using poll()
    # It may need some times to get the response, so we had some retries
    def get_responses_from_client(self, connection_sleep = 1):
        retries = 0
        if self.get_awaiting_request() > 0:
            while retries < self.MAX_POLL_RETRIES:
                resp = self.client.poll()
                if len(resp) > 0:
                    return resp
                time.sleep(connection_sleep)
                retries += 1
            self.close()
            self.module.fail_json(msg='Error while getting responses : no response to request was obtained, please check your client and server configurations.')
        else:
            self.close()
            self.module.fail_json(msg='No pending request, please check your client and server configurations.')

    # Returns the topics list
    def get_topics(self):
        return self.client.cluster.topics()

    # Returns the number of partitions for topic
    def get_total_partitions_for_topic(self, topic):
        return len(self.client.cluster.partitions_for_topic(topic))

    # Returns all partitions for topic, with information
    def get_partitions_for_topic(self, topic):
        return self.client.cluster._partitions[topic]

    # Returns number of brokers available
    def get_total_brokers(self):
        return len(self.client.cluster.brokers())

    # Returns all brokers
    def get_brokers(self):
        return self.client.cluster.brokers()

    # Returns Kafka server version
    def get_api_version(self):
        major, minor, patch = self.client.config['api_version']
        return '%s.%s.%s' % (major,minor,patch)

    # Returns the number of requests currently in the queue
    def get_awaiting_request(self):
        return self.client.in_flight_request_count()

    # Checks that connection with broker is OK and that it is possible to send requests
    # Since the _maybe_connect function is 'async', we need to manually call it several time to make the connection
    def connection_check(self, node_id, connection_sleep = 1):
        retries = 0
        if not self.client.is_ready(node_id):
            while retries < self.MAX_RETRY:
                if self.client.is_ready(node_id):
                    return True
                self.client._maybe_connect(node_id)
                time.sleep(connection_sleep)
                retries += 1
            return False
        else:
            return True

    # Checks whether topic's options need to be updated or not.
    # Since the DescribeConfigsRequest does not give all current configuration entries for a topic, we need to use Zookeeper.
    # Requires zk connection.
    def is_topic_configuration_need_update(self,topic_name, topic_conf):
        current_config, zk_stats = self.zk_client.get(self.ZK_TOPIC_CONFIGURATION_NODE + topic_name)
        current_config = json.loads(current_config)['config']

        if len(topic_conf) != len(current_config.keys()):
            return True
        else:
            for conf_name, conf_value in topic_conf:
                if conf_name not in current_config.keys() or str(conf_value) != str(current_config[conf_name]):
                    return True

        return False

    # Checks whether topic's partitions need to be updated or not.
    def is_topic_partitions_need_update(self,topic_name, partitions):
        total_partitions = self.get_total_partitions_for_topic(topic_name)
        need_update = False

        if partitions != total_partitions:
            if partitions > total_partitions:
                # increasing partition number
                need_update = True
            else:
                # decreasing partition number, which is not possible
                self.close()
                self.module.fail_json(msg='Can\'t update \'%s\' topic partition from %s to %s : only increase is possible.' % (topic_name,total_partitions,partitions))

        return need_update

    # Checks whether a topic replica needs to be updated or not.
    def is_topic_replication_need_update(self,topic_name, replica_factor):
        need_update = False
        for id, part in self.get_partitions_for_topic(topic_name).items():
            topic, partition, leader, replicas, isr, error = part
            if len(replicas) != replica_factor:
                need_update = True

        return need_update

    # Updates the topic partitions
    # Usable for Kafka version >= 1.0.0
    # Requires to be the sended to the current controller of the Kafka cluster.
    # The request requires to precise the total number of partitions and broker assignment for each new partition without forgeting replica.
    # See NewPartitions class for explanations https://github.com/apache/kafka/blob/a553764c8b1611cafd318022d0fc4a34c861f6ba/clients/src/main/java/org/apache/kafka/clients/admin/NewPartitions.java#L53
    def update_topic_partitions(self,topic_name,partitions):
        brokers = []
        for nodeId, host, port, rack in self.get_brokers():
            brokers.append(int(nodeId))
        brokers_iterator = itertools.cycle(brokers)
        topic, partition_id, leader, replicas, isr, error = self.get_partitions_for_topic(topic_name)[0]
        total_replica = len(replicas)
        old_partition = self.get_total_partitions_for_topic(topic_name)
        assignments = []
        for new_partition in range(partitions - old_partition):
            assignment = []
            for replica in range(total_replica):
                assignment.append(next(brokers_iterator))
            assignments.append(assignment)

        request = CreatePartitionsRequest_v0(topic_partitions=[(topic_name, (partitions,assignments))],timeout=self.DEFAULT_TIMEOUT,validate_only=False)
        resp = self.send_request_and_get_response(request)
        for request in resp:
            for topic, error_code, error_message in request.topic_errors:
                if (error_code != self.SUCCESS_CODE):
                    self.close()
                    self.module.fail_json(msg='Error while updating topic \'%s\' partitions. Error key is %s with message %s. Request was %s.' % (topic,error_code,error_message,str(request)))

    # Updates the topic configuration
    # Usable for Kafka version >= 0.11.0
    # Requires to be the sended to the current controller of the Kafka cluster.
    def update_topic_configuration(self,topic_name, topic_conf):
        request = AlterConfigsRequest_v0(resources=[(self.TOPIC_RESOURCE_ID,topic_name,topic_conf)],validate_only=False)
        resp = self.send_request_and_get_response(request)
        for request in resp:
            for error_code, error_message, resource_type, resource_name in request.resources:
                if (error_code != self.SUCCESS_CODE):
                    self.close()
                    self.module.fail_json(msg='Error while updating topic \'%s\' configuration. Error key is %s %s' % (resource_name,error_code,error_message))

    # Generates a json assignment based on replica_factor given to update replicas for a topic.
    # Uses all brokers available and distributes them as replicas using a round robin method.
    def get_assignment_for_replica_factor_update(self,topic_name, replica_factor):
        all_replicas = []
        assign = {'partitions': [], 'version': 1}

        if replica_factor > self.get_total_brokers():
            self.close()
            self.close_zk_client()
            self.module.fail_json(msg='Error while updating topic \'%s\' replication factor : replication factor \'%s\' is more than available brokers \'%s\'' % (topic_name,replica_factor,self.get_total_brokers()))
        else:
            for nodeId, host, port, rack in self.get_brokers():
                all_replicas.append(nodeId)
            brokers_iterator = itertools.cycle(all_replicas)
            for id, part in self.get_partitions_for_topic(topic_name).items():
                topic, partition, leader, replicas, isr, error = part
                assign_tmp = {'topic': topic_name, 'partition': partition, 'replicas': []}
                for i in range(replica_factor):
                    assign_tmp['replicas'].append(next(brokers_iterator))
                assign['partitions'].append(assign_tmp)

            return bytes(str(json.dumps(assign)))

    # Generates a json assignment based on number of partitions given to update partitions for a topic.
    # Uses all brokers available and distributes them among partitions using a round robin method.
    def get_assignment_for_partition_update(self,topic_name,partitions):
        all_brokers = []
        assign = {'partitions': {}, 'version': 1}

        topic, partition_id, leader, replicas, isr, error = self.get_partitions_for_topic(topic_name)[0]
        total_replica = len(replicas)

        for nodeId, host, port, rack in self.get_brokers():
            all_brokers.append(nodeId)
        brokers_iterator = itertools.cycle(all_brokers)

        for i in range(partitions):
            assign_tmp = []
            for j in range(total_replica):
                assign_tmp.append(next(brokers_iterator))
            assign['partitions'][str(i)] = assign_tmp

        return bytes(str(json.dumps(assign)))


    # Updates the topic replica factor using a json assignment
    # Cf https://github.com/apache/kafka/blob/98296f852f334067553e541d6ecdfa624f0eb689/core/src/main/scala/kafka/admin/ReassignPartitionsCommand.scala#L580
    # 1 - Send AlterReplicaLogDirsRequest to allow broker to create replica in the right log dir later if the replica has not been created yet.
    #
    # 2 - Create reassignment znode so that controller will send LeaderAndIsrRequest to create replica in the broker
    #     def path = "/admin/reassign_partitions" -> zk.create("/admin/reassign_partitions", b"a value")
    # case class ReplicaAssignment(@BeanProperty @JsonProperty("topic") topic: String,
                               #@BeanProperty @JsonProperty("partition") partition: Int,
                               #@BeanProperty @JsonProperty("replicas") replicas: java.util.List[Int])
    # 3 - Send AlterReplicaLogDirsRequest again to make sure broker will start to move replica to the specified log directory.
    #     It may take some time for controller to create replica in the broker. Retry if the replica has not been created.
    # It may be possible that the node '/admin/reassign_partitions' is already there for another topic. That's why we need to check for its existence and wait for its consumption if it is already present.
    # Requires zk connection.
    def update_admin_assignment(self,json_assignment, zk_sleep_time = 5):
        retries = 0
        while self.zk_client.exists(self.ZK_REASSIGN_NODE) and retries < self.MAX_ZK_RETRIES:
            retries += 1
            time.sleep(zk_sleep_time)
        if retries >= self.MAX_ZK_RETRIES:
            self.close()
            self.close_zk_client()
            self.module.fail_json(msg='Error while updating assignment: zk node %s is already there after %s retries and not yet consumed, giving up.' % (self.ZK_REASSIGN_NODE,self.MAX_ZK_RETRIES))
        self.zk_client.create(self.ZK_REASSIGN_NODE, json_assignment)

    # Updates the topic partition assignment using a json assignment
    # Used when Kafka version < 1.0.0
    # Requires zk connection.
    def update_topic_assignment(self,json_assignment, zknode):
        if not self.zk_client.exists(zknode):
            self.close()
            self.close_zk_client()
            self.module.fail_json(msg='Error while updating assignment: zk node %s missing. Is the topic name correct?' % (zknode))
        self.zk_client.set(zknode, json_assignment)


# TODO manage configuration deletion for topic
# TODO better errors handler

def main():

    module = AnsibleModule(
        argument_spec=dict(
            resource                    = dict(choices=['topic'], default='topic'), # resource managed, more to come (acl,broker)
            name                        = dict(type='str', required=True), # resource name
            partitions                  = dict(type='int', required=False, default=0), # currently required since only resource topic is available
            replica_factor              = dict(type='int', required=False, default=0), # currently required since only resource topic is available
            state                       = dict(choices=['present','absent'], default='present'),
            options                     = dict(required=False, type='dict', default=None),
            zookeeper                   = dict(type='str', required=False),
            zookeeper_auth_scheme       = dict(choices=['digest','sasl'], default='digest'),
            zookeeper_auth_value        = dict(type='str', no_log=True, required=False, default=''),
            bootstrap_servers           = dict(type='str',required=True),
            security_protocol           = dict(choices=['PLAINTEXT','SSL','SASL_SSL','SASL_PLAINTEXT'], default='PLAINTEXT'),
            api_version                 = dict(type='str',required=True, default=None),
            ssl_check_hostname          = dict(default=True, type='bool', required=False),
            ssl_cafile                  = dict(required=False, default=None, type='path'),
            ssl_certfile                = dict(required=False, default=None, type='path'),
            ssl_keyfile                 = dict(required=False, default=None, no_log=True, type='path'),
            ssl_password                = dict(type='str', no_log=True, required=False),
            ssl_crlfile                 = dict(required=False, default=None, type='path'),
            sasl_mechanism              = dict(choices=['PLAIN','GSSAPI'], default='PLAIN'), # only PLAIN is currently available
            sasl_plain_username         = dict(type='str',required=False),
            sasl_plain_password         = dict(type='str', no_log=True, required=False),
            sasl_kerberos_service_name  = dict(type='str',required=False),
        )
    )

    params = module.params

    resource = params['resource']
    name = params['name']
    partitions = params['partitions']
    replica_factor = params['replica_factor']
    state = params['state']
    zookeeper = params['zookeeper']
    zookeeper_auth_scheme = params['zookeeper_auth_scheme']
    zookeeper_auth_value = params['zookeeper_auth_value']
    bootstrap_servers = params['bootstrap_servers']
    security_protocol = params['security_protocol']
    ssl_check_hostname = params['ssl_check_hostname']
    ssl_cafile = params['ssl_cafile']
    ssl_certfile = params['ssl_certfile']
    ssl_keyfile = params ['ssl_keyfile']
    ssl_password = params['ssl_password']
    ssl_crlfile = params['ssl_crlfile']
    sasl_mechanism = params['sasl_mechanism']
    sasl_plain_username = params['sasl_plain_username']
    sasl_plain_password = params['sasl_plain_password']
    sasl_kerberos_service_name = params['sasl_kerberos_service_name']

    api_version = tuple(params['api_version'].strip(".").split("."))

    options = []
    if params['options'] != None:
        options = params['options'].items()

    ssl_files = {'cafile': { 'path': ssl_cafile, 'is_temp': False }, 'certfile': { 'path': ssl_certfile, 'is_temp': False }, 'keyfile': { 'path': ssl_keyfile, 'is_temp': False }, 'crlfile': { 'path': ssl_crlfile, 'is_temp': False } }
    for key,value in ssl_files.items():
        if value['path'] is not None:
            # TODO is that condition sufficient?
            if value['path'].startswith("-----BEGIN"):
                # value is a content, need to create a tempfile
                fd, path = tempfile.mkstemp(prefix=key)
                with os.fdopen(fd, 'w') as tmp:
                    tmp.write(value['path'])
                ssl_files[key]['path'] = path
                ssl_files[key]['is_temp'] = True
            elif not os.path.exists(os.path.dirname(value['path'])):
                # value is not a content, but path does not exist, fails the module
                module.fail_json(msg='%s is not a content and provided path does not exist, please check your SSL configuration.' % key)

    zookeeper_auth = []
    if zookeeper_auth_value != '':
        auth = (zookeeper_auth_scheme,zookeeper_auth_value)
        zookeeper_auth.append(auth)

    try:
        manager = KafkaManager(module=module, bootstrap_servers=bootstrap_servers, security_protocol=security_protocol, api_version=api_version, ssl_check_hostname=ssl_check_hostname, ssl_cafile=ssl_files['cafile']['path'], ssl_certfile=ssl_files['certfile']['path'], ssl_keyfile=ssl_files['keyfile']['path'], ssl_password=ssl_password, ssl_crlfile=ssl_files['crlfile']['path'], sasl_mechanism=sasl_mechanism, sasl_plain_username=sasl_plain_username, sasl_plain_password=sasl_plain_password, sasl_kerberos_service_name=sasl_kerberos_service_name)
    except Exception:
        e = get_exception()
        module.fail_json(msg='Error while initializing Kafka client : %s ' % str(e))

    changed = False

    if parse_version(manager.get_api_version()) < parse_version('0.11.0'):
        module.fail_json(msg='Current version of library is not compatible with Kafka < 0.11.0.')

    msg = '%s \'%s\': ' % (resource,name)

    if resource == 'topic':
        if state == 'present':
            if name in manager.get_topics():
                # topic is already there
                if zookeeper != '' and partitions > 0 and replica_factor > 0:
                    try:
                        manager.init_zk_client(zookeeper,zookeeper_auth)
                    except Exception:
                        e = get_exception()
                        module.fail_json(msg='Error while initializing Zookeeper client : %s ' % str(e))

                    if manager.is_topic_configuration_need_update(name,options):
                        manager.update_topic_configuration(name,options)
                        changed = True

                    if manager.is_topic_replication_need_update(name,replica_factor):
                        json_assignment = manager.get_assignment_for_replica_factor_update(name,replica_factor)
                        manager.update_admin_assignment(json_assignment)
                        changed = True

                    if manager.is_topic_partitions_need_update(name,partitions):
                        if parse_version(manager.get_api_version()) < parse_version('1.0.0'):
                            json_assignment = manager.get_assignment_for_partition_update(name,partitions)
                            zknode = '/brokers/topics/%s' % name
                            manager.update_topic_assignment(json_assignment,zknode)
                        else:
                            manager.update_topic_partitions(name,partitions)
                        changed = True
                    manager.close_zk_client()
                    if changed:
                        msg += 'successfully updated.'
                else:
                    module.fail_json(msg='\'zookeeper\', \'partitions\' and \'replica_factor\' parameters are needed when parameter \'state\' is \'present\'')
            else:
                # topic is absent
                manager.create_topic(name=name,partitions=partitions,replica_factor=replica_factor,config_entries=options)
                changed = True
                msg += 'successfully created.'
        elif state == 'absent':
            if name in manager.get_topics():
                # delete topic
                manager.delete_topic(name)
                changed = True
                msg += 'successfully deleted.'

    manager.close()
    for key,value in ssl_files.items():
        if value['path'] is not None and value['is_temp'] and os.path.exists(os.path.dirname(value['path'])):
            os.remove(value['path'])

    if not changed:
        msg += 'nothing to do.'

    module.exit_json(changed=changed, msg=msg)

if __name__ == '__main__':
    main()
