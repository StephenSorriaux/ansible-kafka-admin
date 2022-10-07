# ansible-kafka-admin ![Build Status](https://github.com/StephenSorriaux/ansible-kafka-admin/workflows/Python%20package/badge.svg?branch=master) [![Plant Tree](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=Plant%20Tree&query=%24.total&url=https%3A%2F%2Fpublic.offset.earth%2Fusers%2Ftreeware%2Ftrees)](https://plant.treeware.earth/StephenSorriaux/ansible-kafka-admin)
A low level ansible library to manage Kafka configuration. It does not use the Kafka scripts and directly connect to Kafka and Zookeeper (if needed) to ensure resource creation. No ssh connection is needed to the remote host.

If you want to increase partitions, replication factor, change your topic's parameters, manage your ACLs and quotas without any effort, this library would be perfect for you.
## Available Modules
* kafka_lib (deprecated)
* [kafka_topic](library/kafka_topic.py): Manage kafka topic
* [kafka_topics](library/kafka_topics.py): Manage more than one topic in bulk mode
* [kafka_acl](library/kafka_acl.py): Manage kafka acl
* [kafka_acls](library/kafka_acls.py): Manage more than one acl in bulk mode
* [kafka_quotas](library/kafka_quotas.py): Manage quotas on user or client-id
* [kafka_info](library/kafka_info.py): Get infos on kafka resources
* [kafka_stat_lag](library/kafka_stat_lag.py): get lag info on topics / consumer groups
* [kafka_consumer_group](library/kafka_consumer_group.py): interact with kafka consumer groups
* [kafka_user](library/kafka_user.py): Manage kafka user
* [kafka_users](library/kafka_users.py): Manage more than one user in bulk mode
## Requirements
This library uses [kafka-python](https://github.com/dpkp/kafka-python), [kazoo](https://github.com/python-zk/kazoo) and [pure-sasl](https://github.com/thobbs/pure-sasl) libraries. Install them using pip:
```bash
pip install -r requirements.txt
```
**Please use only those versions as some updates might break the library.**

For now, this library is compatible with **Kafka in version 0.11.0 and more**.

It can be used with Kafka configured in PLAINTEXT, SASL_PLAINTEXT, SSL and SASL_SSL.

**Concerning Zookeeper**, it is not compatible with Kerberos authentication yet, only with SSL, SASL and DIGEST authentication.
## Need some help?
Come see us in our Slack workspace using this [invitation link](https://join.slack.com/t/ansible-kafka-admin/shared_invite/zt-rbnfiidg-84VbMa7US66ukqkBPTlADw).

## Installation
### As a role
Add the following requirement in your playbook's **requirements.yml**:
```yaml
---
# from GitHub, overriding the name and specifying a specific tag
- src: https://github.com/StephenSorriaux/ansible-kafka-admin
  name: kafka_lib
```
### As an ansible collection
Add the following requirement in your playbook's **requirements.yml**:
```yaml
---
# See avalable releases for VERSION
collections:
- name: https://github.com/StephenSorriaux/ansible-kafka-admin/releases/download/VERSION/StephenSorriaux-ansible_kafka_admin-VERSION.tar.gz
```
Then you can use it adding `StephenSorriaux.ansible_kafka_admin.` namespace prefix before any of defined modules.
## Usage
### Creating, updating, deleting topics and ACLs
**Note**:
Zookeeper is only needed when:
* replication-factor changed with Kafka < 2.4.0
* using kafka_quotas with Kafka < 2.6.0
* Kafka <= 0.11.0

See the [`examples`](examples) folder for real-life examples.

Here some examples on how to use this library:
```yaml
# creates two topics 'test' & 'test2' with provided configuation for plaintext configured Kafka and Zookeeper
- name: create topics
  kafka_topics:
    api_version: "1.0.1"
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"
    topics:
    - name: "test"
      partitions: 2
      replica_factor: 1
      options:
        retention.ms: 574930
        flush.ms: 12345
      state: "present"
    - name: "test2"
      partitions: 2
      replica_factor: 1
      options:
        retention.ms: 574930
        flush.ms: 12345
      state: "present"

# Force reassign on topics
- name: force reassign
  kafka_topics:
    api_version: "1.0.1"
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"
    topics:
    - name: "test"
      partitions: 2
      replica_factor: 1
      options:
        retention.ms: 574930
        flush.ms: 12345
      state: "present"
      force_reassign: True
      preserve_leader: True
    - name: "test2"
      partitions: 2
      replica_factor: 1
      options:
        retention.ms: 574930
        flush.ms: 12345
      state: "present"
      force_reassign: True
      preserve_leader: True

# creates a topic 'test' with provided configuation for plaintext configured Kafka and Zookeeper
- name: create topic
  kafka_topic:
    api_version: "1.0.1"
    name: "test"
    partitions: 2
    replica_factor: 1
    options:
      retention.ms: 574930
      flush.ms: 12345
    state: "present"
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# same as before but re-using default value for "flush.ms" (thus removing the specific config for topic)
- name: create topic
  kafka_topic:
    api_version: "1.0.1"
    name: "test"
    partitions: 2
    replica_factor: 1
    options:
      retention.ms: 574930
    state: "present"
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# from previous topic, update the number of partitions and the number of replicas. Be aware that this action can take some times to happen on Kafka
# so be sure to set the `zookeeper_max_retries` and `zookeeper_sleep_time` parameters to avoid hitting the timeout.
- name: update topic
  kafka_topic:
    api_version: "1.0.1"
    name: "test"
    partitions: 4
    replica_factor: 2
    options:
      retention.ms: 574930
    state: "present"
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# creates a topic for a sasl_ssl configured Kafka and plaintext Zookeeper
- name: create topic
  kafka_topic:
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
  kafka_topic:
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

# deletes topics
- name: delete topic
  kafka_topics:
    api_version: "1.0.1"
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"
    topics:
    - name: 'test'
      state: 'absent'
    - name: 'test2'
      state: 'absent'

# deletes a topic
- name: delete topic
  kafka_topic:
    api_version: "1.0.1"
    name: 'test'
    state: 'absent'
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# deletes a topic using automatic api_version discovery
- name: delete topic
  kafka_topic:
    name: 'test'
    state: 'absent'
    zookeeper: "{{ hostvars['zookeeper']['ansible_eth0']['ipv4']['address'] }}:2181"
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# create multiple users
- name: create users
  kafka_users:
    users:
      - name: user1
        password: changeit
        state: present

# create a single user
- name: create user
  kafka_user:
    name: user1
    password: changeit
    state: present


# create ACLs for all topics
- name: create acls
  kafka_acls:
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"
    acls:
    - acl_resource_type: 'topic'
      name: '*'
      acl_principal: 'User:Alice'
      acl_operation: 'write'
      acl_permission: 'allow'
      # Only with kafka api >= 2.0.0
      acl_pattern_type: 'literal'
      state: 'present'
    - acl_resource_type: 'topic'
      name: '*'
      acl_principal: 'User:Bob'
      acl_operation: 'write'
      acl_permission: 'allow'
      # Only with kafka api >= 2.0.0
      acl_pattern_type: 'literal'
      state: 'present'

# create an ACL for all topics
- name: create acl
  kafka_acl:
    acl_resource_type: 'topic'
    name: '*'
    acl_principal: 'User:Alice'
    acl_operation: 'write'
    acl_permission: 'allow'
    # Only with kafka api >= 2.0.0
    acl_pattern_type: 'literal'
    state: 'present'
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# delete ACLs for a single topic `test`
- name: delete acls
  kafka_acls:
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"
    acls:
    - acl_resource_type: 'topic'
      name: 'test'
      acl_principal: 'User:Bob'
      acl_operation: 'write'
      acl_permission: 'allow'
      state: 'absent'
    - acl_resource_type: 'topic'
      name: 'test'
      acl_principal: 'User:Alice'
      acl_operation: 'write'
      acl_permission: 'allow'
      state: 'absent'

# delete an ACL for a single topic `test`
- name: delete acl
  kafka_acl:
    acl_resource_type: 'topic'
    name: 'test'
    acl_principal: 'User:Bob'
    acl_operation: 'write'
    acl_permission: 'allow'
    state: 'absent'
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# Ensure Quota present for users `test` and `test2`
- name: Ensure quota for user test and test2
  kafka_quotas:
    entries:
    - entity:
        user: test
      quotas:
        producer_byte_rate: 104405
        consumer_byte_rate: 104405
        request_percentage: 55
    - entity:
        user: test2
      quotas:
        producer_byte_rate: 1044050
        consumer_byte_rate: 1044050
        request_percentage: 65
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"


```
### Getting lag statistics
You might want to perform an updates on your Kafka server only if no (or not much) lag is present for a specific consumer group. The following configuration will allow you to do that:
```yaml
# Get kafka consumers LAG statistics
- name: Get kafka consumers LAG stats
  kafka_stat_lag:
    consummer_group: "{{ consummer_group | default('pra-mirror')}}"
    bootstrap_servers: "{{ ansible_ssh_host }}:9094"
    api_version: "{{ kafka_api_version }}"
    sasl_mechanism: "PLAIN"
    security_protocol: "SASL_SSL"
    sasl_plain_username: "admin"
    sasl_plain_password: "{{ kafka_admin_password }}"
    ssl_check_hostname: False
    ssl_cafile: "{{ kafka_cacert | default('/etc/ssl/certs/cacert.crt') }}"
  register: result
  until:  (result.msg | from_json).global_lag_count == 0
  retries: 60
  delay: 2
```
The available statistics are:
```json
{
  // a JSON object for each topic the consumer group subscribed to
  "topic_A": {
    // a JSON object for each partition
    "0": {
      "current_offset": 1234,
      "last_offset": 1235,
      "lag": 1
    },
    // more partitions
  },
  // more topics
  // all topics lag for the consumer group
  "global_lag_count": 1
}
```
### Getting information about topics, brokers, consumer groups
You can use the `kafka_info` lib to retrieve some infornation about topics, brokers, consumer groups and ACLs.

Depending on Ansible version, results will be saved under the `results` or `ansible_module_results` key (named `<results_key>` in below examples).
#### Brokers
Playbook:
```yaml
- name: get brokers information
  kafka_info:
    resource: "broker"
    bootstrap_servers: "{{ ansible_ssh_host }}"
    api_version: "{{ kafka_api_version }}"
  changed_when: false
  register: brokers
```
`brokers` will be:
```json
{
    "<results_key>": {
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
}
```
#### Topics
Playbook:
```yaml
- name: get topics
  kafka_info:
    resource: "topic"
    bootstrap_servers: "{{ ansible_ssh_host }}"
    api_version: "{{ kafka_api_version }}"
  register: topics
```
`topics` will be:
```json
{
    "<results_key>": {
        "test_1600292339": {
            "0": {
                "isr": [
                    1002
                ],
                "leader": 1002,
                "replicas": [
                    1002
                ],
                "earliest_offset": 0,
                "latest_offset": 42,
                "at_min_isr": true,
                "under_replicated": false,
                "under_min_isr": false,
                "unavailable_partition": false
            }
        }
    }
}
```
#### Topics-Configuration
Playbook:
```yaml
- name: get topics configuration
  kafka_info:
    resource: "topic-config"
    bootstrap_servers: "{{ ansible_ssh_host }}"
    api_version: "{{ kafka_api_version }}"
  register: topics
```
`topics` will be:
```json
{
    "<results_key>": {
        "test_1600292339": {
            "retention.ms": "66574936"
        }
    }
}
```
#### Consumer groups
Playbook:
```yaml
- name: get consumer groups
  kafka_info:
    resource: "consumer_group"
    bootstrap_servers: "{{ ansible_ssh_host }}"
    api_version: "{{ kafka_api_version }}"
  register: consumer_groups
```
`consumer_groups` will be:
```json
{
    "<results_key>": {
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
}
```
#### ACLs
Playbook:
```yaml
- name: get ACLs
  kafka_info:
    resource: "acl"
    bootstrap_servers: "{{ ansible_ssh_host }}"
    api_version: "{{ kafka_api_version }}"
  register: acls
```
`acls` will be:
```json
{
  "<results_key>": {
      "topic": {
          "*": [
              {
                  "host": "*",
                  "operation": "write",
                  "pattern_type": "literal",
                  "permission_type": "allow",
                  "principal": "User:Alice",
                  "resource_name": "*",
                  "resource_type": "topic"
              }
          ],
          "toto": [
              {
                  "host": "*",
                  "operation": "write",
                  "pattern_type": "literal",
                  "permission_type": "allow",
                  "principal": "User:Alice",
                  "resource_name": "toto",
                  "resource_type": "topic"
              }
          ]
      }
  },
}
```

## Interact with kafka consumer groups
When a consumer is no longer subscribed to a topic, it remains present in the consumer group and a lag is noted
We can use this feature to delete it effectivelly from the consumer group
```yaml
- name:  Delete offset for consumer group
kafka_consumer_group:
    consumer_group: "{{ consumer_group | default('test_consumer')}}"
    topics:
    - name: test # mandatory
      partitions: [0, 1, 2] # Optional
    action: delete
    bootstrap_servers: "{{ ansible_ssh_host }}:9094"
    api_version: "{{ kafka_api_version }}"
    sasl_mechanism: "PLAIN"
    security_protocol: "SASL_SSL"
    sasl_plain_username: "admin"
    sasl_plain_password: "{{ kafka_admin_password }}"
    ssl_check_hostname: False
    ssl_cafile: "{{ kafka_cacert | default('/etc/ssl/certs/cacert.crt') }}"
```
## Change kafka client configuration
When using bulks modules you can have sometimes timeout.
You may need to tune this values:
```yaml
  request_timeout_ms:
    description:
      - 'timeout for kafka client requests'
    default: 30000
  connections_max_idle_ms:
    description:
      - 'close idle connections after'
    default: 540000
```
## Using SSL
Since SSL is requiring SSLcontext from Python, you need to use **Python 2.7.9 and superior**.

It is possible to connect to Kafka or Zookeeper using a SSL secured connection by:
* precising the path to cacert / server cert / server key on the remote host ;
* directly giving the cacert / server cert / server key (treated as a password) content: a tempfile will be created on the remote host and deleted before ending the module.

Using path to a cacert file:
```yaml
cacert_path: /path/to/my/cacert/file/on/remote/host

# creates a topic for a sasl_ssl configured Kafka and plaintext Zookeeper
- name: create topic
  kafka_topic:
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
    ssl_cafile: '{{ cacert_path }}'
```
Using cacert file content:

```yaml
cacert_content: |
  -----BEGIN CERTIFICATE-----
  CERT_CONTENT
  -----END CERTIFICATE-----

# creates a topic for a sasl_ssl configured Kafka and plaintext Zookeeper
- name: create topic
  kafka_topic:
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
    ssl_cafile: '{{ cacert_content }}'
```
## Python compatibility
This library is tested with the following versions of Python:
* Python 2.7
* Python 3.5
* Python 3.6
* Python 3.7
* Python 3.8
* Python 3.9
* Python 3.10

## Ansible compatibility
To date, this library is compatible with every versions of Ansible 2.X (`ansible`/`ansible-core` 2.X or `ansible` 4.X).

## Tests
This library is tested using [Molecule](https://github.com/ansible/molecule). In order to avoid code duplication, tests are defined in the `default` scenario.

Each test can be run using (see pytest for more options):

```
molecule create
molecule converge
```

## Contributing
You are very welcomed to contribute to this library, do not hesitate to submit issues and pull-requests.

## License
Apache License 2.0, see [LICENSE file](LICENSE).

## Treeware

You're free to use this package, but if it makes it to your production environment and would like to give some support, we would appreciate you buying the world a tree.

It’s now common knowledge that one of the best tools to tackle the climate crisis and keep our temperatures from rising above 1.5C is to <a href="https://www.bbc.co.uk/news/science-environment-48870920">plant trees</a>. If you support this package and contribute to the Treeware forest you’ll be creating employment for local families and restoring wildlife habitats.

You can buy trees here [offset.earth/treeware](https://plant.treeware.earth/StephenSorriaux/ansible-kafka-admin)

Read more about Treeware at [treeware.earth](http://treeware.earth)
