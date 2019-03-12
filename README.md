# ansible-kafka-admin [![Build Status](https://travis-ci.org/StephenSorriaux/ansible-kafka-admin.svg?branch=master)](https://travis-ci.org/StephenSorriaux/ansible-kafka-admin)
A low level ansible library to manage Kafka configuration. It does not use the Kafka scripts and directly connect to Kafka and Zookeeper (if needed) to ensure resource creation. No ssh connection is needed to the remote host.

If you want to increase partitions, replication factor, change your topic's parameters or manage your ACLs without any effort, this library would be perfect for you.
## Requirements
This library uses [kafka-python](https://github.com/dpkp/kafka-python), [kazoo](https://github.com/python-zk/kazoo) and [pure-sasl](https://github.com/thobbs/pure-sasl) libraries. Install them using pip:
```bash
pip install -r requirements.txt
```
For now, this library is compatible with **Kafka in version 0.11.0 and more**.

It can be used with Kafka configured in PLAINTEXT, SASL_PLAINTEXT, SSL and SASL_SSL.

**Concerning Zookeeper**, it is not compatible with Kerberos authentication yet, only with SSL, SASL and DIGEST authentication.
## Usage
Add the following requirement in your playbook's **requirements.yml**:
```yaml
---
# from GitHub, overriding the name and specifying a specific tag
- src: https://github.com/StephenSorriaux/ansible-kafka-admin
  name: kafka_lib
```
Here some examples on how to use this library:
```yaml
# creates a topic 'test' with provided configuation for plaintext configured Kafka and Zookeeper
- name: create topic
  kafka_lib:
    resource: "topic"
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
  kafka_lib:
    resource: "topic"
    api_version: "1.0.1"
    name: "test"
    partitions: 2
    replica_factor: 1
    options:
      retention.ms: 574930
    state: "present"
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

# create an ACL for all topics
- name: create acl
  kafka_lib:
    resource: 'acl'
    acl_resource_type: 'topic'
    name: '*'
    acl_principal: 'User:Alice'
    acl_operation: 'write'
    acl_permission: 'allow'
    state: 'present'
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

# delete an ACL for a single topic `test`
- name: delete acl
  kafka_lib:
    resource: 'acl'
    acl_resource_type: 'topic'
    name: 'test'
    acl_principal: 'User:Bob'
    acl_operation: 'write'
    acl_permission: 'allow'
    state: 'absent'
    bootstrap_servers: "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,{{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"


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
    ssl_cafile: '{{ cacert_content }}'
```

## Python compatibility
This library is tested with the following versions of Python:
* Python 2.7
* Python 3.6

It should be fine for Python 3.5 too (but not tested using CI)

## Tests
This library is tested using [Molecule](https://github.com/ansible/molecule). In order to avoid code duplication, tests are defined using ansible tags in the `default` scenario.

Tags currently available are:
* test_replica_factor
* test_partitions
* test_options
* test_delete
* test_acl_create
* test_acl_delete

Each test can be run using:

```
molecule create
molecule prepare --force
molecule converge -- --tags {TAG}
```
## Contributing
You are very welcomed to contribute to this library, do not hesitate to submit issues and pull-requests.
