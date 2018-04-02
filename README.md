# ansible-kafka-admin [![Build Status](https://travis-ci.org/StephenSorriaux/ansible-kafka-admin.svg?branch=master)](https://travis-ci.org/StephenSorriaux/ansible-kafka-admin)
A low level ansible library to manage Kafka configuration. It does not use the Kafka scripts and directly connect to Kafka and Zookeeper to ensure resource creation.
No ssh connection is needed.
## Requirements
This library uses kafka-python and kazoo libraries. Install them using pip:
```bash
pip install -r requirements.txt
```
This library is compatible with **Kafka in version 0.11.0 and more**.
It can be used with Kafka configured in PLAINTEXT, SASL_PLAINTEXT, SSL and SASL_SSL.
Concerning Zookeeper, it is not compatible with SSL secured connection.
## Usage
Here some examples on how to use this library:
```yaml
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

```
## Tests
This library is tested using [Molecule](https://github.com/metacloud/molecule). Tests can be run using:
```
molecule test --all
```
## Contributing
You are very welcomed to contribute to this library, do not hesitate to submit issues and pull-requests.
