# -*- coding: utf-8 -*-
"""
Kafka JSON Assignment Library
Contains all logic for JSON-based partition reassignment
"""
import json
from ansible.module_utils.kafka_lib_errors import KafkaManagerError


class JsonAssignmentValidator:
    """Validates JSON assignment parameters and format"""
    
    @staticmethod
    def validate_assignment_format(json_assignment, module=None):
        """
        Validate JSON assignment format and structure
        
        Args:
            json_assignment: JSON assignment data (dict or string)
            module: Ansible module for error reporting (optional)
            
        Returns:
            dict: Parsed assignment data
            
        Raises:
            KafkaManagerError: If validation fails
        """
        try:
            if isinstance(json_assignment, str):
                assignment_data = json.loads(json_assignment)
            else:
                assignment_data = json_assignment
        except (json.JSONDecodeError, ValueError) as e:
            error_msg = 'Invalid JSON assignment format: %s' % str(e)
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        if not isinstance(assignment_data, dict):
            error_msg = 'json_assignment must be a JSON object'
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        if 'partitions' not in assignment_data:
            error_msg = 'json_assignment must contain "partitions" array'
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        partitions = assignment_data['partitions']
        if not isinstance(partitions, list):
            error_msg = 'json_assignment "partitions" must be an array'
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        # Allow empty partitions array for status check
        # The actual status check logic is in the main function
        
        for i, partition in enumerate(partitions):
            if not isinstance(partition, dict):
                error_msg = 'Partition %d in json_assignment must be an object' % i
                if module:
                    module.fail_json(msg=error_msg)
                raise KafkaManagerError(error_msg)
            
            required_fields = ['topic', 'partition', 'replicas']
            for field in required_fields:
                if field not in partition:
                    error_msg = 'Partition %d must have "%s" field' % (i, field)
                    if module:
                        module.fail_json(msg=error_msg)
                    raise KafkaManagerError(error_msg)
            
            # Allow None for replicas when cancelling reassignments
            if partition['replicas'] is not None:
                if not isinstance(partition['replicas'], list):
                    error_msg = 'Partition %d replicas must be an array or null' % i
                    if module:
                        module.fail_json(msg=error_msg)
                    raise KafkaManagerError(error_msg)
                
                if len(partition['replicas']) == 0:
                    error_msg = 'Partition %d replicas array cannot be empty' % i
                    if module:
                        module.fail_json(msg=error_msg)
                    raise KafkaManagerError(error_msg)
        
        return assignment_data
    
    @staticmethod
    def validate_partition_existence(partitions, manager):
        """
        Validate that all specified partitions exist in their respective topics
        
        Args:
            partitions: List of partition assignments
            manager: KafkaManager instance
            
        Raises:
            KafkaManagerError: If any partition doesn't exist
        """
        # Get all topics from the cluster
        all_topics = manager.get_topics(include_internal=False)
        
        # Group partitions by topic for efficient validation
        topics_to_partitions = {}
        for partition in partitions:
            topic_name = partition['topic']
            if topic_name not in topics_to_partitions:
                topics_to_partitions[topic_name] = []
            topics_to_partitions[topic_name].append(partition)
        
        # Validate each topic and its partitions
        for topic_name, topic_partitions in topics_to_partitions.items():
            # Check if topic exists
            if topic_name not in all_topics:
                raise KafkaManagerError(
                    'Unable to proceed with partition reassignment: '
                    'topic "%s" does not exist. '
                    'Available topics: %s' % (
                        topic_name,
                        sorted(all_topics)
                    )
                )
            
            # Get total number of partitions for this topic
            total_partitions = manager.get_total_partitions_for_topic(topic_name)
            
            # Check if specified partitions exist
            for partition in topic_partitions:
                partition_id = partition['partition']
                if partition_id < 0 or partition_id >= total_partitions:
                    raise KafkaManagerError(
                        'Unable to proceed with partition reassignment: '
                        'partition %d does not exist in topic "%s". '
                        'Topic has %d partitions (valid range: 0-%d).' % (
                            partition_id,
                            topic_name,
                            total_partitions,
                            total_partitions - 1
                        )
                    )
    
    @staticmethod
    def validate_broker_availability(partitions, manager, cancel=False):
        """
        Validate that all brokers in assignments are available
        
        Args:
            partitions: List of partition assignments
            manager: KafkaManager instance
            cancel: Whether this is a cancellation operation
            
        Raises:
            KafkaManagerError: If any broker is unavailable
        """
        # Skip broker validation for cancellations
        if cancel:
            return
        
        available_brokers = set()
        for broker in manager.get_brokers():
            available_brokers.add(broker.nodeId)
        
        for partition in partitions:
            replicas = partition['replicas']
            if replicas is None:
                continue
            
            unavailable_brokers = []
            
            for broker_id in replicas:
                if broker_id not in available_brokers:
                    unavailable_brokers.append(broker_id)
            
            if unavailable_brokers:
                raise KafkaManagerError(
                    'Unable to proceed with partition reassignment: '
                    'broker(s) %s in partition %d assignment for topic "%s" are not available. '
                    'Available brokers: %s' % (
                        unavailable_brokers,
                        partition['partition'],
                        partition['topic'],
                        sorted(available_brokers)
                    )
                )
    
    @staticmethod
    def validate_unique_broker_ids(partitions, module=None, cancel=False):
        """
        Validate that all broker IDs in replica lists are unique within each partition
        
        Args:
            partitions: List of partition assignments
            module: Ansible module for error reporting (optional)
            cancel: Whether this is a cancellation operation
            
        Raises:
            KafkaManagerError: If any partition has duplicate broker IDs
        """
        for partition in partitions:
            replicas = partition['replicas']
            if replicas is None:
                continue
            
            # Check for duplicates by comparing list length with set length
            if len(replicas) != len(set(replicas)):
                # Find duplicate broker IDs
                seen = set()
                duplicates = []
                for broker_id in replicas:
                    if broker_id in seen:
                        duplicates.append(broker_id)
                    seen.add(broker_id)
                
                error_msg = (
                    'Partition %d assignment for topic "%s" contains duplicate broker ID(s): %s. '
                    'Each broker ID must appear only once in the replicas list.' % (
                        partition['partition'],
                        partition['topic'],
                        sorted(set(duplicates))
                    )
                )
                if module:
                    module.fail_json(msg=error_msg)
                raise KafkaManagerError(error_msg)


class JsonAssignmentProcessor:
    """Processes and applies JSON assignments"""
    
    @staticmethod
    def parse_assignment(json_assignment):
        """
        Parse string/dict to consistent format
        
        Args:
            json_assignment: JSON assignment (string or dict)
            
        Returns:
            dict: Parsed assignment data
        """
        if isinstance(json_assignment, str):
            return json.loads(json_assignment)
        else:
            return json_assignment
    
    @staticmethod
    def apply_assignment(manager, json_assignment, wait_for_completion=True, cancel=False):
        """
        Apply JSON assignment to Kafka cluster
        
        Args:
            manager: KafkaManager instance
            json_assignment: JSON assignment data
            wait_for_completion: Whether to wait for reassignment completion
            cancel: Whether this is a cancellation operation
            
        Raises:
            KafkaManagerError: If application fails
        """
        from pkg_resources import parse_version
        from ansible.module_utils.kafka_protocol import AlterPartitionReassignmentsRequest_v0
        
        assignment_data = JsonAssignmentProcessor.parse_assignment(json_assignment)
        partitions = assignment_data['partitions']
        
        # Validate partition existence
        JsonAssignmentValidator.validate_partition_existence(partitions, manager)
        
        # Validate broker availability (skip for cancellations)
        JsonAssignmentValidator.validate_broker_availability(partitions, manager, cancel)
        
        # Validate unique broker IDs in replica lists (skip for cancellations)
        JsonAssignmentValidator.validate_unique_broker_ids(partitions, cancel=cancel)
        
        # Group partitions by topic for efficient processing
        topics_to_partitions = {}
        for partition in partitions:
            topic_name = partition['topic']
            if topic_name not in topics_to_partitions:
                topics_to_partitions[topic_name] = []
            topics_to_partitions[topic_name].append(partition)
        
        # Apply assignment based on Kafka version
        if parse_version(manager.get_api_version()) >= parse_version('2.4.0'):
            JsonAssignmentProcessor._apply_assignment_new_api(manager, topics_to_partitions, wait_for_completion, cancel)
        elif manager.zk_configuration is not None:
            if cancel:
                raise KafkaManagerError('Cancelling reassignments requires Kafka >= 2.4.0. ZooKeeper-based cancellation is not supported.')
            JsonAssignmentProcessor._apply_assignment_zookeeper(manager, assignment_data, wait_for_completion)
        else:
            raise KafkaManagerError('Zookeeper is mandatory for partition assignment when using Kafka <= 2.4.0.')
        
        manager.refresh()
    
    @staticmethod
    def _apply_assignment_new_api(manager, topics_to_partitions, wait_for_completion, cancel=False):
        """Apply assignment using Kafka >= 2.4.0 API"""
        from pkg_resources import parse_version
        from ansible.module_utils.kafka_protocol import AlterPartitionReassignmentsRequest_v0
        
        # Build assignment request
        assign = []
        for topic_name, partitions in topics_to_partitions.items():
            partition_assignments = []
            for partition in partitions:
                # For cancellation, replicas should be None
                replicas = None if cancel else partition['replicas']
                partition_assignments.append((
                    partition['partition'],
                    replicas,
                    {}
                ))
            assign.append((topic_name, partition_assignments, {}))
        
        if assign:
            request = AlterPartitionReassignmentsRequest_v0(
                timeout_ms=manager.request_timeout_ms,
                topics=assign,
                tags={}
            )
            
            # For cancellation, don't wait for reassignment to complete
            # The cancellation happens immediately
            if wait_for_completion and not cancel:
                manager.wait_for_partition_assignement()
            
            manager.send_request_and_get_response(request)
            
            # For cancellation, don't wait for reassignment to complete
            # The cancellation happens immediately
            if wait_for_completion and not cancel:
                manager.wait_for_partition_assignement()
    
    @staticmethod
    def _apply_assignment_zookeeper(manager, assignment_data, wait_for_completion):
        """Apply assignment using ZooKeeper (older Kafka versions)"""
        try:
            manager.init_zk_client()
            
            if wait_for_completion:
                manager.wait_for_znode_assignment()
            
            # Create ZooKeeper format assignment
            zk_assignment = {
                'version': 1,
                'partitions': assignment_data['partitions']
            }
            
            manager.zk_client.create(
                manager.ZK_REASSIGN_NODE,
                json.dumps(zk_assignment, ensure_ascii=False).encode('utf-8')
            )
            
            if wait_for_completion:
                manager.wait_for_znode_assignment()
        finally:
            manager.close_zk_client()


class ReassignmentManager:
    """High-level interface for partition reassignment operations"""
    
    def __init__(self, manager):
        self.manager = manager
        self.validator = JsonAssignmentValidator()
        self.processor = JsonAssignmentProcessor()
    
    def validate_assignment(self, json_assignment, module=None, cancel=False):
        """Validate a JSON assignment"""
        validated_assignment = self.validator.validate_assignment_format(json_assignment, module)
        
        # Validate partition existence if there are partitions to validate
        if len(validated_assignment['partitions']) > 0:
            self.validator.validate_partition_existence(validated_assignment['partitions'], self.manager)
            self.validator.validate_unique_broker_ids(validated_assignment['partitions'], module, cancel)
            self.validator.validate_broker_availability(validated_assignment['partitions'], self.manager, cancel)
        
        return validated_assignment
    
    def apply_assignment(self, json_assignment, wait_for_completion=True):
        """Apply a JSON assignment to the cluster"""
        from pkg_resources import parse_version
        
        # Check if there's already an active reassignment
        status = self.get_assignment_status()
        
        # Determine if reassignment is in progress based on the response format
        reassignment_in_progress = False
        if parse_version(self.manager.get_api_version()) >= parse_version('2.4.0'):
            # For Kafka >= 2.4.0, check if there are any ongoing reassignments
            if status.get('topics') and len(status['topics']) > 0:
                reassignment_in_progress = True
        else:
            # For older versions, check the boolean flag
            reassignment_in_progress = status.get('reassignment_in_progress', False)
        
        if reassignment_in_progress:
            raise KafkaManagerError(
                'Unable to proceed with partition reassignment: '
                'a reassignment is already in progress. '
                'Please wait for the current reassignment to complete before starting a new one.'
            )
        
        self.processor.apply_assignment(self.manager, json_assignment, wait_for_completion, cancel=False)
    
    def cancel_assignment(self, json_assignment, wait_for_completion=True):
        """Cancel ongoing partition reassignments"""
        from pkg_resources import parse_version
        
        # Check Kafka version
        if parse_version(self.manager.get_api_version()) < parse_version('2.4.0'):
            raise KafkaManagerError(
                'Cancelling reassignments requires Kafka >= 2.4.0. '
                'Current version: %s' % self.manager.get_api_version()
            )
        
        # Get current reassignment status
        status = self.get_assignment_status()
        
        # Parse the assignment to get the list of partitions to cancel
        assignment_data = JsonAssignmentProcessor.parse_assignment(json_assignment)
        partitions_to_cancel = assignment_data['partitions']
        
        # Build a map of active reassignments
        active_reassignments = {}
        if status.get('topics'):
            for topic_data in status['topics']:
                topic_name = topic_data.get('name')
                if topic_name and topic_data.get('partitions'):
                    for partition_data in topic_data['partitions']:
                        partition_id = partition_data.get('partition_index')
                        if partition_id is not None:
                            key = (topic_name, partition_id)
                            active_reassignments[key] = True
        
        # Filter out partitions that don't have active reassignments
        partitions_with_active_reassignment = []
        for partition in partitions_to_cancel:
            key = (partition['topic'], partition['partition'])
            if key in active_reassignments:
                partitions_with_active_reassignment.append(partition)
        
        if not partitions_with_active_reassignment:
            # No active reassignments to cancel
            return
        
        # Create filtered assignment with only active reassignments
        filtered_assignment = {
            'partitions': partitions_with_active_reassignment
        }
        
        # Apply cancellation
        self.processor.apply_assignment(self.manager, filtered_assignment, wait_for_completion, cancel=True)
    
    def get_assignment_status(self):
        """Get current reassignment status"""
        from pkg_resources import parse_version
        from ansible.module_utils.kafka_protocol import ListPartitionReassignmentsRequest_v0
        
        if parse_version(self.manager.get_api_version()) >= parse_version('2.4.0'):
            request = ListPartitionReassignmentsRequest_v0(
                timeout_ms=self.manager.request_timeout_ms,
                topics=None,
                tags={}
            )
            response = self.manager.send_request_and_get_response(request)
            return response.to_object()
        else:
            # For older versions, check if znode exists
            if self.manager.zk_configuration is not None:
                try:
                    self.manager.init_zk_client()
                    exists = self.manager.zk_client.exists(self.manager.ZK_REASSIGN_NODE)
                    return {'reassignment_in_progress': exists}
                finally:
                    self.manager.close_zk_client()
            else:
                return {'reassignment_in_progress': False}