from kafka.protocol.api import Request, Response, RequestHeader
from kafka.protocol.struct import Struct
from kafka.protocol.types import (Boolean,
                                  Int8,
                                  Int16,
                                  Int32, String, Schema, Array, _pack, _unpack)
from kafka.protocol import API_KEYS
import struct
from kafka.protocol.abstract import AbstractType


# Quotas
class Float64(AbstractType):
    _pack = struct.Struct('>d').pack
    _unpack = struct.Struct('>d').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(8))


class DescribeClientQuotasResponse_v0(Response):
    API_KEY = 48
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('error_message', String('utf-8')),
        ('entries', Array(
            ('entity', Array(
                ('entity_type', String('utf-8')),
                ('entity_name', String('utf-8')))),
            ('values', Array(
                ('name', String('utf-8')),
                ('value', Float64))))),
    )


class DescribeClientQuotasRequest_v0(Request):
    API_KEY = 48
    API_VERSION = 0
    RESPONSE_TYPE = DescribeClientQuotasResponse_v0
    SCHEMA = Schema(
        ('components', Array(
            ('entity_type', String('utf-8')),
            ('match_type', Int8),
            ('match', String('utf-8')),
        )),
        ('strict', Boolean)
    )


class AlterClientQuotasResponse_v0(Response):
    API_KEY = 49
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('entries', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('entity', Array(
                ('entity_type', String('utf-8')),
                ('entity_name', String('utf-8'))))))
    )


class AlterClientQuotasRequest_v0(Request):
    API_KEY = 49
    API_VERSION = 0
    RESPONSE_TYPE = AlterClientQuotasResponse_v0
    SCHEMA = Schema(
        ('entries', Array(
            ('entity', Array(
                    ('entity_type', String('utf-8')),
                    ('entity_name', String('utf-8')))),
            ('ops', Array(
                    ('key', String('utf-8')),
                    ('value', Float64),
                    ('remove', Boolean))))),
        ('validate_only', Boolean)
    )


API_KEYS[48] = 'DescribeClientQuotas'
API_KEYS[49] = 'AlterClientQuotas'


# Bug https://github.com/dpkp/kafka-python/pull/2206
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
                ('config_names', String('utf-8')),
                ('config_value', String('utf-8')),
                ('read_only', Boolean),
                ('config_source', Int8),
                ('is_sensitive', Boolean),
                ('config_synonyms', Array(
                    ('config_name', String('utf-8')),
                    ('config_value', String('utf-8')),
                    ('config_source', Int8)))))))
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


try:
    from kafka.protocol.admin import AlterPartitionReassignmentsRequest_v0 \
        as _AlterPartitionReassignmentsRequest_v0
    from kafka.protocol.admin import ListPartitionReassignmentsRequest_v0 \
        as _ListPartitionReassignmentsRequest_v0
    ListPartitionReassignmentsRequest_v0 = \
        _ListPartitionReassignmentsRequest_v0
    AlterPartitionReassignmentsRequest_v0 = \
        _AlterPartitionReassignmentsRequest_v0
except Exception:
    import struct
    from kafka.protocol.abstract import AbstractType

    import kafka.errors as Errors
    from kafka.protocol.commit import GroupCoordinatorResponse
    from kafka.protocol.parser import KafkaProtocol, log
    from kafka.protocol import API_KEYS

    # TODO use AlterPartitionReassignmentsRequest_v0 when
    # https://github.com/dpkp/kafka-python/commit/9feeb79140ed10e3a7f2036491fc07573740c231
    # will be released

    Request.FLEXIBLE_VERSION = False

    def default_build_request_header(self, correlation_id, client_id):
        if self.FLEXIBLE_VERSION:
            return RequestHeaderV2(self,
                                   correlation_id=correlation_id,
                                   client_id=client_id)
        return RequestHeader(self,
                             correlation_id=correlation_id,
                             client_id=client_id)

    def default_parse_response_header(self, read_buffer):
        if self.FLEXIBLE_VERSION:
            return ResponseHeaderV2.decode(read_buffer)
        return ResponseHeader.decode(read_buffer)

    Request.build_request_header = default_build_request_header
    Request.parse_response_header = default_parse_response_header

    def send_request(self, request, correlation_id=None):
        """Encode and queue a kafka api request for sending.
        Arguments:
           request (object): An un-encoded kafka request.
           correlation_id (int, optional): Optionally specify an ID to
               correlate requests with responses. If not provided, an ID will
               be generated automatically.
        Returns:
           correlation_id
        """
        log.debug('Sending request %s', request)
        if correlation_id is None:
            correlation_id = self._next_correlation_id()

        header = request.build_request_header(
            correlation_id=correlation_id, client_id=self._client_id)
        message = b''.join([header.encode(), request.encode()])
        size = Int32.encode(len(message))
        data = size + message
        self.bytes_to_send.append(data)
        if request.expect_response():
            ifr = (correlation_id, request)
            self.in_flight_requests.append(ifr)
        return correlation_id

    def _process_response(self, read_buffer):
        if not self.in_flight_requests:
            raise Errors.CorrelationIdError(
                'No in-flight-request found for server response')
        (correlation_id, request) = self.in_flight_requests.popleft()
        response_header = request.parse_response_header(read_buffer)
        recv_correlation_id = response_header.correlation_id
        log.debug('Received correlation id: %d', recv_correlation_id)
        # 0.8.2 quirk
        if (recv_correlation_id == 0 and
            correlation_id != 0 and
            request.RESPONSE_TYPE is GroupCoordinatorResponse[0] and
                (self._api_version == (0, 8, 2) or self._api_version is None)):
            log.warning('Kafka 0.8.2 quirk -- GroupCoordinatorResponse'
                        ' Correlation ID does not match request. This'
                        ' should go away once at least one topic has been'
                        ' initialized on the broker.')

        elif correlation_id != recv_correlation_id:
            # return or raise?
            raise Errors.CorrelationIdError(
                'Correlation IDs do not match: sent %d, recv %d'
                % (correlation_id, recv_correlation_id))

        # decode response
        log.debug('Processing response %s', request.RESPONSE_TYPE.__name__)
        try:
            response = request.RESPONSE_TYPE.decode(read_buffer)
        except ValueError:
            read_buffer.seek(0)
            buf = read_buffer.read()
            log.error('Response %d [ResponseType: %s Request: %s]:'
                      ' Unable to decode %d-byte buffer: %r',
                      correlation_id, request.RESPONSE_TYPE,
                      request, len(buf), buf)
            raise Errors.KafkaProtocolError('Unable to decode response')

        return (correlation_id, response)

    KafkaProtocol.send_request = send_request
    KafkaProtocol._process_response = _process_response

    class UnsignedVarInt32(AbstractType):
        @classmethod
        def decode(cls, data):
            value, i = 0, 0
            while True:
                b, = struct.unpack('B', data.read(1))
                if not (b & 0x80):
                    break
                value |= (b & 0x7f) << i
                i += 7
                if i > 28:
                    raise ValueError('Invalid value {}'.format(value))
            value |= b << i
            return value

        @classmethod
        def encode(cls, value):
            value &= 0xffffffff
            ret = b''
            while (value & 0xffffff80) != 0:
                b = (value & 0x7f) | 0x80
                ret += struct.pack('B', b)
                value >>= 7
            ret += struct.pack('B', value)
            return ret

    class VarInt32(AbstractType):
        @classmethod
        def decode(cls, data):
            value = UnsignedVarInt32.decode(data)
            return (value >> 1) ^ -(value & 1)

        @classmethod
        def encode(cls, value):
            # bring it in line with the java binary repr
            value &= 0xffffffff
            return UnsignedVarInt32.encode((value << 1) ^ (value >> 31))

    class VarInt64(AbstractType):
        @classmethod
        def decode(cls, data):
            value, i = 0, 0
            while True:
                b = data.read(1)
                if not (b & 0x80):
                    break
                value |= (b & 0x7f) << i
                i += 7
                if i > 63:
                    raise ValueError('Invalid value {}'.format(value))
            value |= b << i
            return (value >> 1) ^ -(value & 1)

        @classmethod
        def encode(cls, value):
            # bring it in line with the java binary repr
            value &= 0xffffffffffffffff
            v = (value << 1) ^ (value >> 63)
            ret = b''
            while (v & 0xffffffffffffff80) != 0:
                b = (value & 0x7f) | 0x80
                ret += struct.pack('B', b)
                v >>= 7
            ret += struct.pack('B', v)
            return ret

    class CompactString(String):
        def decode(self, data):
            length = UnsignedVarInt32.decode(data) - 1
            if length < 0:
                return None
            value = data.read(length)
            if len(value) != length:
                raise ValueError('Buffer underrun decoding string')
            return value.decode(self.encoding)

        def encode(self, value):
            if value is None:
                return UnsignedVarInt32.encode(0)
            value = str(value).encode(self.encoding)
            return UnsignedVarInt32.encode(len(value) + 1) + value

    class TaggedFields(AbstractType):
        @classmethod
        def decode(cls, data):
            num_fields = UnsignedVarInt32.decode(data)
            ret = {}
            if not num_fields:
                return ret
            prev_tag = -1
            for i in range(num_fields):
                tag = UnsignedVarInt32.decode(data)
                if tag <= prev_tag:
                    raise ValueError('Invalid or out-of-order tag {}'
                                     .format(tag))
                prev_tag = tag
                size = UnsignedVarInt32.decode(data)
                val = data.read(size)
                ret[tag] = val
            return ret

        @classmethod
        def encode(cls, value):
            ret = UnsignedVarInt32.encode(len(value))
            for k, v in value.items():
                # do we allow for other data types ??
                # It could get complicated really fast
                assert isinstance(v, bytes), \
                    'Value {} is not a byte array'.format(v)
                assert isinstance(k, int) and k > 0, \
                    'Key {} is not a positive integer'.format(k)
                ret += UnsignedVarInt32.encode(k)
                ret += v
            return ret

    class CompactBytes(AbstractType):
        @classmethod
        def decode(cls, data):
            length = UnsignedVarInt32.decode(data) - 1
            if length < 0:
                return None
            value = data.read(length)
            if len(value) != length:
                raise ValueError('Buffer underrun decoding Bytes')
            return value

        @classmethod
        def encode(cls, value):
            if value is None:
                return UnsignedVarInt32.encode(0)
            else:
                return UnsignedVarInt32.encode(len(value) + 1) + value

    class CompactArray(Array):

        def encode(self, items):
            if items is None:
                return UnsignedVarInt32.encode(0)
            return b''.join(
                [UnsignedVarInt32.encode(len(items) + 1)] +
                [self.array_of.encode(item) for item in items]
            )

        def decode(self, data):
            length = UnsignedVarInt32.decode(data) - 1
            if length == -1:
                return None
            return [self.array_of.decode(data) for _ in range(length)]

    class RequestHeaderV2(Struct):
        # Flexible response / request headers end in field buffer
        SCHEMA = Schema(
            ('api_key', Int16),
            ('api_version', Int16),
            ('correlation_id', Int32),
            ('client_id', String('utf-8')),
            ('tags', TaggedFields),
        )

        def __init__(self, request, correlation_id=0,
                     client_id='kafka-python', tags=None):
            super(RequestHeaderV2, self).__init__(
                request.API_KEY,
                request.API_VERSION,
                correlation_id,
                client_id,
                tags or {}
            )

    class ResponseHeader(Struct):
        SCHEMA = Schema(
            ('correlation_id', Int32),
        )

    class ResponseHeaderV2(Struct):
        SCHEMA = Schema(
            ('correlation_id', Int32),
            ('tags', TaggedFields),
        )

    class AlterPartitionReassignmentsResponse_v0(Response):
        API_KEY = 45
        API_VERSION = 0
        SCHEMA = Schema(
            ("throttle_time_ms", Int32),
            ("error_code", Int16),
            ("error_message", CompactString("utf-8")),
            ("responses", CompactArray(
                ("name", CompactString("utf-8")),
                ("partitions", CompactArray(
                    ("partition_index", Int32),
                    ("error_code", Int16),
                    ("error_message", CompactString("utf-8")),
                    ("tags", TaggedFields)
                )),
                ("tags", TaggedFields)
            )),
            ("tags", TaggedFields)
        )

    class AlterPartitionReassignmentsRequest_v0(Request):
        FLEXIBLE_VERSION = True
        API_KEY = 45
        API_VERSION = 0
        RESPONSE_TYPE = AlterPartitionReassignmentsResponse_v0
        SCHEMA = Schema(
            ("timeout_ms", Int32),
            ("topics", CompactArray(
                ("name", CompactString("utf-8")),
                ("partitions", CompactArray(
                    ("partition_index", Int32),
                    ("replicas", CompactArray(Int32)),
                    ("tags", TaggedFields)
                )),
                ("tags", TaggedFields)
            )),
            ("tags", TaggedFields)
        )

    class ListPartitionReassignmentsResponse_v0(Response):
        API_KEY = 46
        API_VERSION = 0
        SCHEMA = Schema(
            ("throttle_time_ms", Int32),
            ("error_code", Int16),
            ("error_message", CompactString("utf-8")),
            ("topics", CompactArray(
                ("name", CompactString("utf-8")),
                ("partitions", CompactArray(
                    ("partition_index", Int32),
                    ("replicas", CompactArray(Int32)),
                    ("adding_replicas", CompactArray(Int32)),
                    ("removing_replicas", CompactArray(Int32)),
                    ("tags", TaggedFields)
                )),
                ("tags", TaggedFields)
            )),
            ("tags", TaggedFields)
        )

    class ListPartitionReassignmentsRequest_v0(Request):
        FLEXIBLE_VERSION = True
        API_KEY = 46
        API_VERSION = 0
        RESPONSE_TYPE = ListPartitionReassignmentsResponse_v0
        SCHEMA = Schema(
            ("timeout_ms", Int32),
            ("topics", CompactArray(
                ("name", CompactString("utf-8")),
                ("partition_index", CompactArray(Int32)),
                ("tags", TaggedFields)
            )),
            ("tags", TaggedFields)
        )

    API_KEYS[45] = 'AlterPartitionReassignments'
    API_KEYS[46] = 'ListPartitionReassignments'
