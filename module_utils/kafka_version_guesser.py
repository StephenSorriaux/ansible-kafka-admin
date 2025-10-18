"""
Kafka version management for request API versions.

Adapted from https://raw.githubusercontent.com/twmb/franz-go/refs/heads/master/pkg/kversion/requests.go  # noqa
"""
import copy


MAJ_0_8 = "0.8"
MAJ_0_9 = "0.9"
MAJ_0_10 = "0.10"
MAJ_0_11 = "0.11"

# Most of these keys are broker-to-broker only requests, and
# most non-Kafka implementations do not implement them.
#
# We skip 71 and 72 because telemetry requests are only
# advertised if the broker is configured to support it.
SKIP_KEYS = [4, 5, 6, 7, 27, 56, 57, 58, 67, 71, 72]


class Req:
    """Request version information."""
    key = -1
    v_min = 0
    v_max = 0

    def __init__(self, key, v_min=0, v_max=0):
        self.key = key
        self.v_min = v_min
        self.v_max = v_max


class Release:
    """Kafka version release information."""
    prior = None

    def __init__(self, major, minor, reqs=None, prior=None):
        self.major = major
        self.minor = minor
        if reqs:
            self.reqs = reqs
        else:
            self.reqs = {}
        if prior:
            self.prior = prior

    def clone(self, next_major, next_minor):
        """Clone release with new version numbers."""
        return Release(
            major=next_major,
            minor=next_minor,
            reqs=copy.deepcopy(self.reqs),
            prior=self
        )

    def name(self):
        """Get version name string."""
        if self.major in (MAJ_0_8, MAJ_0_9, MAJ_0_10, MAJ_0_11):
            return "%s.%s" % (self.major, self.minor)
        return "%s.%s.0" % (self.major, self.minor)

    def version_tuple(self):
        if self.major in (MAJ_0_8, MAJ_0_9, MAJ_0_10, MAJ_0_11):
            major, minor = self.major.split('.')
            return (int(major), int(minor), int(self.minor))
        return (int(self.major), int(self.minor), 0)

    def inc_max(self, key, v_max):
        """Increment max version for a request key."""
        if key not in self.reqs:
            raise ValueError("key %s does not yet exist to inc_max" % key)
        req = self.reqs[key]
        if req.v_max + 1 != v_max:
            raise ValueError("key %s next max %s} != exp %s" % (
                key, req.v_max + 1, v_max))
        req.v_max += 1
        self.reqs[key] = req

    # Set minimum versions (KIP-896)
    def set_min(self, key, v_min):
        if key not in self.reqs:
            raise ValueError("setmin on non-existent key %s" % key)
        req = self.reqs[key]
        req.v_min = v_min
        self.reqs[key] = req

    def add_key(self, key):
        """Add new request key with version 0."""
        self.add_key_and_v_max(key, 0)

    def add_key_and_v_max(self, key, v_max):
        """Add new request key with specific max version."""
        if key in self.reqs:
            raise ValueError("key %s already exists" % key)
        self.reqs[key] = Req(key=key, v_max=v_max)

    def __eq__(self, other):
        """Checks equality based on API versions"""
        # 1st check mine against theirs
        for k, mine in self.reqs.items():
            if k in SKIP_KEYS:
                continue
            theirs = other.reqs.get(k, None)
            if theirs is None:
                return False
            if theirs.v_min != mine.v_min or theirs.v_max != mine.v_max:
                return False
        # then check theirs against mine
        for k, theirs in other.reqs.items():
            if k in SKIP_KEYS:
                continue
            mine = self.reqs.get(k, None)
            if mine is None:
                return False
            if theirs.v_min != mine.v_min or theirs.v_max != mine.v_max:
                return False
        return True

    def matches(self, other):
        if self == other:
            return self
        if self.prior:
            return self.prior.matches(other)
        return None


def from_api_versions(api_versions):
    reqs = {}
    for k, v in api_versions.items():
        min_version, max_version = v
        reqs[k] = Req(
            key=k, v_min=min_version, v_max=max_version
        )
    return Release(0, 0, reqs=reqs)


# Zookeeper versions
def z080():
    return Release(
        major=MAJ_0_8,
        minor=0,
        reqs={
            0: Req(key=0),  # produce
            1: Req(key=1),  # fetch
            2: Req(key=2),  # list offset
            3: Req(key=3),  # metadata
            4: Req(key=4),  # leader and isr
            5: Req(key=5),  # stop replica
            6: Req(key=6),  # update metadata
            7: Req(key=7),  # controlled shutdown
        }
    )


def z081():
    prior = z080()
    now = prior.clone(MAJ_0_8, 1)
    now.add_key(8)  # offset commit
    now.add_key(9)  # offset fetch
    return now


def z082():
    now = z081().clone(MAJ_0_8, 2)
    now.inc_max(8, 1)  # offset commit
    now.inc_max(9, 1)  # offset fetch
    now.add_key(10)  # find coordinator
    now.add_key(11)  # join group
    now.add_key(12)  # heartbeat
    return now


def z090():
    now = z082().clone(MAJ_0_9, 0)
    now.inc_max(0, 1)  # produce
    now.inc_max(1, 1)  # fetch
    now.inc_max(6, 1)  # update metadata
    now.inc_max(7, 1)  # controlled shutdown
    now.inc_max(8, 2)  # offset commit
    now.add_key(13)  # leave group
    now.add_key(14)  # sync group
    now.add_key(15)  # describe groups
    now.add_key(16)  # list groups
    return now


def z0100():
    now = z090().clone(MAJ_0_10, 0)
    now.inc_max(0, 2)  # produce
    now.inc_max(1, 2)  # fetch
    now.inc_max(3, 1)  # metadata
    now.inc_max(6, 2)  # update metadata
    now.add_key(17)  # sasl handshake
    now.add_key(18)  # api versions
    return now


def z0101():
    now = z0100().clone(MAJ_0_10, 1)
    now.inc_max(1, 3)  # fetch
    now.inc_max(2, 1)  # list offset
    now.inc_max(3, 2)  # metadata
    now.inc_max(11, 1)  # join group
    now.add_key(19)  # create topics
    now.add_key(20)  # delete topics
    return now


def z0102():
    now = z0101().clone(MAJ_0_10, 2)
    now.inc_max(6, 3)  # update metadata
    now.inc_max(19, 1)  # create topics
    return now


def z0110():
    now = z0102().clone(MAJ_0_11, 0)
    now.inc_max(0, 3)  # produce
    now.inc_max(1, 4)  # fetch
    now.inc_max(1, 5)  # fetch
    now.inc_max(9, 2)  # offset fetch
    now.inc_max(10, 1)  # find coordinator
    now.add_key(21)  # delete records
    now.add_key(22)  # init producer id
    now.add_key(23)  # offset for leader epoch
    now.add_key(24)  # add partitions to txn
    now.add_key(25)  # add offsets to txn
    now.add_key(26)  # end txn
    now.add_key(27)  # write txn markers
    now.add_key(28)  # txn offset commit
    now.add_key(29)  # describe acls
    now.add_key(30)  # create acls
    now.add_key(31)  # delete acls
    now.add_key(32)  # describe configs
    now.add_key(33)  # alter configs
    # Flexible version bumps
    now.inc_max(2, 2)  # list offset
    now.inc_max(3, 3)  # metadata
    now.inc_max(8, 3)  # offset commit
    now.inc_max(9, 3)  # offset fetch
    now.inc_max(11, 2)  # join group
    now.inc_max(12, 1)  # heartbeat
    now.inc_max(13, 1)  # leave group
    now.inc_max(14, 1)  # sync group
    now.inc_max(15, 1)  # describe groups
    now.inc_max(16, 1)  # list group
    now.inc_max(18, 1)  # api versions
    now.inc_max(19, 2)  # create topics
    now.inc_max(20, 1)  # delete topics
    now.inc_max(3, 4)  # metadata
    return now


def z10():
    now = z0110().clone(1, 0)
    now.inc_max(0, 4)  # produce
    now.inc_max(1, 6)  # fetch
    now.inc_max(3, 5)  # metadata
    now.inc_max(4, 1)  # leader and isr
    now.inc_max(6, 4)  # update metadata
    now.inc_max(0, 5)  # produce
    now.inc_max(17, 1)  # sasl handshake
    now.add_key(34)  # alter replica log dirs
    now.add_key(35)  # describe log dirs
    now.add_key(36)  # sasl authenticate
    now.add_key(37)  # create partitions
    return now


def z11():
    now = z10().clone(1, 1)
    now.add_key(38)  # create delegation token
    now.add_key(39)  # renew delegation token
    now.add_key(40)  # expire delegation token
    now.add_key(41)  # describe delegation token
    now.add_key(42)  # delete groups
    now.inc_max(1, 7)  # fetch
    now.inc_max(32, 1)  # describe configs
    return now


def z20():
    now = z11().clone(2, 0)
    # Large version bump for flexible versions
    now.inc_max(0, 6)  # produce
    now.inc_max(1, 8)  # fetch
    now.inc_max(2, 3)  # list offset
    now.inc_max(3, 6)  # metadata
    now.inc_max(8, 4)  # offset commit
    now.inc_max(9, 4)  # offset fetch
    now.inc_max(10, 2)  # find coordinator
    now.inc_max(11, 3)  # join group
    now.inc_max(12, 2)  # heartbeat
    now.inc_max(13, 2)  # leave group
    now.inc_max(14, 2)  # sync group
    now.inc_max(15, 2)  # describe groups
    now.inc_max(16, 2)  # list group
    now.inc_max(18, 2)  # api versions
    now.inc_max(19, 3)  # create topics
    now.inc_max(20, 2)  # delete topics
    now.inc_max(21, 1)  # delete records
    now.inc_max(22, 1)  # init producer id
    now.inc_max(24, 1)  # add partitions to txn
    now.inc_max(25, 1)  # add offsets to txn
    now.inc_max(26, 1)  # end txn
    now.inc_max(28, 1)  # txn offset commit
    now.inc_max(32, 2)  # describe configs
    now.inc_max(33, 1)  # alter configs
    now.inc_max(34, 1)  # alter replica log dirs
    now.inc_max(35, 1)  # describe log dirs
    now.inc_max(37, 1)  # create partitions
    now.inc_max(38, 1)  # create delegation token
    now.inc_max(39, 1)  # renew delegation token
    now.inc_max(40, 1)  # expire delegation token
    now.inc_max(41, 1)  # describe delegation token
    now.inc_max(42, 1)  # delete groups
    now.inc_max(29, 1)  # describe acls
    now.inc_max(30, 1)  # create acls
    now.inc_max(31, 1)  # delete acls
    now.inc_max(23, 1)  # offset for leader epoch
    return now


def z21():
    now = z20().clone(2, 1)
    now.inc_max(8, 5)  # offset commit
    now.inc_max(20, 3)  # delete topics
    now.inc_max(1, 9)  # fetch
    now.inc_max(2, 4)  # list offset
    now.inc_max(3, 7)  # metadata
    now.inc_max(8, 6)  # offset commit
    now.inc_max(9, 5)  # offset fetch
    now.inc_max(23, 2)  # offset for leader epoch
    now.inc_max(28, 2)  # txn offset commit
    now.inc_max(0, 7)  # produce
    now.inc_max(1, 10)  # fetch
    return now


def z22():
    now = z21().clone(2, 2)
    now.inc_max(2, 5)  # list offset
    now.inc_max(11, 4)  # join group
    now.inc_max(36, 1)  # sasl authenticate
    now.inc_max(4, 2)  # leader and isr
    now.inc_max(5, 1)  # stop replica
    now.inc_max(6, 5)  # update metadata
    now.inc_max(7, 2)  # controlled shutdown
    now.add_key(43)  # elect preferred leaders
    return now


def z23():
    now = z22().clone(2, 3)
    now.inc_max(3, 8)  # metadata
    now.inc_max(15, 3)  # describe groups
    now.inc_max(1, 11)  # fetch
    now.inc_max(23, 3)  # offset for leader epoch
    now.inc_max(11, 5)  # join group
    now.inc_max(8, 7)  # offset commit
    now.inc_max(12, 3)  # heartbeat
    now.inc_max(14, 3)  # sync group
    now.add_key(44)  # incremental alter configs
    return now


def z24():
    now = z23().clone(2, 4)
    now.inc_max(4, 3)  # leader and isr
    now.inc_max(15, 4)  # describe groups
    now.inc_max(19, 4)  # create topics
    now.inc_max(43, 1)  # elect preferred leaders
    now.add_key(45)  # alter partition reassignments
    now.add_key(46)  # list partition reassignments
    now.add_key(47)  # offset delete
    now.inc_max(13, 3)  # leave group
    # Flexible version bumps
    now.inc_max(3, 9)  # metadata
    now.inc_max(4, 4)  # leader and isr
    now.inc_max(5, 2)  # stop replica
    now.inc_max(6, 6)  # update metadata
    now.inc_max(7, 3)  # controlled shutdown
    now.inc_max(8, 8)  # offset commit
    now.inc_max(9, 6)  # offset fetch
    now.inc_max(10, 3)  # find coordinator
    now.inc_max(11, 6)  # join group
    now.inc_max(12, 4)  # heartbeat
    now.inc_max(13, 4)  # leave group
    now.inc_max(14, 4)  # sync group
    now.inc_max(15, 5)  # describe groups
    now.inc_max(16, 3)  # list group
    now.inc_max(18, 3)  # api versions
    now.inc_max(19, 5)  # create topics
    now.inc_max(20, 4)  # delete topics
    now.inc_max(22, 2)  # init producer id
    now.inc_max(38, 2)  # create delegation token
    now.inc_max(42, 2)  # delete groups
    now.inc_max(43, 2)  # elect preferred leaders
    now.inc_max(44, 1)  # incremental alter configs
    now.inc_max(0, 8)  # produce
    return now


def z25():
    now = z24().clone(2, 5)
    now.inc_max(22, 3)  # init producer id
    now.inc_max(9, 7)  # offset fetch
    now.inc_max(36, 2)  # sasl authenticate
    now.inc_max(37, 2)  # create partitions
    now.inc_max(39, 2)  # renew delegation token
    now.inc_max(40, 2)  # expire delegation token
    now.inc_max(41, 2)  # describe delegation token
    now.inc_max(28, 3)  # txn offset commit
    now.inc_max(29, 2)  # describe acls
    now.inc_max(30, 2)  # create acls
    now.inc_max(31, 2)  # delete acls
    now.inc_max(11, 7)  # join group
    now.inc_max(14, 5)  # sync group
    return now


def z26():
    now = z25().clone(2, 6)
    now.inc_max(21, 2)  # delete records
    now.inc_max(35, 2)  # describe log dirs
    now.add_key(48)  # describe client quotas
    now.add_key(49)  # alter client quotas
    now.inc_max(5, 3)  # stop replica
    now.inc_max(16, 4)  # list group
    now.inc_max(32, 3)  # describe configs
    return now


def z27():
    now = z26().clone(2, 7)
    now.inc_max(37, 3)  # create partitions
    now.inc_max(19, 6)  # create topics
    now.inc_max(20, 5)  # delete topics
    now.inc_max(22, 4)  # init producer id
    now.inc_max(24, 2)  # add partitions to txn
    now.inc_max(25, 2)  # add offsets to txn
    now.inc_max(26, 2)  # end txn
    now.add_key(50)  # describe user scram creds
    now.add_key(51)  # alter user scram creds
    now.inc_max(1, 12)  # fetch
    now.add_key(56)  # alter isr
    now.add_key(57)  # update features
    return now


def z28():
    now = z27().clone(2, 8)
    now.inc_max(0, 9)  # produce
    now.inc_max(2, 6)  # list offsets
    now.inc_max(23, 4)  # offset for leader epoch
    now.inc_max(24, 3)  # add partitions to txn
    now.inc_max(25, 3)  # add offsets to txn
    now.inc_max(26, 3)  # end txn
    now.inc_max(27, 1)  # write txn markers
    now.inc_max(32, 4)  # describe configs
    now.inc_max(33, 2)  # alter configs
    now.inc_max(34, 2)  # alter replica log dirs
    now.inc_max(48, 1)  # describe client quotas
    now.inc_max(49, 1)  # alter client quotas
    now.inc_max(3, 10)  # metadata
    now.inc_max(6, 7)  # update metadata
    now.inc_max(4, 5)  # leader and isr
    now.add_key(60)  # describe cluster
    now.inc_max(3, 11)  # metadata
    now.inc_max(19, 7)  # create topics
    now.inc_max(20, 6)  # delete topics
    now.add_key(61)  # describe producers
    return now


def z30():
    now = z28().clone(3, 0)
    now.add_key(65)  # describe transactions
    now.add_key(66)  # list transactions
    now.add_key(67)  # allocate producer ids
    now.inc_max(2, 7)  # list offsets
    now.inc_max(10, 4)  # find coordinator
    now.inc_max(9, 8)  # offset fetch
    return now


def z31():
    now = z30().clone(3, 1)
    now.inc_max(1, 13)  # fetch
    now.inc_max(3, 12)  # metadata
    return now


def z32():
    now = z31().clone(3, 2)
    now.inc_max(11, 8)  # join group
    now.inc_max(13, 5)  # leave group
    now.inc_max(35, 3)  # describe log dirs
    now.inc_max(11, 9)  # join group
    now.inc_max(4, 6)  # leader and isr
    now.inc_max(56, 1)  # alter partition
    return now


def z33():
    now = z32().clone(3, 3)
    now.inc_max(57, 1)  # update features
    now.inc_max(35, 4)  # describe log dirs
    now.inc_max(56, 2)  # alter partition
    now.inc_max(29, 3)  # describe acls
    now.inc_max(30, 3)  # create acls
    now.inc_max(31, 3)  # delete acls
    now.inc_max(38, 3)  # create delegation token
    now.inc_max(41, 3)  # describe delegation token
    return now


def z34():
    now = z33().clone(3, 4)
    now.inc_max(4, 7)  # leader and isr
    now.inc_max(5, 4)  # stop replica
    now.inc_max(6, 8)  # update metadata
    now.add_key(58)  # envelope
    return now


def z35():
    now = z34().clone(3, 5)
    now.inc_max(1, 14)  # fetch
    now.inc_max(2, 8)  # list offsets
    now.inc_max(1, 15)  # fetch
    now.inc_max(56, 3)  # alter partition
    return now


def z36():
    now = z35().clone(3, 6)
    now.inc_max(24, 4)  # add partitions to txn
    return now


def z37():
    now = z36().clone(3, 7)
    now.inc_max(0, 10)  # produce
    now.inc_max(1, 16)  # fetch
    now.inc_max(8, 9)  # offset commit
    now.inc_max(9, 9)  # offset fetch
    now.inc_max(60, 1)  # describe cluster
    now.add_key(68)  # consumer group heartbeat
    return now


def z38():
    now = z37().clone(3, 8)
    now.inc_max(0, 11)  # produce
    now.inc_max(10, 5)  # find coordinator
    now.inc_max(22, 5)  # init producer id
    now.inc_max(24, 5)  # add partitions to txn
    now.inc_max(25, 4)  # add offsets to txn
    now.inc_max(26, 4)  # end txn
    now.inc_max(28, 4)  # txn offset commit
    now.inc_max(16, 5)  # list groups
    now.add_key(69)  # consumer group describe
    now.inc_max(66, 1)  # list transactions
    return now


def z39():
    now = z38().clone(3, 9)
    now.inc_max(1, 17)  # fetch
    now.inc_max(2, 9)  # list offsets
    now.inc_max(10, 6)  # find coordinator
    now.inc_max(18, 4)  # api versions
    return now


def z_latest():
    """Latest ZooKeeper-based version."""
    return z39()


# KRaft Broker versions
def b28():
    return Release(
        major=2,
        minor=8,
        reqs={
            0: Req(key=0, v_max=9),
            1: Req(key=1, v_max=12),
            2: Req(key=2, v_max=6),
            3: Req(key=3, v_max=11),
            8: Req(key=8, v_max=8),
            9: Req(key=9, v_max=7),
            10: Req(key=10, v_max=3),
            11: Req(key=11, v_max=7),
            12: Req(key=12, v_max=4),
            13: Req(key=13, v_max=4),
            14: Req(key=14, v_max=5),
            15: Req(key=15, v_max=5),
            16: Req(key=16, v_max=4),
            17: Req(key=17, v_max=1),
            18: Req(key=18, v_max=3),
            19: Req(key=19, v_max=7),
            20: Req(key=20, v_max=6),
            21: Req(key=21, v_max=2),
            23: Req(key=23, v_max=4),
            27: Req(key=27, v_max=1),
            32: Req(key=32, v_max=4),
            34: Req(key=34, v_max=2),
            35: Req(key=35, v_max=2),
            36: Req(key=36, v_max=2),
            42: Req(key=42, v_max=2),
            44: Req(key=44, v_max=1),
            47: Req(key=47, v_max=0),
            49: Req(key=49, v_max=1),
            55: Req(key=55, v_max=0),
            60: Req(key=60, v_max=0),
            61: Req(key=61, v_max=0),
            58: Req(key=58, v_max=0),
        }
    )


def b30():
    now = b28().clone(3, 0)
    del now.reqs[55]  # describe quorum not present in 3.0
    now.inc_max(2, 7)
    now.inc_max(9, 8)
    now.inc_max(10, 4)
    now.add_key_and_v_max(22, 4)
    now.add_key_and_v_max(24, 3)
    now.add_key_and_v_max(25, 3)
    now.add_key_and_v_max(26, 3)
    now.add_key_and_v_max(28, 3)
    now.add_key_and_v_max(29, 2)
    now.add_key_and_v_max(30, 2)
    now.add_key_and_v_max(31, 2)
    now.add_key_and_v_max(33, 2)
    now.add_key_and_v_max(37, 3)
    now.add_key_and_v_max(43, 2)
    now.add_key(45)
    now.add_key(46)
    now.add_key_and_v_max(48, 1)
    now.add_key(57)
    now.add_key(65)
    now.add_key(66)
    return now


def b31():
    now = b30().clone(3, 1)
    now.inc_max(1, 13)
    now.inc_max(3, 12)
    return now


def b32():
    now = b31().clone(3, 2)
    now.inc_max(11, 8)
    now.inc_max(11, 9)
    now.inc_max(13, 5)
    now.inc_max(35, 3)
    del now.reqs[58]
    return now


def b33():
    now = b32().clone(3, 3)
    now.inc_max(29, 3)
    now.inc_max(30, 3)
    now.inc_max(31, 3)
    now.inc_max(35, 4)
    now.add_key_and_v_max(55, 1)
    now.inc_max(57, 1)
    now.add_key(64)
    return now


def b34():
    now = b33().clone(3, 4)
    return now


def b35():
    now = b34().clone(3, 5)
    now.inc_max(1, 14)
    now.inc_max(1, 15)
    now.inc_max(2, 8)
    now.add_key(50)
    now.add_key(51)
    return now


def b36():
    now = b35().clone(3, 6)
    now.inc_max(24, 4)
    now.add_key_and_v_max(38, 3)
    now.add_key_and_v_max(39, 2)
    now.add_key_and_v_max(40, 2)
    now.add_key_and_v_max(41, 3)
    return now


def b37():
    now = b36().clone(3, 7)
    now.inc_max(0, 10)
    now.inc_max(1, 16)
    now.inc_max(8, 9)
    now.inc_max(9, 9)
    now.inc_max(60, 1)
    now.add_key(71)
    now.add_key(72)
    now.add_key(68)
    now.add_key(74)
    return now


def b38():
    now = b37().clone(3, 8)
    now.inc_max(0, 11)
    now.inc_max(10, 5)
    now.inc_max(16, 5)
    now.inc_max(22, 5)
    now.inc_max(24, 5)
    now.inc_max(25, 4)
    now.inc_max(26, 4)
    now.inc_max(28, 4)
    now.inc_max(66, 1)
    now.add_key(69)
    now.add_key(75)
    return now


def b39():
    now = b38().clone(3, 9)
    now.inc_max(1, 17)
    now.inc_max(2, 9)
    now.inc_max(10, 6)
    now.inc_max(18, 4)
    now.inc_max(55, 2)
    now.add_key(80)
    now.add_key(81)
    return now


def b40():
    now = b39().clone(4, 0)
    now.set_min(1, 4)
    now.set_min(2, 1)
    now.set_min(8, 2)
    now.set_min(9, 1)
    now.set_min(11, 2)
    now.set_min(19, 2)
    now.set_min(20, 1)
    now.set_min(23, 2)
    now.set_min(27, 1)
    now.set_min(29, 1)
    now.set_min(30, 1)
    now.set_min(31, 1)
    now.set_min(32, 1)
    now.set_min(34, 1)
    now.set_min(35, 1)
    now.set_min(38, 1)
    now.set_min(39, 1)
    now.set_min(40, 1)
    now.set_min(41, 1)

    now.inc_max(0, 12)  # produce
    now.inc_max(2, 10)  # list offsets
    now.inc_max(3, 13)  # metadata
    now.inc_max(15, 6)  # describe groups
    now.inc_max(26, 5)  # end txn
    now.inc_max(28, 5)  # txn offset commit
    now.inc_max(57, 2)  # update features
    now.inc_max(60, 2)  # describe cluster
    now.inc_max(68, 1)  # consumer group heartbeat
    now.inc_max(69, 1)  # consumer group describe
    return now


def b41():
    now = b40().clone(4, 1)
    now.inc_max(0, 13)  # produce
    now.inc_max(1, 18)  # fetch
    now.inc_max(45, 1)  # alter partition assignments
    now.inc_max(66, 2)  # list transactions
    now.inc_max(74, 1)  # list config resources
    now.add_key_and_v_max(76, 1)  # share group heartbeat
    now.add_key_and_v_max(77, 1)  # share group describe
    now.add_key_and_v_max(78, 1)  # share fetch
    now.add_key_and_v_max(79, 1)  # share acknowledge
    now.add_key(83)  # initialize share group state
    now.add_key(84)  # read share group state
    now.add_key(85)  # write share group state
    now.add_key(86)  # delete share group state
    now.add_key(87)  # read share group state summary
    now.add_key(90)  # describe share group offsets
    now.add_key(91)  # alter share group offsets
    now.add_key(92)  # delete share group offsets
    return now


def b_latest():
    """Latest KRaft broker version."""
    return b41()


# KRaft Controller versions
def c28():
    return Release(
        major=2,
        minor=8,
        reqs={
            1: Req(key=1, v_max=12),
            3: Req(key=3, v_max=11),
            7: Req(key=7, v_max=3),
            17: Req(key=17, v_max=1),
            18: Req(key=18, v_max=3),
            19: Req(key=19, v_max=7),
            20: Req(key=20, v_max=6),
            36: Req(key=36, v_max=2),
            44: Req(key=44, v_max=1),
            49: Req(key=49, v_max=1),
            52: Req(key=52, v_max=0),  # vote
            53: Req(key=53, v_max=0),  # begin quorum epoch
            54: Req(key=54, v_max=0),  # end quorum epoch
            55: Req(key=55, v_max=0),  # describe quorum
            56: Req(key=56, v_max=0),  # alter partition
            58: Req(key=58, v_max=0),  # envelope
            59: Req(key=59, v_max=0),  # fetch snapshot
            62: Req(key=62, v_max=0),  # broker registration
            63: Req(key=63, v_max=0),  # broker heartbeat
            64: Req(key=64, v_max=0),  # unregister broker
        }
    )


def c30():
    now = c28().clone(3, 0)
    del now.reqs[3]  # metadata not present in controller 3.0
    now.add_key_and_v_max(29, 2)
    now.add_key_and_v_max(30, 2)
    now.add_key_and_v_max(31, 2)
    now.add_key_and_v_max(33, 2)
    now.add_key_and_v_max(37, 3)
    now.add_key_and_v_max(43, 2)
    now.add_key(45)
    now.add_key(46)
    now.add_key(67)
    return now


def c31():
    now = c30().clone(3, 1)
    now.inc_max(1, 13)
    return now


def c32():
    now = c31().clone(3, 2)
    now.inc_max(56, 1)
    return now


def c33():
    now = c32().clone(3, 3)
    now.inc_max(29, 3)
    now.inc_max(30, 3)
    now.inc_max(31, 3)
    now.inc_max(55, 1)
    now.inc_max(56, 2)
    now.add_key_and_v_max(57, 1)
    return now


def c34():
    now = c33().clone(3, 4)
    now.inc_max(62, 1)
    return now


def c35():
    now = c34().clone(3, 5)
    now.inc_max(1, 14)
    now.inc_max(1, 15)
    now.add_key(50)
    now.add_key(51)
    now.inc_max(56, 3)
    return now


def c36():
    now = c35().clone(3, 6)
    now.add_key_and_v_max(38, 3)
    now.add_key_and_v_max(39, 2)
    now.add_key_and_v_max(40, 2)
    now.add_key_and_v_max(41, 3)
    return now


def c37():
    now = c36().clone(3, 7)
    now.inc_max(1, 16)
    now.add_key_and_v_max(32, 4)
    now.add_key_and_v_max(60, 1)
    now.inc_max(62, 2)
    now.inc_max(62, 3)
    now.inc_max(63, 1)
    now.add_key(70)  # controller registration
    now.add_key(73)  # assign replica to dirs
    return now


def c38():
    now = c37().clone(3, 8)
    return now


def c39():
    now = c38().clone(3, 9)
    now.inc_max(1, 17)
    now.inc_max(18, 4)
    now.inc_max(52, 1)
    now.inc_max(53, 1)
    now.inc_max(54, 1)
    now.inc_max(59, 1)
    now.inc_max(55, 2)
    now.inc_max(62, 4)
    now.add_key(80)  # add raft voter
    now.add_key(81)  # remove raft voter
    now.add_key(82)  # update raft voter
    return now


def c40():
    now = c39().clone(4, 0)
    now.set_min(1, 4)
    now.set_min(19, 2)
    now.set_min(20, 1)
    now.set_min(29, 1)
    now.set_min(30, 1)
    now.set_min(31, 1)
    now.set_min(32, 1)
    now.set_min(38, 1)
    now.set_min(39, 1)
    now.set_min(40, 1)
    now.set_min(41, 1)
    now.set_min(56, 2)
    del now.reqs[7]

    now.inc_max(52, 2)
    now.inc_max(57, 2)
    now.inc_max(60, 2)
    return now


def c41():
    now = c40().clone(4, 1)
    now.inc_max(1, 18)
    now.inc_max(45, 1)
    return now


def c_latest():
    """Latest KRaft controller version."""
    return c41()
