import sys
import types

if sys.version_info >= (3, 12, 0):
    m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
    setattr(m, 'range', range)
    sys.modules['kafka.vendor.six.moves'] = m
