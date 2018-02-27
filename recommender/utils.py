from collections import Counter

_access_counter = Counter()
_memoized = {}
def memoize(f):
    def memoized(*args, **kwargs):
        key = (id(f), args[0])
        if key not in _memoized:
            _memoized[key] = f(*args, **kwargs)
        _access_counter[key] += 1
        return _memoized[key]
    return memoized
