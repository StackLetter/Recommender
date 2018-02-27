
_memoized = {}
def memoize(f):
    def memoized(*args, **kwargs):
        key = (args[0])
        if key not in _memoized:
            _memoized[key] = f(*args, **kwargs)
        return _memoized[key]
    return memoized
