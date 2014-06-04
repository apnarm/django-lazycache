class Missed(object):
    pass


class Null:
    pass


class RaiseKeyError:
    pass


class LazyCache(object):
    """Wraps a Django cache object to provide more features."""

    missed = Missed()

    def __init__(self, cache, default_timeout=None):
        self.cache = cache
        self.default_timeout = default_timeout

    def __getattr__(self, name):
        return getattr(self, self.cache, name)

    def __getitem__(self, key):
        return self.get(key, default=RaiseKeyError)

    def __delitem__(self, key):
        self.delete(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def _prepare_value(self, value):
        if value is None:
            value = Null
        return value

    def _restore_value(self, key, value):
        if value is Null:
            return None
        if value is RaiseKeyError:
            raise KeyError('"%s" was not found in the cache.' % key)
        return value

    def add(self, key, value, timeout=0, **kwargs):
        value = self._prepare_value(key, value, timeout)
        return self.cache.add(key, value, timeout=timeout, **kwargs)

    def get(self, key, default=None, **kwargs):
        value = self.cache.get(key, default=default, **kwargs)
        value = self._restore_value(key, value)
        return value

    def get_many(self, keys, **kwargs):
        data = self.cache.get_many(keys, **kwargs)
        restored_data = {}
        for key, value in data.items():
            value = self._restore_value(key, value)
            restored_data[key] = value
        return restored_data

    def get_or_miss(self, key, miss=False):
        """
        Returns the cached value, or the "missed" object if it was not found
        in the cache. Passing in True for the second argument will make it
        bypass the cache and always return the "missed" object.

        Example usage:

            def get_value(refresh_cache=False):
                key = 'some.key.123'
                value = cache.get_or_miss(key, refresh_cache)
                if value is cache.missed:
                    value = generate_new_value()
                    cache.set(key, value)
                return value

        """

        return miss and self.missed or self.get(key, default=self.missed)

    def set(self, key, value, timeout=None, **kwargs):
        if timeout is None:
            timeout = self.default_timeout
        value = self._prepare_value(key, value, timeout)
        return self.cache.set(key, value, timeout=timeout, **kwargs)

    def set_many(self, data, timeout=None, **kwargs):
        if timeout is None:
            timeout = self.default_timeout
        prepared_data = {}
        for key, value in data.items():
            value = self._prepare_value(key, value, timeout)
            prepared_data[key] = value
        self.cache.set_many(prepared_data, timeout=timeout, **kwargs)
