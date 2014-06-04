from itertools import izip

from threading import RLock

from django.core.cache import cache


class CachedList(list):
    """
    This list will, when pickled, cache each item individually and then only
    reference those items by their identifiers. This allows a list that might
    ordinarily exceed the maximum cache size to be stored in smaller pieces
    that fit within the limit.

    This uses the set_many and get_many features of the cache backend
    to optimize cache access.

    """

    _unpack_lock = RLock()

    def __init__(self, items, cache_backend=cache, cache_timeout=None):
        super(CachedList, self).__init__(items)
        self.cache_backend = cache_backend
        self.cache_timeout = cache_timeout

    def __iter__(self):
        with self._unpack_lock:
            self._unpack_items()
        return super(CachedList, self).__iter__()

    def __getitem__(self, i):
        with self._unpack_lock:
            self._unpack_items()
        return super(CachedList, self).__getitem__(i)

    def __getslice__(self, i, j):
        with self._unpack_lock:
            if hasattr(self, '_unpack'):
                # Avoid unpacking all items when slicing an unpacked list.
                # Instead, make a new list and unpack that.
                items = super(CachedList, self).__getslice__(i, j)
                new_list = self.__class__(items)
                new_list._unpack = True
                new_list._unpack_items()
                return new_list[:]
        return super(CachedList, self).__getslice__(i, j)

    def __reduce__(self):
        """
        When pickling instances of this class, pack the items so that only
        their identifiers are required to reference them. By packing the items
        and storing only their identifiers in this list, the cache value size
        of this list can be greatly reduced.

        """
        init_args = (
            self.__class__,
            self._pack_items(),
        )
        if self.cache_timeout:
            init_kwargs = {'cache_timeout': self.cache_timeout}
        else:
            init_kwargs = {}
        return (_unpickle_cached_list, init_args, init_kwargs)

    def _pack_items(self):
        """
        Reduce the items in this list to identifiers that can be used to
        recreate them from scratch. This adds each item to the cache too.

        """
        identifiers = tuple(self.identify_items(self))
        cache_keys = self.make_cache_keys(identifiers)
        cache_items = dict(izip(cache_keys, self))
        self.cache.set_many(cache_items, self.cache_timeout)
        return identifiers

    def _unpack_items(self):
        """
        Update the values of this list to the items which the identifiers
        represent. They are either found in the cache or rebuilt and added
        to the cache.

        """

        # Prevent the unpack operation from occurring more than once.
        if hasattr(self, '_unpack'):
            delattr(self, '_unpack')
        else:
            return

        # The list contains identifiers that will be unpacked into real items.
        # Copy them so they won't be lost when the list values are altered.
        identifiers = self[:]

        cache_keys = dict(izip(identifiers, self.make_cache_keys(identifiers)))
        cached_items = self.cache_backend.get_many(cache_keys.values())

        items = {}
        missed = []
        for identifier, cache_key in cache_keys.items():
            try:
                item = cached_items[cache_key]
                assert item is not None
            except (AssertionError, KeyError):
                missed.append(identifier)
            else:
                items[identifier] = item

        if missed:

            # Rebuild the missing items using their identifiers and
            # replace the contents of this list with those new items.
            self[:] = self.rebuild_items(missed)

            # Use the pack_items method to add them to the cache and also
            # get back their identifiers. Finally, put the new items into
            # the items dict to be returned at the end.
            found_identifiers = self._pack_items()
            items.update(izip(found_identifiers, self))

        # Replace the value of this list with the final result.
        del self[:]
        for identifier in identifiers:
            item = items.get(identifier)
            if item is not None:
                self.append(item)

    def cache(self, key, timeout=None):
        """
        Cache this list using the given key and timeout value. The timeout
        is also used when caching the individual items of this list.

        If this list is cached manually, without using this method, then the
        items will be cached using the default timeout and may end up expiring
        before the list, resulting in a rebuild of those items next time the
        list is fetched from the cache.

        """
        original_cache_timeout = self.cache_timeout
        self.cache_timeout = timeout
        self.cache_backend.set(key, self, timeout)
        self.cache_timeout = original_cache_timeout

    def identify_items(self, items):
        """
        Return the identifiers for every item, in the same order. Each
        identifier must be enough to recreate the item that it represents.
        Identifiers can be of any type as long as 1) they can be pickled
        and 2) they work with make_cache_keys and rebuild_items.

        """

        raise NotImplementedError

    def make_cache_keys(self, identifiers):
        """
        Return cache keys to be used for every item that the identifiers
        represent, in the same order.

        """

        raise NotImplementedError

    def rebuild_items(self, identifiers):
        """
        Return objects which the identifiers represent. Order does not matter.
        This is required when cached items are not found and need to be
        created again.

        """
        raise NotImplementedError


def _unpickle_cached_list(cls, *args, **kwargs):
    """
    When unpickling the list, attach an attribute which tells it to unpack
    the values when first accessed. This is done lazily because the cache
    backend breaks if trying to access the cache while in the middle of
    unpickling a cached object.

    """
    new_list = cls(*args, **kwargs)
    new_list._unpack = True
    return new_list
