import pickle

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import TestCase

from lazycache.lists import CachedList


class TestUserCachedList(CachedList):

    def identify_items(self, users):
        for user in users:
            yield user.pk

    def make_cache_keys(self, user_pks):
        for user_pk in user_pks:
            yield 'TestUserCacheList:%d' % user_pk

    def rebuild_items(self, user_pks):
        return User.objects.filter(pk__in=user_pks)


class CachedListTests(TestCase):

    def test_cached_list(self):

        # Set up the test data.
        users = User.objects.all()[:10]
        user_cache = TestUserCachedList(users)
        self.assertEqual([user.pk for user in users], [user.pk for user in user_cache])

        # Force it through the pickle cycle.
        user_cache = pickle.loads(pickle.dumps(user_cache))
        self.assertEqual([user.pk for user in users], [user.pk for user in user_cache])

        # The pickle size is greatly reduced. While making this test, it went
        # from 6377 bytes to 201 bytes. To avoid a brittle test, just check
        # that it's less that half the size.
        normal_pickle_size = len(pickle.dumps(users))
        improved_pickle_size = len(pickle.dumps(user_cache))
        self.assertTrue(improved_pickle_size < normal_pickle_size / 2.0)

        # Force it through the cache cycle.
        cache_key = 'apncore.cache.tests.test_cached_list'
        user_cache.cache(cache_key)
        user_cache = cache.get(cache_key)
        self.assertEqual([user.pk for user in users], [user.pk for user in user_cache])

        # Delete the cached items, forcing the class to rebuild them.
        # The main list must be retrieved again to test unpacking its items.
        item_cache_keys = list(user_cache.make_cache_keys([user.pk for user in users]))
        cache.delete_many(item_cache_keys)
        user_cache = cache.get(cache_key)
        self.assertEqual([user.pk for user in users], [user.pk for user in user_cache])
