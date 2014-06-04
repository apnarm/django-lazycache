import pickle

from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from lazymodel import LazyModel, LazyModelDict, ModelWithCaching
from lazymodel.backend import lazymodel_cache
from lazymodel.models import Account, PhotoGallery
from lazymodel.utils import get_identifier, lookup_cache_key, model_cache_key


class ModelCacheTests(TestCase):
    """
    Tests for ModelWithCaching and also how LazyModel interacts with it.

    """

    def assertUncached(self, cache_key, message=None):
        try:
            lazymodel_cache[cache_key]
        except KeyError:
            pass
        else:
            if not message:
                message = 'Expected cache key %r to be missing, but it was found.'
            self.fail(message)

    def test_cache_version_setting(self):
        """
        Ensure that when the cache key version is changed,
        the model cache keys will be changed too.

        """

        gallery = PhotoGallery.objects.all()[0]

        key_before = model_cache_key(gallery)

        cache_version = settings.CACHE_KEY_VERSIONS['model_cache']
        settings.CACHE_KEY_VERSIONS['model_cache'] = cache_version + 'test'

        key_after = model_cache_key(gallery)
        self.assertNotEqual(key_before, key_after)

    def test_cache_sharing(self):

        gallery = PhotoGallery.objects.all()[0]
        self.assertTrue(isinstance(gallery, ModelWithCaching), 'This test needs to test a cached model.')

        cache_key = model_cache_key(gallery)
        del lazymodel_cache[cache_key]

        # Test the following accessors, they should work as normal and
        # add the result to the cache.
        self.assertEqual(PhotoGallery.objects.get(pk=gallery.pk), gallery)
        self.assertEqual(lazymodel_cache[cache_key], gallery)
        del lazymodel_cache[cache_key]
        self.assertEqual(gallery, LazyModel(PhotoGallery, gallery.pk))
        self.assertEqual(lazymodel_cache[cache_key], gallery)

        # Put a dummy value into the cache key which both accessors use.
        # Both accessors should then return that dummy value when called.
        dummy = 'dummy value for the cache'
        lazymodel_cache[cache_key] = dummy
        self.assertEqual(dummy, PhotoGallery.objects.get(pk=gallery.pk))
        self.assertEqual(dummy, LazyModel(PhotoGallery, gallery.pk))
        del lazymodel_cache[cache_key]

    def test_lookup_keys(self):
        gallery = PhotoGallery.objects.all()[0]
        PhotoGallery.objects.get(slug=gallery.slug)
        bool(LazyModel(PhotoGallery, slug=gallery.slug))
        PhotoGallery.objects.get(slug=gallery.slug)
        bool(LazyModel(PhotoGallery, slug=gallery.slug))

    def test_that_it_works(self):
        """
        Check that this whole thing works.
        It's a bit complicated.

        """

        gallery = PhotoGallery.objects.all().filter(sites__isnull=False)[0]
        self.assertTrue(isinstance(gallery, ModelWithCaching), 'This test needs to test a cached model.')
        for gallery in PhotoGallery.objects.filter(slug=gallery.slug).exclude(pk=gallery.pk):
            gallery.delete()

        pk_key = model_cache_key(gallery)
        lookup_key = lookup_cache_key(PhotoGallery, slug=gallery.slug)

        # Accessing with a pk should add a cached version of the object
        # to the cache without any fuss.
        del lazymodel_cache[pk_key]
        self.assertEqual(PhotoGallery.objects.get(pk=gallery.pk), gallery)
        self.assertEqual(lazymodel_cache[pk_key], gallery)
        del lazymodel_cache[pk_key]
        self.assertEqual(LazyModel(PhotoGallery, gallery.pk), gallery)
        self.assertEqual(lazymodel_cache[pk_key], gallery)

        # Accessing via ORM lookups, such as using the slug, should cache a
        # lookup_key that is only a reference to the object pk. The cache key
        # using that object pk should then contain the cached object as usual.
        del lazymodel_cache[lookup_key]
        del lazymodel_cache[pk_key]
        self.assertEqual(PhotoGallery.objects.get(slug=gallery.slug), gallery)
        self.assertEqual(lazymodel_cache[lookup_key], gallery.pk)
        self.assertEqual(lazymodel_cache[pk_key], gallery)

        # Removing a site from the gallery should trigger the m2m signals that
        # delete the cached object.
        gallery.sites.remove(gallery.sites.all()[0])
        self.assertUncached(pk_key, 'M2M changes did not delete the cached value!')

        # The lookup key will still be there.
        self.assertEqual(lazymodel_cache[lookup_key], gallery.pk)

        # Saving the object should also delete the cached object.
        # First access the object to add it back to the cache.
        self.assertEqual(PhotoGallery.objects.get(pk=gallery.pk), gallery)
        self.assertEqual(lazymodel_cache[pk_key], gallery)
        gallery.save()
        self.assertUncached(pk_key, 'Saving did not delete the cached value!')

        # Deleting the object should also delete from the cache.
        self.assertEqual(PhotoGallery.objects.get(pk=gallery.pk), gallery)
        self.assertEqual(lazymodel_cache[pk_key], gallery)
        gallery.delete()
        self.assertUncached(pk_key, 'Deleting did not delete the cached value!')

    def test_saving_content_type(self):
        """
        The model cache key stuff has special handling to allow passing in a
        content type instead of the model. Sometimes, we are actually working
        with the content type and not the model it represents, so make sure
        that works.

        """

        ContentType.objects.all()[0].save()


class LazyModelTests(TestCase):

    def test_identifier(self):
        """
        Ensure that get_identifier is consistent when given a single object
        using different parameter types.

        """

        results = set()

        # This is what get_identifier should return in each case.
        identifier = 'auth.user.1'
        results.add(identifier)

        # Call with a pre-created identifier.
        results.add(get_identifier(identifier))

        # Call with the Model and object_pk
        results.add(get_identifier(User, 1))

        # Call with the content type and pk
        user_content_type = ContentType.objects.get_for_model(User)
        results.add(get_identifier(user_content_type, 1))

        # Call with a model instance.
        results.add(get_identifier(User.objects.get(pk=1)))

        self.assertEqual(len(results), 1, 'Inconsistent results from get_identifier: %s' % list(results))

    def test_pickle(self):
        """Test that you can pickle and unpickle LazyModel instances."""

        gallery = PhotoGallery.objects.order_by('-id')[0]

        # Check all ways of creating them.
        lazy_galleries = (
            LazyModel(gallery),
            LazyModel(get_identifier(gallery)),
            LazyModel(PhotoGallery, gallery.pk),
            LazyModel(PhotoGallery, slug=gallery.slug),
        )

        for lazy_gallery in lazy_galleries:

            pickled = pickle.dumps(lazy_gallery)
            unpickled = pickle.loads(pickled)

            self.assertEqual(
                gallery.get_absolute_url(),
                unpickled.get_absolute_url(),
            )

        # Check that invalid instances can still be pickled.
        broken_gallery = LazyModel(PhotoGallery, gallery.pk + 10000000)
        pickled = pickle.dumps(broken_gallery)
        unpickled = pickle.loads(pickled)
        self.assertEqual(
            broken_gallery.object_pk,
            unpickled.object_pk,
        )

    def test_lookup(self):
        """Test that the keyword lookup arguments work."""

        user = User.objects.filter(account__isnull=False)[0]
        account = user.account

        self.assertEqual(LazyModel(Account, account.id), account)
        self.assertEqual(LazyModel(Account, user__id=user.id), account)
        self.assertEqual(LazyModel(Account, user__id=user.id, locality__id=account.locality.id), account)
        self.assertEqual(user.lazy_account, account)

    def test_lazy_model_dict(self):

        user1, user2 = User.objects.all()[:2]

        users = LazyModelDict()
        self.assertEqual(len(users), 0)

        # Add a User instance to the dict.
        # A new LazyModel instance should be returned.
        user1 = users.get_or_add(user1)
        self.assertEqual(len(users), 1)
        self.assertTrue(LazyModel.get_identifier(user1) in users)
        self.assertTrue(isinstance(user1, LazyModel))

        # Add the user again.
        # The previous LazyModel instance should be returned.
        new_user1 = users.get_or_add(User, user1.pk)
        self.assertEqual(len(users), 1)
        self.assertTrue(new_user1 is user1)

        # Add the other user.
        # A new LazyModel instance should be returned.
        users.get_or_add(user2)
        self.assertEqual(len(users), 2)
        self.assertTrue(LazyModel.get_identifier(user2) in users)
