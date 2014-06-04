import logging

from django.core.exceptions import ObjectDoesNotExist
from django.contrib.contenttypes.models import ContentType
from django.db import models, DatabaseError
from django.db.models.signals import m2m_changed, pre_delete, post_delete, post_save
from django.db.models.base import ModelBase
from django.utils.functional import SimpleLazyObject

from lazymodel.backend import lazymodel_cache
from lazymodel.utils import (
    get_identifier,
    get_identifier_string,
    lookup_cache_key,
    model_cache_key,
)

try:
    import psycopg2
except:
    DatabaseExceptions = (DatabaseError,)
else:
    DatabaseExceptions = (DatabaseError, psycopg2.Error)


class LazyModelError(ValueError):
    pass


class LazyModel(SimpleLazyObject):
    """
    A lazy wrapper for a database object, with caching.

    The cache layer uses the same cache keys as ModelWithCaching,
    and both rely on the same signal handlers for invalidation.

    Raises a LazyModelError (subclass of ValueError) if fail_silently=False.

    """

    def __init__(self, object_or_string, *args, **kwargs):
        self._wrapped = None
        self.__dict__['_fail_silently'] = kwargs.pop('fail_silently', True)
        self.__dict__['_cache_backend'] = kwargs.pop('cache_backend', lazymodel_cache)
        self.__dict__['_setupfunc'] = self._get_cached_instance
        self.__dict__['_init_args'] = (object_or_string, args, kwargs)

    def __nonzero__(self):
        if self._wrapped is None:
            self._setup()
        return bool(self._wrapped)

    def __reduce_ex__(self, proto):
        """
        Allow lazy model instances to be pickled.

        The cache backend will not retained;
        the default backend will be used when the object is unpickled.

        """

        (object_or_string, args, kwargs) = self._init_args

        # If a model was used to initialize this object, then swap it out for
        # its identifier string. The resulting data will be much smaller.
        if isinstance(object_or_string, models.Model):
            object_or_string = self.get_identifier(object_or_string)

        return (unpickle_lazy_object, (object_or_string, args, kwargs))

    def _get_cached_instance(self):
        """
        A cache wrapper around _get_instance, using the same cache keys
        as the row-level cache.

        If no object was found, then return a False instead of None. This is
        necessary because the LazyObject code relies on None to mean that it
        has not been evaluated yet. And I don't feel like rebuilding the whole
        class to avoid that reliance.

        """

        try:
            identifier = self._get_identifier()
        except ValueError as error:
            if self._fail_silently:
                return False
            else:
                raise LazyModelError(error)

        # Get the cache key, basically just namespacing
        # and versioning the identifier.
        cache_key = model_cache_key(identifier)

        try:
            instance = self._cache_backend[cache_key]
        except KeyError:
            instance = self._get_instance(identifier)
            self._cache_backend[cache_key] = instance

        if instance is None:
            if self._fail_silently:
                return False
            else:
                raise LazyModelError('%s not found.' % identifier)
        else:
            return instance

    def _get_identifier(self):
        """Get the identifier string for the represented object."""

        if '_identifier' not in self.__dict__:

            object_or_string, args, kwargs = self._init_args

            # Get the identifier for the wrapped object, e.g. 'auth.user.1234'
            # If there is a lookup in the kwargs, then the following call
            # will figure out the object_pk. It caches these lookups.
            self.__dict__['_identifier'] = get_identifier(object_or_string, *args, **kwargs)

        return self.__dict__['_identifier']

    def _get_instance(self, identifier):
        """Get the object from the database."""
        try:
            app_label, model, object_pk = identifier.split('.', 2)
            if object_pk == 'None':
                raise ObjectDoesNotExist
            content_type = ContentType.objects.get_by_natural_key(app_label, model)
            return content_type.get_object_for_this_type(pk=object_pk)
        except ContentType.DoesNotExist:
            logging.warning('Could not find content type for %r' % identifier)
        except ObjectDoesNotExist:
            logging.warning('Could not find related object for %r' % identifier)
        except DatabaseExceptions:
            raise
        except Exception as error:
            logging.error('Could not get related object for %r - %s' % (identifier, error))

    @classmethod
    def get_identifier(cls, *args, **kwargs):
        if args and type(args[0]) is cls:
            # Handle instances of this class differently.
            object_or_string, args, kwargs = args[0]._init_args
            args = [object_or_string] + list(args)
        return get_identifier(*args, **kwargs)

    @classmethod
    def get_model_class(cls, *args, **kwargs):
        identifier = cls.get_identifier(*args, **kwargs)
        app_label, model, object_pk = identifier.split('.', 2)
        content_type = ContentType.objects.get_by_natural_key(app_label, model)
        return content_type.model_class()

    @property
    def object_pk(self):
        """The object pk value as a string."""

        if self._wrapped not in (None, False):
            return str(self._wrapped.pk)

        if '_object_pk' in self.__dict__:
            return self.__dict__['_object_pk']

        identifier = self._get_identifier()
        if identifier:
            try:
                object_pk = identifier.split('.', 2)[2]
                if object_pk == 'None':
                    object_pk = None
                self.__dict__['_object_pk'] = object_pk
                return object_pk
            except Exception:
                pass

        raise AttributeError


def unpickle_lazy_object(object_or_string, args, kwargs):
    return LazyModel(object_or_string, *args, **kwargs)


class LazyModelDict(dict):
    """
    A dictionary of LazyModel instances.  Use this to avoid duplicate
    database/cache lookups and having duplicate model instances in memory.

    """

    def get_or_add(self, *args, **kwargs):
        """
        Get or add a LazyModel instance to this dictionary. Accepts the same
        arguments as the LazyModel class. Returns a LazyModel instance.

        Note: this will evaluate LazyModel instances when adding new ones.

        Usage:
            items = LazyModelDict()
            user = items.get_or_add(User, 123)

        """

        key = LazyModel.get_identifier(*args, **kwargs)
        try:
            return self[key]
        except KeyError:
            item = LazyModel(*args, **kwargs)
            if not item:
                item = None
            self[key] = item
            return item


class RelatedFieldManager(models.Manager):

    use_for_related_fields = True

    def using(self, *args, **kwargs):
        """
        Returns a QuerySet using the selected database.

        This will also override the QuerySet's "get" method to use the
        manager's "get" method instead, taking advantage of caching. This
        only applies to the first, top-level QuerySet; filtering it will
        clone the queryset and then not having caching applied.

        The purpose of this hack is to apply caching to ForeignKeyField
        properties, which end up using this method to get related values.

        """

        queryset = self.get_query_set().using(*args, **kwargs)

        manager = self

        def get(*args, **kwargs):
            return manager.get(*args, **kwargs)

        queryset.get = get

        return queryset


class CachedGetManager(RelatedFieldManager):
    """
    Manager for caching results of the get() method. Uses an ordinary
    dictionary by default, but can be overridden to use anything that
    supports dictionary-like access, such as a memcache wrapper.

    This does not support invalidation.

    """

    cache_backend = {}

    def get(self, *args, **kwargs):
        if not args and len(kwargs) == 1:
            key, value = kwargs.items()[0]
            if key in ('id', 'id__exact', 'pk', 'pk__exact'):
                pk = value
                try:
                    result = self.cache_backend[pk]
                except KeyError:
                    result = super(CachedGetManager, self).get(*args, **kwargs)
                    self.cache_backend[pk] = result
                return result
        return super(CachedGetManager, self).get(*args, **kwargs)


class RowCacheManager(RelatedFieldManager):
    """
    Manager for caching single-row queries. To make invalidation easy,
    we use an extra layer of indirection. The query arguments are used as a
    cache key, whose stored value is the object pk, from which the final pk
    cache key can be generated. When a model using RowCacheManager is saved,
    this pk cache key should be invalidated. Doing two memcached queries is
    still faster than fetching from the database.

    """

    cache_backend = lazymodel_cache

    def get(self, *args, **kwargs):

        if len(kwargs) == 1 and kwargs.keys()[0] in ('id', 'id__exact', 'pk', 'pk__exact'):
            # Generate the cache key directly, since we have the id/pk.
            pk_key = model_cache_key(self.model, kwargs.values()[0])
            lookup_key = None
        else:
            # This lookup is not simply an id/pk lookup.
            # Get the cache key for this lookup.

            # Handle related managers, which automatically use core_filters
            # to filter querysets using the related object's ID.
            core_filters = getattr(self, 'core_filters', None)
            if isinstance(core_filters, dict):
                # Combine the core filters and the kwargs because that is
                # basically what the related manager will do when building
                # the queryset.
                lookup_kwargs = dict(core_filters)
                lookup_kwargs.update(kwargs)
                lookup_key = lookup_cache_key(self.model, **lookup_kwargs)
            else:
                lookup_key = lookup_cache_key(self.model, **kwargs)

            # Try to get the cached pk_key.
            object_pk = self.cache_backend.get(lookup_key)
            pk_key = object_pk and model_cache_key(self.model, object_pk)

        # Try to get a cached result if the pk_key is known.
        result = pk_key and self.cache_backend.get(pk_key)

        if not result:

            # The result was not cached, so get it from the database.
            result = super(RowCacheManager, self).get(*args, **kwargs)
            object_pk = result.pk

            # And cache the result against the pk_key for next time.
            pk_key = model_cache_key(result, object_pk)
            self.cache_backend[pk_key] = result

            # If a lookup was used, then cache the pk against it. Next time
            # the same lookup is requested, it will find the relevent pk and
            # be able to get the cached object using that.
            if lookup_key:
                self.cache_backend[lookup_key] = object_pk

        # Return the cache-protected object.
        return result


class MetaCaching(ModelBase):
    """
    Sets .objects on any model that inherits from ModelWithCaching to be a
    RowCacheManager. This is tightly coupled to Django internals, so it could
    break if you upgrade Django. This was done partially as a proof-of-concept.

    """

    def __new__(*args, **kwargs):
        new_class = ModelBase.__new__(*args, **kwargs)
        new_manager = RowCacheManager()
        if not hasattr(new_class, 'objects'):
            # Attach a new manager.
            new_manager.contribute_to_class(new_class, 'objects')
            new_class._default_manager = new_manager
        else:
            # Mix in the manager into the existing one.
            if new_class.objects.__class__ != RowCacheManager and RowCacheManager not in new_class.objects.__class__.__bases__:
                new_class.objects.__class__.__bases__ = (RowCacheManager,) + new_class.objects.__class__.__bases__
        return new_class


class ModelWithCaching(models.Model):

    __metaclass__ = MetaCaching

    class Meta:
        abstract = True

    # If we ever have issues with related fields being cached, use this:
    #def __reduce__(self):
    #    default = super(ModelWithCaching, self).__reduce__()
    #    (model_unpickle, (model, defers, factory), data) = default
    #    for field in self._meta.fields:
    #        if hasattr(field, 'get_cache_name'):
    #            cache_name = field.get_cache_name()
    #            if cache_name in data:
    #                del data[cache_name]
    #    return (model_unpickle, (model, defers, factory), data)


def remove_object_from_cache(sender, instance, **kwargs):
    if isinstance(instance, ContentType):
        # The model cache key stuff has special handling to allow passing
        # in a content type instead of the model. At this point though, we are
        # actually working with the content type itself and not the model it
        # represents. So we need to bypass that special handling code.
        cache_key = model_cache_key(get_identifier_string(instance, instance.pk))
    else:
        cache_key = model_cache_key(instance)
    lazymodel_cache.delete(cache_key)


pre_delete.connect(remove_object_from_cache)
post_delete.connect(remove_object_from_cache)
post_save.connect(remove_object_from_cache)
m2m_changed.connect(remove_object_from_cache)
