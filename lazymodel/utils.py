import inspect
import hashlib
import re

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db.models import Model
from django.utils.encoding import force_unicode

from lazymodel.backend import lazymodel_cache


IDENTIFIER_REGEX = re.compile('^[\w\d_]+\.[\w\d_]+\.[\w\d]+$')


class HashableTuple(tuple):

    def __new__(cls, *items):
        return tuple.__new__(cls, cls._create_sequence(items))

    @classmethod
    def _create_one(cls, item):
        if isinstance(item, cls):
            return item
        elif isinstance(item, (list, tuple, set)):
            return tuple(cls._create_sequence(*item))
        if isinstance(item, dict):
            return tuple((key, cls._create_one(value)) for (key, value) in sorted(item.items()))
        elif inspect.isclass(item) or inspect.isfunction(item) or inspect.ismethod(item):
            return item.__name__
        elif isinstance(item, (int, basestring)):
            return force_unicode(item)
        else:
            return item

    @classmethod
    def _create_sequence(cls, *items):
        for item in items:
            yield cls._create_one(item)

    @property
    def hash(self):
        return hashlib.sha256(repr(self)).hexdigest()


def get_model(obj):
    """
    Returns a model class or instance.
    Performs no validation.

    """
    if isinstance(obj, ContentType):
        return obj.model_class()
    return obj


def get_identifier(obj_or_string, pk=None, **kwargs):
    """
    Get an unique identifier for a database object or model + pk,
    or a string representing the object.

    Taken from Haystack and modified.

    """

    if isinstance(obj_or_string, basestring):
        if not IDENTIFIER_REGEX.match(obj_or_string):
            raise ValueError('Provided string %r is not a valid identifier.' % obj_or_string)
        return obj_or_string

    model = get_model(obj_or_string)

    if pk is None:
        if kwargs:
            for key, value in kwargs.items():
                if key in ('id', 'id__exact', 'pk', 'pk__exact'):
                    pk = value
                    break
            else:
                pk = get_object_pk(model, **kwargs)
        elif isinstance(model, Model):
            pk = model._get_pk_val()

    return get_identifier_string(model, pk)


def get_identifier_string(model, pk):
    """This must match haystack.utils.get_identifier exactly!"""
    return u'%s.%s.%s' % (
        model._meta.app_label,
        model._meta.module_name,
        str(pk).replace(' ', ''),
    )


def get_object_pk(model, **kwargs):
    cache_key = lookup_cache_key(model, **kwargs)
    object_pk = lazymodel_cache.get_or_miss(cache_key)
    if object_pk is lazymodel_cache.missed:
        try:
            object_pk = model.objects.get(**kwargs).pk
        except model.DoesNotExist:
            object_pk = None
        lazymodel_cache[cache_key] = object_pk
    return object_pk


def versioned_cache_key(namespace, cache_key):
    versions = getattr(settings, 'CACHE_KEY_VERSIONS', {})
    version = versions.get('model_cache') or settings.VERSION
    return '%s:%s:%s' % (namespace, version, cache_key)


def model_cache_key(obj_or_string, pk=None, **kwargs):
    identifier = get_identifier(obj_or_string, pk=pk, **kwargs)
    return versioned_cache_key('ModelCache', identifier)


def lookup_cache_key(model, **kwargs):
    identifier = get_identifier(model, HashableTuple(kwargs).hash)
    return versioned_cache_key('ModelCacheLookup', identifier)
