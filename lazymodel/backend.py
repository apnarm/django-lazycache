from django.conf import settings
from django.core.cache import cache

from lazycache import LazyCache


lazymodel_cache = LazyCache(
    cache=cache,
    default_timeout=int(getattr(settings, 'LAZYMODEL_CACHE_SECONDS', 60 * 60 * 24)),
)
