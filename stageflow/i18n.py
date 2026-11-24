"""Translations: the framework's own messages and stage descriptions.

The framework's strings (validation errors, built-in stage descriptions) are
written in English and translated through a gettext catalog shipped in the
package. Available locales are the catalogs found on disk.

A host's stages can use a per-locale mapping instead of a catalog::

    description:
      en: "Takes a ticket out of the queue"
      ru: "Берёт тикет из очереди"

The current locale is a `ContextVar`, so concurrent requests can use
different languages. Without one, the English source strings are used.
"""

from __future__ import annotations

import gettext as _gettext
import re
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

#: The language of the source strings.
SOURCE_LOCALE = "en"

#: The framework's catalog. Hosts register their own domains (`register_domain`).
DOMAIN = "stageflow"

_DOMAINS: dict[str, Path] = {DOMAIN: Path(__file__).with_name("locale")}
_CATALOGS: dict[tuple[str, str], _gettext.NullTranslations] = {}
_NULL = _gettext.NullTranslations()

#: Per request/task, not per process.
_current: ContextVar[str | None] = ContextVar("stageflow_locale", default=None)

_TAG_RE = re.compile(r"^([A-Za-z]{2,8})(?:[-_]([A-Za-z]{4}))?(?:[-_]([A-Za-z]{2}|\d{3}))?")


# -------------------------------------------------------------------- domains


def register_domain(domain: str, directory: str | Path) -> None:
    """Register a gettext domain whose catalogs live in
    `<directory>/<locale>/LC_MESSAGES/<domain>.mo`. Registering again replaces
    the path and clears the cache."""
    _DOMAINS[domain] = Path(directory)
    for key in [key for key in _CATALOGS if key[1] == domain]:
        del _CATALOGS[key]


def domain_directory(domain: str = DOMAIN) -> Path | None:
    return _DOMAINS.get(domain)


# -------------------------------------------------------------------- locales


def normalize(tag: str) -> str:
    """Normalize a locale tag to gettext's form: `ru-RU`, `ru_RU.UTF-8` -> `ru_RU`."""
    match = _TAG_RE.match((tag or "").strip())
    if not match:
        return ""
    language, script, region = match.groups()
    parts = [language.lower()]
    if script:
        parts.append(script.title())
    if region:
        parts.append(region.upper())
    return "_".join(parts)


def candidates(tag: str) -> list[str]:
    """A tag followed by its less specific fallbacks: `pt_BR` -> `pt_BR`, `pt`."""
    normalized = normalize(tag)
    if not normalized:
        return []
    parts = normalized.split("_")
    return ["_".join(parts[:count]) for count in range(len(parts), 0, -1)]


def available_locales(domain: str = DOMAIN) -> list[str]:
    """Locales with a catalog on disk, the source language first."""
    directory = _DOMAINS.get(domain)
    found: set[str] = set()
    if directory and directory.is_dir():
        for path in directory.glob(f"*/LC_MESSAGES/{domain}.mo"):
            found.add(path.parent.parent.name)
    found.discard(SOURCE_LOCALE)
    return [SOURCE_LOCALE, *sorted(found)]


def parse_accept_language(header: str) -> list[str]:
    """Tags of an `Accept-Language` header, best first (`*` is ignored)."""
    weighted: list[tuple[float, int, str]] = []
    for position, part in enumerate(header.split(",")):
        tag, _, params = part.strip().partition(";")
        tag = tag.strip()
        if not tag or tag == "*":
            continue
        quality = 1.0
        for param in params.split(";"):
            key, _, value = param.partition("=")
            if key.strip() == "q":
                try:
                    quality = float(value)
                except ValueError:
                    quality = 0.0
        if quality > 0:
            weighted.append((-quality, position, tag))
    return [tag for _, _, tag in sorted(weighted)]


def negotiate(
    requested: str | Iterable[str] | None,
    *,
    domain: str = DOMAIN,
    available: Iterable[str] | None = None,
) -> str:
    """The best available locale for a tag, a list of tags or an
    `Accept-Language` header. Falls back to the source language."""
    if requested is None:
        return SOURCE_LOCALE
    if isinstance(requested, str):
        tags = parse_accept_language(requested) if ("," in requested or ";" in requested) else [requested]
    else:
        tags = list(requested)

    offered = list(available) if available is not None else available_locales(domain)
    by_key = {normalize(tag).lower(): tag for tag in offered}
    for tag in tags:
        for candidate in candidates(tag):
            match = by_key.get(candidate.lower())
            if match is not None:
                return match
    return SOURCE_LOCALE


# ------------------------------------------------------------- current locale


def get_locale() -> str:
    """The current locale, or the source language if none is set."""
    return _current.get() or SOURCE_LOCALE


def set_locale(locale: str | None) -> Any:
    """Set the locale for the current context and return a reset token.
    `None` means the source language. Prefer `use_locale` for a block."""
    return _current.set(locale or None)


def reset_locale(token: Any) -> None:
    _current.reset(token)


@contextmanager
def use_locale(locale: str | None) -> Iterator[str]:
    """Use this locale for the duration of the block."""
    token = set_locale(locale)
    try:
        yield get_locale()
    finally:
        reset_locale(token)


# --------------------------------------------------------------------- lookup


def catalog(locale: str | None = None, domain: str = DOMAIN) -> _gettext.NullTranslations:
    """The compiled catalog for a locale, or a null catalog if there is none."""
    resolved = locale or get_locale()
    key = (resolved, domain)
    cached = _CATALOGS.get(key)
    if cached is not None:
        return cached

    directory = _DOMAINS.get(domain)
    found = _NULL
    if directory is not None and resolved != SOURCE_LOCALE:
        try:
            found = _gettext.translation(
                domain, localedir=str(directory), languages=candidates(resolved)
            )
        except OSError:
            found = _NULL
    _CATALOGS[key] = found
    return found


def gettext(message: str, /, locale: str | None = None, domain: str = DOMAIN, **params: Any) -> str:
    """Translate a message, then format it with the keyword arguments.

    Formatting happens after translation so that the whole message (with its
    placeholders) reaches the catalog. Use::

        _("{node}: stage '{stage}' is not allowed", node=node.id, stage=name)

    not an f-string. Literal braces are doubled, as in `str.format`.
    """
    text = catalog(locale, domain).gettext(message)
    return text.format(**params) if params else text


def ngettext(
    singular: str,
    plural: str,
    n: int,
    /,
    locale: str | None = None,
    domain: str = DOMAIN,
    **params: Any,
) -> str:
    """Plural form of a message for `n`; `n` is also available to the format."""
    text = catalog(locale, domain).ngettext(singular, plural, n)
    return text.format(n=n, **params)


#: The conventional short name.
_ = gettext


def translate(value: Any, locale: str | None = None, domain: str | None = None) -> Any:
    """Translate a spec value.

    A `{locale: text}` mapping is resolved for `locale`. A string is looked
    up in `domain`; with `domain=None` it is returned as written. Other
    values (numbers, booleans) are returned unchanged.
    """
    if isinstance(value, Mapping):
        return from_mapping(value, locale)
    if isinstance(value, str) and domain is not None:
        return gettext(value, locale=locale, domain=domain)
    return value


def translate_all(value: Any, domain: str | None = None) -> Any:
    """A spec value in every available language, as `{locale: text}`.

    Used when the reader's language is not known yet (an editor picks it
    later). A mapping is returned as is; a string with translations becomes
    a mapping; a string without any stays a string.
    """
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or domain is None:
        return value
    locales = available_locales(domain)
    if len(locales) < 2:
        return value
    every = {locale: gettext(value, locale=locale, domain=domain) for locale in locales}
    if len(set(every.values())) == 1:
        return value
    return every


def from_mapping(mapping: Mapping[str, Any], locale: str | None = None) -> Any:
    """Pick a value from `{locale: text}`: the requested locale, then the
    source language, then the first entry."""
    if not mapping:
        return ""
    keys = [str(key) for key in mapping]
    chosen = negotiate(locale or get_locale(), available=keys)
    for key in (chosen, *candidates(locale or get_locale()), SOURCE_LOCALE):
        if key in mapping:
            return mapping[key]
    lower = {str(key).lower(): key for key in mapping}
    for key in candidates(locale or get_locale()) + [SOURCE_LOCALE]:
        if key.lower() in lower:
            return mapping[lower[key.lower()]]
    return mapping[keys[0]]


__all__ = [
    "DOMAIN",
    "SOURCE_LOCALE",
    "available_locales",
    "candidates",
    "catalog",
    "domain_directory",
    "from_mapping",
    "get_locale",
    "gettext",
    "negotiate",
    "ngettext",
    "normalize",
    "parse_accept_language",
    "register_domain",
    "reset_locale",
    "set_locale",
    "translate",
    "translate_all",
    "use_locale",
    "_",
]
