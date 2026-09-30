"""Locale-aware text: the framework's own messages and what a stage says about
itself.

Two audiences, two mechanisms, one lookup.

The framework's own strings — validation errors, refusals, the descriptions of
the built-in stages — are written in English in the source and translated
through a `gettext` catalog shipped inside the package. No language is named
anywhere in the code: a locale exists here because a catalog for it exists on
disk, so adding one is adding a file, not editing a list.

A host's stages are not in that catalog and should not have to be. So a spec
string may be a per-locale mapping instead of a string, which needs no
catalog, no extraction and no build step::

    description:
      en: "Takes a ticket out of the queue"
      ru: "Берёт тикет из очереди"

The locale is not a global. It lives in a `ContextVar`, which is what makes it
usable in a server: a request handler sets the locale it was asked for, two
requests being answered at the same time get two languages, and a task
started from a handler inherits the handler's locale. A process that never
sets anything gets the source strings, which is why nothing has to be
configured for the framework to work in English.
"""

from __future__ import annotations

import gettext as _gettext
import re
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

#: the language the strings in the source are written in. A catalog for it
#: would be a catalog mapping every message to itself.
SOURCE_LOCALE = "en"

#: the framework's own catalog. A host with strings of its own registers a
#: second domain rather than adding to this one — see `register_domain`.
DOMAIN = "stageflow"

_DOMAINS: dict[str, Path] = {DOMAIN: Path(__file__).with_name("locale")}
_CATALOGS: dict[tuple[str, str], _gettext.NullTranslations] = {}
_NULL = _gettext.NullTranslations()

#: set per request, not per process — see the module docstring
_current: ContextVar[str | None] = ContextVar("stageflow_locale", default=None)

_TAG_RE = re.compile(r"^([A-Za-z]{2,8})(?:[-_]([A-Za-z]{4}))?(?:[-_]([A-Za-z]{2}|\d{3}))?")


# --------------------------------------------------------------- the domains


def register_domain(domain: str, directory: str | Path) -> None:
    """Point a gettext domain at the directory holding its catalogs.

    The layout is the usual one, `<directory>/<locale>/LC_MESSAGES/<domain>.mo`,
    so a host can keep its translations wherever it keeps them and hand the
    path over once at startup. Registering the same domain again replaces the
    path and drops what was cached from the old one.
    """
    _DOMAINS[domain] = Path(directory)
    for key in [key for key in _CATALOGS if key[1] == domain]:
        del _CATALOGS[key]


def domain_directory(domain: str = DOMAIN) -> Path | None:
    return _DOMAINS.get(domain)


# --------------------------------------------------------------- the locales


def normalize(tag: str) -> str:
    """A locale tag in the shape gettext looks for on disk: `ru`, `pt_BR`.

    Everything people actually send arrives here — `ru-RU`, `RU_ru`,
    `ru_RU.UTF-8`, `zh-Hant-TW` — and the difference between those spellings
    is not a difference anyone means.
    """
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
    """A tag and the less specific tags it falls back to: `pt_BR` → `pt`.

    A translation for the language is a better answer than no translation at
    all: someone asking for `pt_BR` reads `pt` without noticing.
    """
    normalized = normalize(tag)
    if not normalized:
        return []
    parts = normalized.split("_")
    return ["_".join(parts[:count]) for count in range(len(parts), 0, -1)]


def available_locales(domain: str = DOMAIN) -> list[str]:
    """Locales this build can answer in, the source language first.

    Read off the disk rather than declared, so a catalog dropped into the
    package is available without a release, and a catalog deleted stops being
    offered.
    """
    directory = _DOMAINS.get(domain)
    found: set[str] = set()
    if directory and directory.is_dir():
        for path in directory.glob(f"*/LC_MESSAGES/{domain}.mo"):
            found.add(path.parent.parent.name)
    found.discard(SOURCE_LOCALE)
    return [SOURCE_LOCALE, *sorted(found)]


def parse_accept_language(header: str) -> list[str]:
    """The tags of an `Accept-Language` header, best first.

    `*` is dropped: it means "anything", and the answer to that is the
    default, which is what an empty list already produces.
    """
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
    """The best locale on offer for what was asked for.

    Takes one tag, a list of tags, or a whole `Accept-Language` header —
    a backend passes along whatever it has without picking it apart first.
    Nothing matches, or nothing was asked for: the source language, because
    the alternative is refusing to answer over a preference.
    """
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


# ------------------------------------------------------- the current locale


def get_locale() -> str:
    """The locale in force here, or the source language if none was set."""
    return _current.get() or SOURCE_LOCALE


def set_locale(locale: str | None) -> Any:
    """Set the locale for this context; returns the token to reset with.

    `None` goes back to the source language. Prefer `use_locale` where the
    scope is a block — a token that is never reset leaks the locale into
    whatever runs next in the same task.
    """
    return _current.set(locale or None)


def reset_locale(token: Any) -> None:
    _current.reset(token)


@contextmanager
def use_locale(locale: str | None) -> Iterator[str]:
    """Answer in this locale for the duration of the block."""
    token = set_locale(locale)
    try:
        yield get_locale()
    finally:
        reset_locale(token)


# --------------------------------------------------------------- the lookup


def catalog(locale: str | None = None, domain: str = DOMAIN) -> _gettext.NullTranslations:
    """The compiled catalog for a locale, or a catalog that translates nothing.

    A missing catalog is not an error. A locale nobody has translated yet
    still has to produce text, and the source strings are that text.
    """
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
    """Translate a message, then fill in its parameters.

    The formatting happens here on purpose. The message has to reach the
    catalog whole — a message assembled from pieces cannot be translated into
    a language that orders those pieces differently — and an f-string is
    assembled before anything sees it, so the thing to write is::

        _("{node}: stage '{stage}' is not allowed", node=node.id, stage=name)

    A literal brace in a message is doubled, as `str.format` expects.
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
    """The plural form for `n` — two forms in English, up to four elsewhere.

    `n` is passed to the formatting as well, so a message can say it without
    being handed the same number twice.
    """
    text = catalog(locale, domain).ngettext(singular, plural, n)
    return text.format(n=n, **params)


#: the conventional short name, so a message reads as a message
_ = gettext


def translate(value: Any, locale: str | None = None, domain: str | None = None) -> Any:
    """A spec string: either a per-locale mapping or a catalog lookup.

    A mapping is a translation table written on the spot, and the language of
    its keys is negotiated exactly as an HTTP request's would be.

    A plain string is looked up in `domain` — and `domain=None`, the default,
    means there is none to look in, so the string is used as written. That is
    the right default for a host's stages: passing their text through the
    framework's catalog would be looking for someone else's strings in it.
    Anything else is returned untouched, because a spec holds numbers and
    booleans too.
    """
    if isinstance(value, Mapping):
        return from_mapping(value, locale)
    if isinstance(value, str) and domain is not None:
        return gettext(value, locale=locale, domain=domain)
    return value


def translate_all(value: Any, domain: str | None = None) -> Any:
    """A spec string in every language this build can answer in.

    The counterpart to `translate`, for a caller that is not answering one
    reader. An editor holds one copy of the specs and draws them for whoever is
    looking at it, possibly in a language chosen after the specs were fetched —
    so what it wants is the choice, not the result of somebody else having
    chosen. Hence `{locale: text}` back, and the picking happens where the
    reader is.

    A mapping written in a docstring already *is* that, and comes back as
    written. A string with a catalog behind it becomes one. A string with no
    catalog, or one nobody has translated, has nothing to choose and stays a
    string — a mapping of one entry would only make every reader of the spec
    handle a case that carries no information.
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
    """Pick a value out of `{locale: text}`.

    The source language is the fallback, and if even that is missing the
    first entry wins: a mapping written only in one language is still an
    answer, and an empty string would be a worse one.
    """
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
