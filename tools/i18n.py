#!/usr/bin/env python
"""The translation catalogs: extract, update, compile.

    python tools/i18n.py extract           # sources -> locale/stageflow.pot
    python tools/i18n.py update            # pot -> every locale's .po
    python tools/i18n.py update --locale ru # or just one, creating it if new
    python tools/i18n.py compile           # .po -> the .mo that ships
    python tools/i18n.py stats             # how much of each is done

Extraction reads two places, because the framework's translatable text lives in
two. Most of it is `_("…")` in the code and comes out of the source. The rest is
the prose of the built-in stages, which is written in a YAML docstring — no
extractor parses that, so the stages are imported and asked, which has the
happy side effect of extracting exactly what `get_specs()` would publish.

`babel` does the .po and .mo work; it is a dev dependency (`pip install -e
'.[i18n]'`), never needed to run StageFlow.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PACKAGE = ROOT / "stageflow"
LOCALE_DIR = PACKAGE / "locale"
DOMAIN = "stageflow"
POT = LOCALE_DIR / f"{DOMAIN}.pot"

#: `_` and its aliases, plus the plural form's two arguments
KEYWORDS = {"_": None, "gettext": None, "ngettext": (1, 2)}


def _babel():
    try:
        from babel.messages import extract, mofile, pofile
        from babel.messages.catalog import Catalog
    except ImportError:  # pragma: no cover - a dev-only path
        raise SystemExit(
            "babel is needed for the catalogs: pip install -e '.[i18n]'"
        ) from None
    return extract, mofile, pofile, Catalog


# ------------------------------------------------------------------ extract


def _spec_strings() -> list[tuple[str, str]]:
    """The prose of the built-in stages, as (message, where).

    Asked of the registry rather than parsed out of the docstrings: the spec is
    what an editor is shown, so extracting the spec extracts the strings that
    are actually published — and a string a stage stops publishing stops being
    extracted, instead of lingering in the catalog for nobody.

    A per-locale mapping is skipped: it carries its own translations, and there
    is nothing for a catalog to add.
    """
    import stageflow  # noqa: F401 - importing is what registers the stages
    from stageflow.core.spec import parse_docstring_spec, parse_fields
    from stageflow.core.stage import get_stages
    from stageflow.i18n import DOMAIN as OURS

    found: list[tuple[str, str]] = []

    def add(value: object, where: str) -> None:
        if isinstance(value, str) and value.strip():
            found.append((value, where))

    for name, cls in sorted(get_stages().items()):
        if getattr(cls, "i18n_domain", None) != OURS:
            continue  # somebody else's stage, somebody else's catalog
        doc = parse_docstring_spec(cls.__doc__)
        add(doc.get("description"), f"{name}.description")
        for section in ("arguments", "outputs"):
            for field in parse_fields(doc.get(section)):
                add(field.description, f"{name}.{section}.{field.name}")
        for spec in list(cls.allowed_events) + list(cls.allowed_inputs):
            add(getattr(spec, "description", None), f"{name}.{spec.type}")
    return found


def extract_messages() -> Path:
    extract, _mofile, pofile, Catalog = _babel()

    catalog = Catalog(
        project="StageFlow",
        version=_version(),
        msgid_bugs_address="https://github.com/leo-need-more-coffee/stageflow/issues",
        charset="utf-8",
        creation_date=datetime.now(timezone.utc),
    )

    for filename, lineno, message, comments, context in extract.extract_from_dir(
        str(PACKAGE),
        method_map=[("**.py", "python")],
        options_map={"**.py": {}},
        keywords=KEYWORDS,
        comment_tags=("i18n:",),
    ):
        path = Path(filename).as_posix()
        if path.startswith("locale/"):
            continue
        catalog.add(message, None, [(f"stageflow/{path}", lineno)],
                    auto_comments=comments, context=context)

    for message, where in _spec_strings():
        catalog.add(message, None, [(f"stageflow/builtins ({where})", 0)],
                    auto_comments=["the spec of a built-in stage"])

    LOCALE_DIR.mkdir(parents=True, exist_ok=True)
    with POT.open("wb") as handle:
        pofile.write_po(handle, catalog, width=88)
    print(f"{POT.relative_to(ROOT)}: {len(catalog)} messages")
    return POT


def _version() -> str:
    text = (ROOT / "pyproject.toml").read_text()
    for line in text.splitlines():
        if line.startswith("version"):
            return line.split("=", 1)[1].strip().strip('"')
    return "0"


# ------------------------------------------------------------------- update


def po_path(locale: str) -> Path:
    return LOCALE_DIR / locale / "LC_MESSAGES" / f"{DOMAIN}.po"


def known_locales() -> list[str]:
    return sorted(p.name for p in LOCALE_DIR.iterdir()
                  if p.is_dir() and po_path(p.name).exists()) if LOCALE_DIR.is_dir() else []


def update(locales: list[str]) -> None:
    _extract, _mofile, pofile, Catalog = _babel()
    if not POT.exists():
        extract_messages()
    with POT.open("rb") as handle:
        template = pofile.read_po(handle)

    for locale in locales:
        path = po_path(locale)
        if path.exists():
            with path.open("rb") as handle:
                catalog = pofile.read_po(handle, locale=locale, domain=DOMAIN)
            catalog.update(template, no_fuzzy_matching=False)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            catalog = Catalog(locale=locale, domain=DOMAIN, charset="utf-8")
            catalog.update(template)
        with path.open("wb") as handle:
            pofile.write_po(handle, catalog, width=88)
        print(f"{path.relative_to(ROOT)}: {_done(catalog)}")


# ------------------------------------------------------------------ compile


def compile_catalogs(locales: list[str]) -> None:
    _extract, mofile, pofile, _Catalog = _babel()
    for locale in locales:
        path = po_path(locale)
        if not path.exists():
            print(f"{locale}: no .po, skipped")
            continue
        with path.open("rb") as handle:
            catalog = pofile.read_po(handle, locale=locale, domain=DOMAIN)
        target = path.with_suffix(".mo")
        with target.open("wb") as handle:
            mofile.write_mo(handle, catalog, use_fuzzy=False)
        print(f"{target.relative_to(ROOT)}: {_done(catalog)}")


def _done(catalog) -> str:
    total = len([m for m in catalog if m.id])
    translated = len([m for m in catalog if m.id and m.string and not m.fuzzy])
    fuzzy = len([m for m in catalog if m.id and m.fuzzy])
    share = f"{translated * 100 // total}%" if total else "—"
    tail = f", {fuzzy} fuzzy" if fuzzy else ""
    return f"{translated}/{total} translated ({share}){tail}"


def stats(locales: list[str]) -> None:
    _extract, _mofile, pofile, _Catalog = _babel()
    for locale in locales:
        path = po_path(locale)
        with path.open("rb") as handle:
            catalog = pofile.read_po(handle, locale=locale, domain=DOMAIN)
        print(f"{locale}: {_done(catalog)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("extract", "update", "compile", "stats"))
    parser.add_argument("--locale", action="append", dest="locales",
                        help="one locale; repeatable. Default: every locale on disk")
    args = parser.parse_args()

    locales = args.locales or known_locales()
    if args.command == "extract":
        extract_messages()
    elif not locales:
        raise SystemExit("no locales: pass --locale ru to start one")
    elif args.command == "update":
        update(locales)
    elif args.command == "compile":
        compile_catalogs(locales)
    else:
        stats(locales)


if __name__ == "__main__":
    main()
