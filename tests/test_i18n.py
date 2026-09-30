import asyncio
import pathlib
import tempfile
import unittest

from stageflow import (
    BaseStage,
    Pipeline,
    Policy,
    capabilities,
    get_stages,
    i18n,
    register_stage,
)

ROOT = pathlib.Path(__file__).resolve().parent.parent


@register_stage("MappedProseStage")
class MappedProseStage(BaseStage):
    """
    description:
      en: "Takes a ticket"
      ru: "Берёт тикет"
    arguments:
      ticket:
        type: string
        description:
          en: "Which one"
          ru: "Какой именно"
    outputs: {}
    """

    async def run(self):  # pragma: no cover - the spec is what is under test
        pass


# A host's stage translated in the host's own catalog: the prose is written
# plainly, the way a built-in's is, and `i18n_domain` is what says where to look
# it up — so the docstring needs no per-locale mapping.
@register_stage("HostCatalogStage")
class HostCatalogStage(BaseStage):
    """
    description: "Takes a ticket out of the queue"
    outputs:
      - name: ticket
        description: "The ticket taken"
    """

    i18n_domain = "host_catalog"

    async def run(self):  # pragma: no cover
        pass


@register_stage("OneLanguageStage")
class OneLanguageStage(BaseStage):
    """
    description: "Written once, in one language"
    outputs: {}
    """

    async def run(self):  # pragma: no cover
        pass


class TagTests(unittest.TestCase):
    def test_spellings_of_the_same_locale_normalize_together(self):
        for spelling in ("ru", "RU", "ru-RU", "ru_ru", "ru_RU.UTF-8"):
            self.assertTrue(i18n.normalize(spelling).lower().startswith("ru"), spelling)

    def test_a_script_and_a_region_survive_normalization(self):
        self.assertEqual(i18n.normalize("zh-hant-tw"), "zh_Hant_TW")

    def test_candidates_go_from_specific_to_general(self):
        self.assertEqual(i18n.candidates("pt-BR"), ["pt_BR", "pt"])

    def test_nonsense_is_not_a_locale(self):
        self.assertEqual(i18n.normalize("???"), "")
        self.assertEqual(i18n.candidates("???"), [])


class NegotiationTests(unittest.TestCase):
    def test_accept_language_is_ordered_by_quality(self):
        self.assertEqual(
            i18n.parse_accept_language("en;q=0.4,ru;q=0.9,de;q=0.8"),
            ["ru", "de", "en"],
        )

    def test_the_wildcard_is_not_a_preference(self):
        self.assertEqual(i18n.parse_accept_language("*"), [])

    def test_a_header_is_accepted_whole(self):
        self.assertEqual(
            i18n.negotiate("ru-RU,ru;q=0.9,en;q=0.8", available=["en", "ru"]), "ru"
        )

    def test_a_region_falls_back_to_its_language(self):
        self.assertEqual(i18n.negotiate("pt-BR", available=["en", "pt"]), "pt")

    def test_nothing_on_offer_means_the_source_language(self):
        self.assertEqual(i18n.negotiate("de", available=["en", "ru"]), i18n.SOURCE_LOCALE)
        self.assertEqual(i18n.negotiate(None), i18n.SOURCE_LOCALE)

    def test_the_shipped_catalogs_are_what_is_on_offer(self):
        offered = i18n.available_locales()
        self.assertEqual(offered[0], i18n.SOURCE_LOCALE)
        self.assertIn("ru", offered)
        self.assertEqual(capabilities()["locales"], offered)


class CurrentLocaleTests(unittest.TestCase):
    def test_nothing_set_is_the_source_language(self):
        self.assertEqual(i18n.get_locale(), i18n.SOURCE_LOCALE)

    def test_the_block_puts_it_back(self):
        with i18n.use_locale("ru"):
            self.assertEqual(i18n.get_locale(), "ru")
        self.assertEqual(i18n.get_locale(), i18n.SOURCE_LOCALE)

    def test_two_tasks_can_be_answered_in_two_languages(self):
        """The reason the locale is a ContextVar and not a global.

        Two requests are served at once and each gets its own answer; with a
        module-level variable whichever ran last would win for both.
        """

        async def answer(locale):
            with i18n.use_locale(locale):
                await asyncio.sleep(0)  # let the other one run in between
                return i18n.gettext("The pipeline has no entry")

        async def both():
            return await asyncio.gather(answer("ru"), answer("en"))

        russian, english = asyncio.run(both())
        self.assertEqual(english, "The pipeline has no entry")
        self.assertNotEqual(russian, english)


class MessageTests(unittest.TestCase):
    def test_a_validation_error_speaks_the_locale(self):
        graph = Pipeline.from_dict({"nodes": [{"id": "a", "type": "entry", "next": "nope"}]})
        with i18n.use_locale("ru"):
            with self.assertRaises(Exception) as caught:
                graph.validate()
            russian = str(caught.exception)
        with self.assertRaises(Exception) as caught:
            graph.validate()
        self.assertIn("not found in the graph", str(caught.exception))
        self.assertIn("в графе не найден", russian)
        # the node id is not translated, and it is the part that matters
        self.assertIn("nope", russian)

    def test_a_refusal_speaks_the_locale(self):
        graph = Pipeline.from_dict({"nodes": [
            {"id": "a", "type": "entry", "next": "s"},
            {"id": "s", "type": "stage", "stage": "SleepStage"},
        ]})
        with i18n.use_locale("ru"):
            with self.assertRaises(Exception) as caught:
                graph.validate(Policy(stages={"ConcatStage"}))
        self.assertIn("не разрешена политикой", str(caught.exception))

    def test_an_untranslated_locale_falls_back_to_the_source(self):
        with i18n.use_locale("de"):
            self.assertEqual(
                i18n.gettext("The pipeline has no entry"), "The pipeline has no entry"
            )

    def test_parameters_are_filled_after_the_lookup(self):
        with i18n.use_locale("ru"):
            text = i18n.gettext("Unknown type '{name}'", name="Ticket")
        self.assertIn("Ticket", text)
        self.assertNotIn("{name}", text)


class SpecTests(unittest.TestCase):
    def test_a_builtin_reads_in_the_locale_asked_for(self):
        stage = get_stages()["ConcatStage"]
        self.assertEqual(stage.get_specs("en")["description"],
                         "Concatenate stringified parts with separator")
        self.assertEqual(stage.get_specs("ru")["description"],
                         "Склеивает части через разделитель, приводя их к строкам")

    def test_a_builtin_translates_its_arguments_too(self):
        spec = get_stages()["PopListStage"].get_specs("ru")
        self.assertEqual(spec["arguments"][0]["description"],
                         "Список, из которого извлекаем")

    def test_a_host_stage_may_write_the_translations_in_the_docstring(self):
        self.assertEqual(MappedProseStage.get_specs("ru")["description"], "Берёт тикет")
        self.assertEqual(MappedProseStage.get_specs("en")["description"], "Takes a ticket")
        self.assertEqual(
            MappedProseStage.get_specs("ru")["arguments"][0]["description"],
            "Какой именно",
        )

    def test_a_mapping_falls_back_to_the_source_language(self):
        self.assertEqual(MappedProseStage.get_specs("de")["description"], "Takes a ticket")

    def test_a_host_string_is_not_looked_up_in_the_framework_catalog(self):
        """A stage with no `i18n_domain` reads as written, in every locale.

        Passing a host's prose through the framework's catalog would be looking
        for somebody else's strings in it; the answer for an untranslated stage
        is the text its author wrote.
        """
        for locale in ("en", "ru", "de"):
            self.assertEqual(OneLanguageStage.get_specs(locale)["description"],
                             "Written once, in one language")

    def test_no_locale_asked_for_means_every_locale_in_the_spec(self):
        """The spec is not built for a reader, so it does not choose for one.

        An editor fetches the specs once and its reader picks a language
        afterwards; a backend that had collapsed the prose to one language would
        have to be asked again on every change of mind.
        """
        description = get_stages()["ConcatStage"].get_specs()["description"]
        self.assertEqual(description, {
            "en": "Concatenate stringified parts with separator",
            "ru": "Склеивает части через разделитель, приводя их к строкам",
        })

    def test_the_locale_in_force_does_not_collapse_the_prose(self):
        """`use_locale` is for the framework's messages, not for a spec.

        The locale in force says which language THIS answer is in. A spec is not
        an answer to anybody, so it stays every language — otherwise a handler
        that had set a locale for its error messages would silently narrow the
        specs it serves alongside them.
        """
        with i18n.use_locale("ru"):
            self.assertEqual(get_stages()["ConcatStage"].get_specs()["description"],
                             {
                                 "en": "Concatenate stringified parts with separator",
                                 "ru": "Склеивает части через разделитель, "
                                       "приводя их к строкам",
                             })

    def test_a_locale_asked_for_still_collapses_to_it(self):
        self.assertEqual(get_stages()["ConcatStage"].get_specs("ru")["description"],
                         "Склеивает части через разделитель, приводя их к строкам")

    def test_prose_nobody_translated_stays_a_string(self):
        """A mapping of one entry would be a choice that carries no information."""
        self.assertEqual(OneLanguageStage.get_specs()["description"],
                         "Written once, in one language")

    def test_a_docstring_mapping_comes_back_as_written(self):
        """It is already every language: nothing to look up, nothing to collapse."""
        spec = MappedProseStage.get_specs()
        self.assertEqual(spec["description"], {"en": "Takes a ticket", "ru": "Берёт тикет"})
        self.assertEqual(spec["arguments"][0]["description"],
                         {"en": "Which one", "ru": "Какой именно"})

    def test_identifiers_are_never_translated(self):
        spec = get_stages()["ConcatStage"].get_specs("ru")
        self.assertEqual(spec["stage_name"], "ConcatStage")
        self.assertEqual(spec["category"], "builtin.strings")
        self.assertEqual([f["name"] for f in spec["arguments"]], ["parts", "separator"])


class DomainTests(unittest.TestCase):
    def test_an_unregistered_domain_translates_nothing(self):
        self.assertEqual(i18n.gettext("The pipeline has no entry",
                                      locale="ru", domain="nobody"),
                         "The pipeline has no entry")

    def test_a_host_can_register_its_own(self):
        i18n.register_domain("host_under_test", ROOT / "tests")
        try:
            self.assertEqual(i18n.domain_directory("host_under_test"), ROOT / "tests")
            # no catalog there, so it still reads as written — what matters is
            # that the framework's own domain was not consulted for it
            self.assertEqual(i18n.available_locales("host_under_test"), ["en"])
        finally:
            i18n._DOMAINS.pop("host_under_test", None)

    def test_a_host_catalog_translates_a_stage_of_its_own(self):
        """The whole extension point, end to end, with a real compiled catalog.

        The test above proves the wiring; this one proves the result. Without it
        `i18n_domain` is documented behaviour that nothing exercises: the two
        tests around it use a domain with no catalog in it, so a lookup that
        quietly returned the source string would pass them both.
        """
        try:
            from babel.messages import mofile
            from babel.messages.catalog import Catalog
        except ImportError:  # pragma: no cover - babel is a dev extra
            raise unittest.SkipTest("babel is not installed")

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            messages = root / "ru" / "LC_MESSAGES"
            messages.mkdir(parents=True)
            catalog = Catalog(locale="ru", domain="host_catalog")
            catalog.add("Takes a ticket out of the queue", "Берёт тикет из очереди")
            catalog.add("The ticket taken", "Взятый тикет")
            with (messages / "host_catalog.mo").open("wb") as handle:
                mofile.write_mo(handle, catalog)

            i18n.register_domain("host_catalog", root)
            try:
                self.assertEqual(i18n.available_locales("host_catalog"), ["en", "ru"])

                spec = HostCatalogStage.get_specs(locale="ru")
                self.assertEqual(spec["description"], "Берёт тикет из очереди")
                self.assertEqual(spec["outputs"][0]["description"], "Взятый тикет")

                # the source language, and a language the host has no catalog
                # for, both read as written rather than as an error
                for locale in ("en", "de"):
                    spec = HostCatalogStage.get_specs(locale=locale)
                    self.assertEqual(spec["description"],
                                     "Takes a ticket out of the queue")

                # the host's domain is its own: the framework's catalog has a
                # Russian string for this one, and it must not leak in
                self.assertEqual(
                    i18n.gettext("The pipeline has no entry", locale="ru",
                                 domain="host_catalog"),
                    "The pipeline has no entry",
                )
            finally:
                i18n._DOMAINS.pop("host_catalog", None)
                i18n._CATALOGS.clear()


class CatalogTests(unittest.TestCase):
    """The compiled catalog is what ships, so it has to match the source one."""

    @staticmethod
    def _babel():
        try:
            from babel.messages import mofile, pofile
        except ImportError:  # pragma: no cover - babel is a dev extra
            raise unittest.SkipTest("babel is not installed")
        return mofile, pofile

    def _catalogs(self, locale):
        mofile, pofile = self._babel()
        directory = ROOT / "stageflow" / "locale" / locale / "LC_MESSAGES"
        with (directory / "stageflow.po").open("rb") as handle:
            source = pofile.read_po(handle, locale=locale)
        with (directory / "stageflow.mo").open("rb") as handle:
            compiled = mofile.read_mo(handle)
        return source, compiled

    def test_the_compiled_catalog_is_not_stale(self):
        for locale in i18n.available_locales():
            if locale == i18n.SOURCE_LOCALE:
                continue
            source, compiled = self._catalogs(locale)
            translated = {m.id: m.string for m in source
                          if m.id and m.string and not m.fuzzy}
            shipped = {m.id: m.string for m in compiled if m.id}
            self.assertEqual(translated, shipped,
                             f"{locale}: run tools/i18n.py compile")

    def test_every_message_is_translated(self):
        for locale in i18n.available_locales():
            if locale == i18n.SOURCE_LOCALE:
                continue
            source, _compiled = self._catalogs(locale)
            missing = sorted(m.id for m in source if m.id and not m.string)
            self.assertEqual(missing, [], f"{locale}: untranslated")

    def test_the_placeholders_survive_every_translation(self):
        """A placeholder lost in translation is a `KeyError` at the worst moment.

        The message is formatted after the lookup, so a msgstr that renamed or
        dropped `{node}` would raise while something is already going wrong —
        and only in that language.
        """
        import re

        braces = re.compile(r"\{[^{}]*\}")
        for locale in i18n.available_locales():
            if locale == i18n.SOURCE_LOCALE:
                continue
            source, _compiled = self._catalogs(locale)
            for message in source:
                if not message.id or not message.string:
                    continue
                self.assertEqual(
                    sorted(braces.findall(message.id)),
                    sorted(braces.findall(message.string)),
                    f"{locale}: {message.id}",
                )


if __name__ == "__main__":
    unittest.main()
