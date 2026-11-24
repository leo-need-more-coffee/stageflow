"""Unit: the registry that stages and node types are looked up in."""
import unittest

from stageflow.core.registry import Registry
from stageflow.exceptions import RegistryError


class RegistryTests(unittest.TestCase):
    def test_what_was_added_is_found(self):
        registry = Registry("stage")
        registry.add("A", 1)
        self.assertEqual(registry.get("A"), 1)
        self.assertIn("A", registry)
        self.assertNotIn("B", registry)

    def test_a_name_is_taken_once(self):
        registry = Registry("stage")
        registry.add("A", 1)
        with self.assertRaisesRegex(RegistryError, "stage 'A' already registered"):
            registry.add("A", 2)
        self.assertEqual(registry.get("A"), 1, "the first registration stays")

    def test_a_missing_name_is_an_error_naming_the_kind(self):
        cases = {
            "stage": "stage 'X' not found in registry",
            "node type": "node type 'X' not found in registry",
            "widget": "widget 'X' not found in registry",
        }
        for kind, message in cases.items():
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(RegistryError, message):
                    Registry(kind).get("X")

    def test_a_duplicate_names_the_kind_too(self):
        for kind in ("node type", "widget"):
            with self.subTest(kind=kind):
                registry = Registry(kind)
                registry.add("X", 1)
                with self.assertRaisesRegex(RegistryError, f"{kind} 'X' already registered"):
                    registry.add("X", 1)

    def test_iteration_length_and_a_copy(self):
        registry = Registry("stage")
        for name in ("a", "b", "c"):
            registry.add(name, name.upper())
        self.assertEqual(list(registry), ["a", "b", "c"])
        self.assertEqual(len(registry), 3)
        copy = registry.as_dict()
        copy["d"] = "D"
        self.assertNotIn("d", registry, "as_dict hands out a copy, not the registry")

    def test_the_error_is_not_a_key_error(self):
        # callers catch RegistryError; a KeyError leaking through would be
        # caught by nothing that is meant for it
        with self.assertRaises(RegistryError) as caught:
            Registry("stage").get("nope")
        self.assertIsNone(caught.exception.__cause__)
        self.assertTrue(caught.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
