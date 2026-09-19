import pathlib
import tomllib
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


class RequirementsTests(unittest.TestCase):
    @staticmethod
    def _requirements():
        text = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        return [line.strip() for line in text.splitlines() if line.strip()]

    @staticmethod
    def _pyproject():
        with open(ROOT / "pyproject.toml", "rb") as fh:
            return tomllib.load(fh)

    def test_requirements_match_pyproject(self):
        self.assertEqual(self._requirements(),
                         self._pyproject()["project"]["dependencies"])

    def test_declared_python_floor_is_supported(self):
        floor = self._pyproject()["project"]["requires-python"]
        self.assertEqual(floor, ">=3.11")


if __name__ == "__main__":
    unittest.main()
