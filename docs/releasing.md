# Releasing

Pushing a tag builds the package and publishes it to PyPI
(`.github/workflows/publish.yml`):

```bash
# bump project.version in pyproject.toml first — the workflow refuses
# a tag that does not match it
git tag -a 1.0.0 -m "StageFlow 1.0.0"
git push origin 1.0.0
```

The workflow runs the test suite, builds an sdist and a wheel, validates both
with `twine check`, and uploads them through PyPI Trusted Publishing, so no
API token is stored in the repository. The upload step runs in the `pypi`
environment, which can be configured to require a manual approval.
