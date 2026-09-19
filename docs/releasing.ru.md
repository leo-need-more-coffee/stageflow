# Релизы

Пуш тега собирает пакет и публикует его на PyPI
(`.github/workflows/publish.yml`):

```bash
# сперва поднять project.version в pyproject.toml — воркфлоу отвергнет
# тег, который с ней не совпадает
git tag -a 1.0.0 -m "StageFlow 1.0.0"
git push origin 1.0.0
```

Воркфлоу прогоняет тесты, собирает sdist и wheel, проверяет оба через
`twine check` и загружает их по PyPI Trusted Publishing — токен в секретах
репозитория не хранится. Шаг загрузки идёт в окружении `pypi`, которому
можно включить ручное подтверждение.
