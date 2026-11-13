"""Validate our Power BI JSON against Microsoft's published format schemas.

Run inside the dbt environment, which already includes jsonschema:
docker compose exec airflow /home/airflow/dbt-venv/bin/python scripts/validate_powerbi.py
"""
import json
from functools import lru_cache
from pathlib import Path
from urllib.request import urlopen
from urllib.error import HTTPError

from jsonschema import Draft7Validator
from referencing import Registry, Resource

@lru_cache(maxsize=None)
def fetch_schema(url):
    # Schemas are public documentation, not application data or credentials.
    try:
        with urlopen(url, timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        # Some Microsoft schema IDs use a dot, but their published filename uses a hyphen.
        if error.code == 404 and 'schema.embedded.json' in url:
            with urlopen(url.replace('schema.embedded.json', 'schema-embedded.json'), timeout=30) as response:
                return json.load(response)
        raise

root = Path(__file__).resolve().parents[1] / 'powerbi'
registry = Registry(retrieve=lambda url: Resource.from_contents(fetch_schema(url)))
failures = []
checked = 0
for file in sorted(root.rglob('*')):
    if not file.is_file() or '.pbi' in file.parts:
        continue
    if file.suffix not in ('.json', '.pbip', '.pbir', '.pbism'):
        continue
    document = json.loads(file.read_text(encoding='utf-8-sig'))
    url = document.get('$schema')
    if not url:
        continue
    schema = fetch_schema(url)
    # Register the root so relative references remain tied to the correct file.
    document_registry = registry.with_resource(url, Resource.from_contents(schema))
    for error in Draft7Validator(schema, registry=document_registry).iter_errors(document):
        failures.append(f'{file.relative_to(root)}: {list(error.path)}: {error.message}')
    checked += 1
for failure in failures:
    print(failure)
assert not failures, f'{len(failures)} Power BI schema errors found.'
print(f'PASS: {checked} Power BI project/definition files satisfy their public schemas.')
