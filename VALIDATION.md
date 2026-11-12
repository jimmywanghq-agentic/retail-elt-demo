# Current validation status

The source is built and committed, but the complete live demo has not yet been verified.

## Passed

- The workstation can reach the supplied Snowflake account hostname over port 443.
- A read-only Snowsight query returned `CONNECTION_OK = 1` for account locator `TJ90853`.
- Python source files parse successfully.
- Docker Compose configuration validation succeeded before the engine became unavailable.
- Git exclusions protect the local `.env`, generated private key, and generated setup SQL.

## Still required before claiming the demo works end to end

1. Free enough space on C: for Docker's image build and runtime data. The first build stopped with only about 50 MB free; Docker then stopped responding. A practical next step is to free at least 10 GB before retrying.
2. Complete and confirm the one-time Snowflake setup that registers the restricted service account's public key.
3. Finish the Docker image build, test the service-account connection, and start the services.
4. Run `python scripts/smoke_test.py` against the running UI. It checks a real insert, retry deduplication, and rejected invalid inputs.
5. Follow one order ID through a scheduled Airflow run, RAW, and the dbt fact table. Check the dbt tests pass and a subsequent run does not duplicate facts.
6. Open the supplied Power BI project, complete data-source authentication, enable one-minute page refresh, and verify a newly placed order appears after the next batch and report refresh.

The Power BI files are generated source definitions. They have not yet been opened and validated in Desktop. A Power BI Desktop window was present, but computer automation could not activate or capture its contents reliably.

The first image build exposed an Airflow/dbt dependency conflict. The Dockerfile now installs dbt in its own virtual environment. The retry got through the main requirements installation, then stopped when Docker lost its connection; the revised full image is not yet verified.

The working source repository is `https://github.com/jwangdbe/retail-elt-demo`. An earlier browser-created repository under a different signed-in account contains only its initial README. This source repository uses the workstation's authenticated `jwangdbe` Git identity so changes can be pushed normally.
