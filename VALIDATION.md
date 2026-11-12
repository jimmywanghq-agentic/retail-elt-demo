# Current validation status

Validated on November 12, 2026. The complete PostgreSQL-to-Snowflake backend works. Power BI Desktop opened the supplied project and reached its Snowflake authentication dialog; report authentication and automatic page refresh remain to be completed by the user and verified.

## Passed against the real running services

- The revised Docker image built successfully, including Spark/Java and dbt's separate virtual environment.
- The one-time approved Snowflake setup completed with `Demo resources ready`.
- A key-pair connection from both Windows and the running container returned account `TJ90853`, user `ELT_SERVICE`, role `ELT_ROLE`, and warehouse `ELT_WH`.
- PostgreSQL, the order UI, and Airflow passed their runtime health checks. Airflow's scheduler heartbeat and metadata database were healthy.
- The order API saved a real synthetic purchase, returned the same transaction ID on a repeated request, and rejected an unknown product and zero quantity.
- A purchase placed through the browser UI reached Snowflake on a scheduled Airflow run.
- The UI simulator generated purchases every five seconds and stopped when requested.
- Scheduled Airflow runs completed the Spark load and dbt transformation successfully. One-minute cron scheduling is active with one concurrent run allowed.
- All three dbt models succeeded and all six data tests passed, including uniqueness, required fields, money calculations, and UTC transformation timing.
- The exact reconciliation below passed after the simulator stopped and the delivery queue drained.

| Check | Observed result |
|---|---:|
| PostgreSQL purchases | 40 |
| PostgreSQL undelivered purchases | 0 |
| Snowflake RAW rows / distinct IDs | 40 / 40 |
| dbt fact rows / distinct IDs | 40 / 40 |
| PostgreSQL net revenue | $447.33 |
| Snowflake fact net revenue | $447.33 |
| Invalid UTC transformation timestamps | 0 |

Order #1 (two coffees with a 10% discount) produced the expected $8.10 net revenue. Order #3 was submitted directly through the browser UI and reached the reporting facts. Gaps in transaction IDs are normal: PostgreSQL sequences advance on attempted inserts, including retried requests, without creating duplicate purchase rows.

## Remaining Power BI verification

1. Complete the Snowflake authentication dialog in the already-open Power BI project. Credentials are entered by the user and kept out of Git.
2. Confirm the supplied visuals display the Snowflake facts.
3. Enable one-minute automatic page refresh on the active DirectQuery report page and save the project.
4. Submit another order in the separate UI, then verify its ID and changed metrics appear after a scheduled batch and automatic report refresh.

Until those steps pass, do not describe the complete dashboard path as verified. The simulator is stopped after reconciliation; the UI and scheduled pipeline remain running.

## Fixes confirmed during runtime testing

- dbt has a separate Python environment to avoid conflicting with Airflow's dependencies.
- The order UI launches through bash so Airflow's base-image entrypoint does not treat `uvicorn` as an Airflow subcommand.
- Startup health checks distinguish a created container from a ready service.
- The dbt transformation timestamp explicitly converts to UTC instead of inheriting the Snowflake account timezone. Existing facts were rebuilt and the new timing test passed.
- The dbt profile uses the adapter's supported `query_tag` field.

Git exclusions protect `.env`, generated private keys, setup SQL with the generated public key, dbt build outputs, and Power BI's local caches. The working source repository is `https://github.com/jwangdbe/retail-elt-demo`.
