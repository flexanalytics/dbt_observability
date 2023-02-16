# dbt Observability Package
This package builds a data mart of tables that track dbt projects.

The package currently supports Redshift, BigQuery, Databricks, Spark, Snowflake, Postgres, and MS SQL Server adapters.

## Quickstart

1. Add this package to your `packages.yml`:
```
packages:
  - package: flexanalytics/dbt_observability
    version: 1.0.0
```

2. Run `dbt deps` to install the package

3. Add an on-run-end hook to your `dbt_project.yml`: `on-run-end: "{{ dbt_observability.upload_results(results) }}"`
(We recommend adding a conditional here so that the upload only occurs in your production environment, such as `on-run-end: "{% if target.name == 'prod' %}{{ dbt_observability.upload_results(results) }}{% endif %}"`)

4. If you are using [selectors](https://docs.getdbt.com/reference/node-selection/syntax), be sure to include the `dbt_observability` models in your dbt invocation step.

5. Run your project!

> :construction_worker: Always run the dbt_observability models in every dbt invocation which uses the `upload_results` macro. This ensures that the source models always have the correct fields in case of an update.

## Configuration

The following configuration can be used to specify where the raw (sources) data is uploaded, and where the dbt models are created:

```yml
models:
  ...
  dbt_observability:
    +database: your_destination_database # optional, default is your target database
    +schema: your_destination_schema # optional, default is your target schema
    staging:
      +database: your_destination_database # optional, default is your target database
      +schema: your_destination_schema # optional, default is your target schema
    sources:
      +database: your_sources_database # optional, default is your target database
      +schema: your sources_database # optional, default is your target schema
```

Note that model materializations and `on_schema_change` configs are defined in this package's `dbt_project.yml`, so do not set them globally in your `dbt_project.yml` ([see docs on configuring packages](https://docs.getdbt.com/docs/building-a-dbt-project/package-management#configuring-packages)):

> Configurations made in your dbt_project.yml file will override any configurations in a package (either in the dbt_project.yml file of the package, or in config blocks).

### Environment Variables

If the project is running in dbt Cloud, the following five columns (https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/cloud-environment-variables#special-environment-variables) will be automatically populated in the invocations model:
- dbt_cloud_project_id
- dbt_cloud_job_id
- dbt_cloud_run_id
- dbt_cloud_run_reason_category
- dbt_cloud_run_reason

To capture other environment variables in the invocations model in the `env_vars` column, add them to the `env_vars` variable in your `dbt_project.yml`. Note that environment variables with secrets (`DBT_ENV_SECRET_`) can't be logged.
```yml
vars:
  env_vars: [
    'ENV_VAR_1',
    'ENV_VAR_2',
    '...'
  ]
```

### dbt Variables

To capture dbt variables in the invocations model in the `dbt_vars` column, add them to the `dbt_vars` variable in your `dbt_project.yml`.
```yml
vars:
  dbt_vars: [
    'var_1',
    'var_2',
    '...'
  ]
```

## Acknowledgements
This package is based on [dbt_artifacts](https://github.com/brooklyn-data/dbt_artifacts). Thanks to [Brooklyn Data Co.](https://brooklyndata.co/) for all the hard work put into the initial versions of this project.
