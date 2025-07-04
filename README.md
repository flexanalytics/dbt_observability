# dbt Observability Package

This dbt package builds a data mart of tables that track dbt projects, run/build history, and a history of table and columns.

Based on [dbt_artifacts](https://github.com/brooklyn-data/dbt_artifacts), this project differs in a few key ways:
* Adds a `columns.sql` model to track model column details (column_name, data_type, meta, and more)
* Maintains a history of column metrics (rowcount, distinct, nulls, min, max, avg, sum, stdev) for anomaly detection and data catalogs
* Maintains a history of schema changes
* Computes model rowcount for anomaly detection and data catalogs
* Adds support for Redshift, Postgres, Oracle, MySQL, MariaDB and MS SQL Server
* Brings all resource types (models, seeds, exposures, tests, snapshots, sources) into one unified `all_executions.sql` model
* Cleans up redundant tables and views, minimizing duplicate logic and data
* Does not observe this package

Also similar to [dbt-data-reliability](https://github.com/elementary-data/dbt-data-reliability), `dbt-observability` differs in a few key ways:
* `dbt-observability` requires nearly zero configuration, with no model configurations required
* `dbt-observability` is meant less for testing and more for observing. We found the capabilities of `dbt-data-reliability` to be robust for a use case that needs very specific things, but in practice the constant failing of packages due to anomaly and schema issues was too much. There are ways to configure around this, but our use case has simpler needs.
* `dbt-observability` stores a history of table and column metrics and schema changes in one table, rather than individual tables for each, making analytic capabilities more robust
* `dbt-observability` supports more database adapters

The package currently supports BigQuery, Databricks, Spark, Snowflake, Redshift, Postgres, Oracle, MySQL, MariaDB and MS SQL Server adapters.

## Quickstart

1. Add this package to your `packages.yml`:
```
packages:
  - package: flexanalytics/dbt_observability
    version: 2.5.0
```

2. Run `dbt deps` to install the package

3. Add an on-run-end hook to your `dbt_project.yml`: `on-run-end: "{{ dbt_observability.upload_results(results) }}"`
(We recommend using the `"dbt_observability:environments": ["prod"]` variable in your project to control which environments this package runs in [currently defaults to `["prod"]`]). If your project will be imported as a package to another project that also runs observability, you should add the project name to the on-run-end hook, like so: `on-run-end: "{{ dbt_observability.upload_results(results, 'your_project_name') }}"` to avoid observability running multiple times in the same dbt invocation.
> [!NOTE]
> dbt Cloud defaults all targets to `default`, if you want observability to run on a dbt cloud job you will need to ensure the corresponding target is updated. [More information can be found here](https://docs.getdbt.com/docs/build/custom-target-names).
>
> The same functionality can be expected when developing in dbt core via the cli, you will need to ensure that dbt_observability's target matches the relevant target names in your profiles.yml.

1. If you are using [selectors](https://docs.getdbt.com/reference/node-selection/syntax), be sure to include the `dbt_observability` models in your dbt invocation step, for example:
`dbt build --select some_model dbt_observability`

1. Run your project!

> :construction_worker: Always run the dbt_observability models in every dbt invocation which uses the `upload_results` macro. This ensures that the source models always have the correct fields in case of an update.

## Configuration

The following configuration can be used to specify where the raw (sources) data is uploaded, and where the dbt models are created:

```yml

vars:
...
  "dbt_observability:tracking_enabled": true # optional, create observability base tables - default is true
  "dbt_observability:environments": ["prod"] # optional, default is ["prod"]
  "dbt_observability:projects": ["main_project", "installed_package"] # optional, which projects should observability monitor. default is the current project only
  "dbt_observability:path": "models/marts/" # optional, which paths should observability monitor. must be in the form of "dbt_observability:path": "path/subpath/" - default is `None`, will run on all paths in the project
  "dbt_observability:materialization": ["table","incremental"] # optional, which model materialization should observability run on. must be array of "table", "view", "incremental", "ephemeral" - default is ["table","incremental"]
  "dbt_observability:track_source_rowcounts": false # optional, track source rowcounts - default is false [depending on your dbms, this can be slow and resource intensive as it may require a full table scan if the dbms does not store rowcounts in information_schema.tables]
  "dbt_observability:rowcount_diff_threshold_pct": .05 # optional, set threshold for no_anomaly test (see below)
...

models:
  ...
  dbt_observability:
    +database: your_destination_database # optional, default is your target database
    +schema: your_destination_schema # optional, default is `observability`
    staging:
      +database: your_destination_database # optional, default is your target database
      +schema: your_destination_schema # optional, default is `observability`
    sources:
      +database: your_sources_database # optional, default is your target database
      +schema: your sources_database # optional, default is `observability`
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

### dbt Tests

Projects can use the `no_anomaly` test on source tables in their sources.yml. The test will check if the difference between the current source table rowcount and the average of all previous observed rowcounts is significant enough to raise an error/warning. Set the `dbt_observability:rowcount_diff_threshold_pct` variable (see above) to override the default threshold of .05 (i.e. a 5% difference in rowcount).

```yml
sources:
  - name: sourcename
    tables:
      - name: some_source_table
        tests:
          - dbt_observability.no_anomaly:
              config:
                severity: error
```

## Acknowledgements

This package is based on [dbt_artifacts](https://github.com/brooklyn-data/dbt_artifacts). Thanks to [Brooklyn Data Co.](https://brooklyndata.co/) for all the hard work put into the initial versions of this project. Thank you to Tails.com for initial development and maintenance of this package. On 2021/12/20, the repository was transferred from the Tails.com GitHub organization to Brooklyn Data Co. The macros in the early versions package were adapted from code shared by Kevin Chan and Jonathan Talmi of Snaptravel.

Thank you for sharing your work with the community!
