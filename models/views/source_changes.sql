{{
    config(
        materialized='incremental',
        unique_key=['change', 'table_name', 'column_name', 'data_type']
      )
}}
with
    current_state as (
        select
            table_name,
            column_name,
            data_type,
            table_schema
        from information_schema.columns c
        where table_schema in (
            select distinct table_schema from {{ ref('source_snapshot') }}
        )
    ),

    previous_state as (
        select
            table_name,
            column_name,
            data_type,
            table_schema
        from {{ ref('stg_source_schema') }}
    ),

    type_changes as (
        select
            'type_changed' as change,
            current_state.column_name,
            current_state.table_name,
            current_state.data_type,
            previous_state.data_type as pre_data_type,
            {{ dbt_date.now() }} as detected_at
        from previous_state inner join current_state
            on (lower(current_state.table_name) = lower(previous_state.table_name))
            and (lower(current_state.column_name) = lower(previous_state.column_name))
            and (lower(current_state.table_schema) = lower(previous_state.table_schema))
        where previous_state.data_type is not null and lower(current_state.data_type) != lower(previous_state.data_type)
    ),

columns_added as (
    select
        'column_added' as change,
        current_state.column_name,
        current_state.table_name,
        current_state.data_type,
        null as pre_data_type,
        {{ dbt_date.now() }} as detected_at
    from current_state
    left join previous_state as ps
        on lower(current_state.table_name) = lower(ps.table_name)
        and lower(current_state.table_schema) = lower(ps.table_schema)
    left join previous_state
        on lower(current_state.table_name) = lower(previous_state.table_name)
        and lower(current_state.column_name) = lower(previous_state.column_name)
        and lower(current_state.table_schema) = lower(previous_state.table_schema)
    where previous_state.column_name is null
        and ps.table_name is not null
),

    columns_removed as (
        select
            'column_removed' as change,
            previous_state.column_name,
            previous_state.table_name,
            null as data_type,
            previous_state.data_type as pre_data_type,
            {{ dbt_date.now() }} as detected_at
        from previous_state
        left outer join current_state
            on previous_state.table_name = current_state.table_name
            and previous_state.column_name = current_state.column_name
        where current_state.column_name is null
        and previous_state.table_name is not null
    ),


    columns_removed_filter_deleted_tables as (
        select
            removed.change,
            removed.column_name,
            removed.table_name,
            removed.data_type,
            removed.pre_data_type,
            removed.detected_at
        from columns_removed as removed inner join current_state
            on (lower(removed.table_name) = lower(current_state.table_name))

    ),

    all_changes as (
        select
            change,
            table_name,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from type_changes
        union all
        select
            change,
            table_name,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from columns_removed_filter_deleted_tables
        union all
        select
            change,
            table_name,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from columns_added
    )

select distinct * from all_changes
