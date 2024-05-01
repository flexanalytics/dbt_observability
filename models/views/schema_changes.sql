with

    _executions as (
        select
            *,
            lag(run_started_at) over (partition by node_id order by run_started_at) as previous_run_started_at,
            lead(run_started_at) over (partition by node_id order by run_started_at) as next_run_started_at
        from {{ ref('stg_execution') }}
    ),

    cur as (
        select
            cols.*,
            excs.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_column') }} as cols
        inner join _executions as excs on cols.command_invocation_id = excs.command_invocation_id
    ),

    pre as (
        select
            cols.*,
            excs.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_column') }} as cols
        inner join _executions as excs on cols.command_invocation_id = excs.command_invocation_id
    ),

    type_changes as (
        select
            cur.node_id,
            'type_changed' as change,
            cur.column_name,
            cur.data_type,
            pre.data_type as pre_data_type,
            pre.run_started_at as detected_at
        from pre inner join cur
            on (lower(cur.node_id) = lower(pre.node_id) and lower(cur.column_name) = lower(pre.column_name))
                and pre.run_started_at = cur.previous_run_started_at
        where pre.data_type is not NULL and lower(cur.data_type) != lower(pre.data_type)
    ),

    columns_added as (
        select
            cur.node_id,
            'column_added' as change,
            cur.column_name,
            cur.data_type,
            NULL as pre_data_type,
            cur.run_started_at as detected_at
        from pre inner join cur
            on (lower(cur.node_id) = lower(pre.node_id) and lower(cur.column_name) = lower(pre.column_name))
                and pre.run_started_at = cur.previous_run_started_at
        where pre.node_id is NULL
    ),

    columns_removed as (
        select
            pre.node_id,
            'column_removed' as change,
            pre.column_name,
            NULL as data_type,
            pre.data_type as pre_data_type,
            pre.run_started_at as detected_at
        from pre left outer join cur
            on (lower(cur.node_id) = lower(pre.node_id) and lower(cur.column_name) = lower(pre.column_name))
                and pre.run_started_at = cur.previous_run_started_at
        where cur.node_id is not NULL and cur.column_name is NULL

    ),

    columns_removed_filter_deleted_tables as (
        select
            removed.node_id,
            removed.change,
            removed.column_name,
            removed.data_type,
            removed.pre_data_type,
            removed.detected_at
        from columns_removed as removed inner join cur
            on (lower(removed.node_id) = lower(cur.node_id))

    ),

    all_column_changes as (
        select * from type_changes
        union all
        select * from columns_removed_filter_deleted_tables
        union all
        select * from columns_added

    )

select * from all_column_changes
