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

    _first_seen_model as (
    select
        cols.node_id, min(run_started_at) as first_detected from {{ ref('stg_column') }} as cols
        inner join _executions as excs on cols.command_invocation_id = excs.command_invocation_id group by cols.node_id
    ),


    _first_seen_column as (
        select
            cols.node_id, cols.column_name, min(run_started_at) as first_detected from {{ ref('stg_column') }} as cols
        inner join _executions as excs on cols.command_invocation_id = excs.command_invocation_id group by cols.node_id, cols.column_name
    ),

    _new_columns as (
    select fsc.node_id, fsc.column_name, fsc.first_detected from _first_seen_column fsc inner join _first_seen_model fsm on fsc.node_id = fsm.node_id
    where fsc.first_detected > fsm.first_detected
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
            new_cols.first_detected as detected_at
        from cur inner join _new_columns new_cols
            on (lower(cur.node_id) = lower(new_cols.node_id) and lower(cur.column_name) = lower(new_cols.column_name))
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

select distinct * from all_column_changes
