{% test no_anomaly(model) %}

with
    hist as (
        select avg(total_rowcount) as average_rowcount
         from {{ database }}.{{ target.schema }}_observability.sources  {# we can't use a ref here, see https://github.com/flexanalytics/dbt_observability/issues/32 #}
        where {{ dbt.concat(['\'"\'', "database_name", '\'"."\'' , "source_name", '\'"."\'', "identifier", '\'"\'']) }} = '{{ model }}'
            and coalesce(total_rowcount, 0) > 0
    ),

    curr as (
        select count(*) as current_rowcount
        from {{ model }}
    )

select
    curr.current_rowcount,
    hist.average_rowcount,
    curr.current_rowcount - coalesce(hist.average_rowcount, 0) as rowcount_diff,
    (cast(curr.current_rowcount as numeric(19, 6)) - coalesce(hist.average_rowcount, 0)) / nullif(cast(hist.average_rowcount as numeric(19, 6)),
    0) as rowcount_diff_pct
from curr
left outer join hist
    on 1 = 1
where rowcount_diff_pct
    < {{ var('dbt_observability:rowcount_diff_threshold_pct', '-.05') }}

{% endtest %}
