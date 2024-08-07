{% test no_anomoly(model) %}

with
    hist as (
        select avg(total_rowcount) as average_rowcount
        from {{ ref('sources') }} -- {{ database }}.{{ target.schema }}_observability.sources
        where ('"' || database_name || '"' || '.'
            '"' || source_name || '"' || '.'
            '"' || identifier || '"') = '{{ model }}'
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
    cast(rowcount_diff as numeric(19, 6)) / cast(curr.current_rowcount as numeric(19, 6)) as rowcount_diff_pct
from curr
left outer join hist
    on 1 = 1
where rowcount_diff_pct > .05 -- TODO: pass as parameter
    or rowcount_diff_pct < -.05

{% endtest %}
