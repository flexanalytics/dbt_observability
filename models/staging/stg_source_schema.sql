select
    table_name,
    column_name,
    data_type,
    table_schema
from {{ ref('source_snapshot') }}
