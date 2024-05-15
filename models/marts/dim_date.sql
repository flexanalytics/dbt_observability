with
    dt as (
        select * from {{ ref('stg_date') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
    date_day as date_full,
    case
        when month_of_year >= 7
            then year_number + 1
        else year_number
    end as fiscal_year,
    case
        when month_of_year >= 7
            then quarter_of_year - 2
        else quarter_of_year + 2
    end as fiscal_quarter,
    case
        when month_of_year >= 7
            then month_of_year - 6
        else month_of_year + 6
    end as fiscal_month,
	{{ concat(["cast(case
			when month_of_year >= 7
			then month_of_year - 6
			else month_of_year + 6
		end as varchar(2))",
		"cast(' - ' as varchar(3))",
		"cast(month_name as varchar(20))"])
	}} as fiscal_month_name,
	{{ concat(["cast(case
			when month_of_year >= 7
			then month_of_year - 6
			else month_of_year + 6
		end as varchar(2))",
		"cast(' - ' as varchar(3))",
		"cast(month_name_short as varchar(20))"])
	}} as fiscal_month_abbrev,
    year_number as calendar_year,
    quarter_of_year as calendar_quarter,
    month_of_year as calendar_month_nbr,
    month_name as calendar_month_name,
    month_name_short as calendar_month_abbrev,
    week_of_year as calendar_week_of_year,
    cast(day_of_year as int) as calendar_day_of_year,
    cast(day_of_month as int) as day_of_month,
    day_of_week,
    day_of_week_name_short as day_of_week_abbrev,
    right(cast(date_day as varchar(10)), 5) as month_day_num,
    {{ concat(["month_name_short", "' '", "day_of_month"]) }} as month_day_desc
from dt
