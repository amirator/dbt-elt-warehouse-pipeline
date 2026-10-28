-- No gaps in the yearly series between first and last loaded year
with years as (select distinct landing_year from {{ ref('fct_landings') }}),
bounds as (select min(landing_year) as lo, max(landing_year) as hi, count(*) as n from years)
select * from bounds where hi - lo + 1 <> n
