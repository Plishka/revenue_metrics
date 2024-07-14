--WITH test AS ( -- test final table by aggregating data
-- REVENUE METRICS
with main as (
	select 
		date(date_trunc('month', payment_date)) as payment_month,
		user_id,
		sum(revenue_amount_usd) as user_revenue -- monthly revenue per user
	from 
		project.games_payments
	group by 1, 2
	order by 1, 2
),
lt_ltv as ( -- CUSTOMER LT and LTV
	with a as(	
		select 
--			min(payment_date) over(partition by user_id) as first_pay, -- for CLT calculation
--			max(payment_date) over(partition by user_id) as last_pay, -- for CLT calculation
			distinct user_id,
			max(payment_date) over(partition by user_id)-min(payment_date) over(partition by user_id)+1 as lifetime,
			sum(revenue_amount_usd) over(partition by user_id) as lifetime_value
		from 
			project.games_payments
		order by 1
			)
			select 
				distinct user_id as user_id,
				lifetime as customer_lifetime,
				lifetime_value as customer_lifetime_value,
				gpu.game_name,
				gpu.language,
				gpu.has_older_device_model,
				gpu.age,
				case 
					when gpu.age <18 then '14-17'
					when gpu.age >=18 and gpu.age <25 then '18-24'
					when gpu.age >=25 and gpu.age <31 then '25-30'
					when gpu.age >30 and gpu.age <36 then '31-35'
					when gpu.age >35 and gpu.age <41 then '36-40'
					when gpu.age >40 and gpu.age <46 then '41-45'
					end as age_groups	
			from a
			left join project.games_paid_users gpu using(user_id)
),
revenue_months as (
	select 
		date(main.payment_month - interval '1 month') as previous_calendar_month, -- previous calendar months
		main.payment_month,
		date(main.payment_month + interval '1 month') as next_calendar_month, -- next calendar months
		lag(main.payment_month) over(partition by main.user_id order by main.payment_month) as previous_paid_month, -- previous month user made a payment
		lead(main.payment_month) over(partition by main.user_id order by main.payment_month) as next_paid_month, -- next month user made a payment
		min(main.payment_month) over(partition by main.user_id order by main.payment_month) as first_paid_month, -- first payment month to calculate new_mrr
		max(main.payment_month) over(partition by main.user_id) as churn_month, -- last pay month to calculate churned users
		main.user_id,
		main.user_revenue,
		lag(main.user_revenue) over(partition by main.user_id order by main.payment_month) as previous_month_revenue
	from 
		main
	order by 2, 6
),
calculations as (
	select 
		payment_month,
		previous_paid_month,
		first_paid_month,
		user_id,
		case -- New Users 
			when payment_month = first_paid_month then user_id
			else null end as new_users,
		user_revenue,
		case -- New Revenue (MRR)
			when payment_month = first_paid_month then user_revenue
			else null end as new_revenue,	
		case -- Churned Users (in next month)
			when payment_month = churn_month then user_id
			else null end as churned_users,
		case -- Churned Revenue (in next month)
			when payment_month = churn_month then user_revenue
			else null end as churned_revenue,		
		case -- Returned Users
			when payment_month > first_paid_month and (EXTRACT(MONTH FROM payment_month) - EXTRACT(MONTH FROM previous_paid_month))> 1 then user_id
			else null end as returned_users,
		case -- Back from Churn Revenue
			when payment_month > first_paid_month and (EXTRACT(MONTH FROM payment_month) - EXTRACT(MONTH FROM previous_paid_month))> 1 then user_revenue
			else null end as back_from_churn_revenue,
		case -- Revenue Expansion - revenue difference between current month and previous calendar month revenue (if was)
			when previous_paid_month=previous_calendar_month
			and user_revenue > previous_month_revenue
			then user_revenue-previous_month_revenue 
			else null end as expansion_mrr,
		case -- Revenue Contraction - revenue difference between current month and previous calendar month revenue (if was)
			when previous_paid_month=previous_calendar_month
			and user_revenue < previous_month_revenue
			then user_revenue-previous_month_revenue 
			else null end as contraction_mrr
	from
		revenue_months
)
select -- final Table
	calculations.payment_month,
	calculations.user_id,
	calculations.new_users,
	calculations.user_revenue,
	calculations.new_revenue, -- next month
	calculations.churned_users, -- next month
	calculations.churned_revenue,
	calculations.returned_users,
	calculations.back_from_churn_revenue,
	calculations.expansion_mrr,
	calculations.contraction_mrr,
	lt_ltv.customer_lifetime,
	lt_ltv.customer_lifetime_value,
	lt_ltv.game_name,
	lt_ltv.language,
	lt_ltv.has_older_device_model,
	lt_ltv.age,
	lt_ltv.age_groups
from calculations
left join lt_ltv on calculations.user_id= lt_ltv.user_id 
order by 
	1, 2

--)
--select -- TEST
--	payment_month,
--	count(distinct user_id) as total_users,
--	count(distinct new_users) as new,
--	sum(user_revenue) as mrr,
--	sum (new_revenue) as new_revenue,
--	count(returned_users) as returned,
--	sum (back_from_churn_revenue) as back_from_churn,
--	sum(expansion_mrr) as expansion,
--	sum(contraction_mrr) as contraction,
--	count(distinct churned_users) as churned_users,
--	sum(churned_revenue) as churned_revenue,
--	avg(customer_lifetime) as customer_lifetime,
--	avg(customer_lifetime_value) as customer_lifetime
--	--	
----	round(avg(distinct customer_lt), 1) as customer_lt,
----	round(avg(distinct customer_ltv), 1) as customer_ltv
----	*
--from 
--	test
----	calculations
----	revenue_months
--group by 1
--order by payment_month

