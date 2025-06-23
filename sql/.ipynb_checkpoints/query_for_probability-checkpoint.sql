-----最终游泳的landing page，不许改了！！用的是新方法
SET odps.sql.hive.compatible = true;
SET odps.sql.groupby.position.alias = true;
SET odps.sql.allow.fullscan = true;
SET odps.sql.validate.orderby.limit = false;
with freq_uplift AS(
with 
study_mem as 
(select 
    member_card
    ,min(age_group) as age_group_new_agg
    ,min(pay_time) as min_pay_time
    ,max(pay_time) as max_pay_time
    ,count(distinct(transaction_id)) as freq 
from 
    cndp_df.dpd_tpc_omni_transaction_ticket_member_item
where 
    date(pay_time) between '{PERIOD_STARTING_DATE}' and '{PERIOD_ENDING_DATE}' 
    and is_member = 1
    and item_qty > 0
    and model_code is not null
group by 
    1
    having freq = 2 ---------🌼🌼🌼🌼🌼🌼这里1-2写2，2-3写3
    and date(min_pay_time) between '{FIRST_STARTING_DATE}'and '{FIRST_ENDING_DATE}' 
    and date(max_pay_time) between '{FIRST_ENDING_DATE}'  and '{PERIOD_ENDING_DATE}' 
    --and age_group_new_agg < '36-40'
)
,
detail as (
    select 
    sal.member_card
    --,purchase_department_name_en
    ,model_name_en
    ,model_code
    ,super_model_code
    ,purchase_department_name_en
    ,pay_time
    --,sal.recency_group
    ,dense_rank() over (partition by sal.member_card order by pay_time asc) as rn
    ,sum(item_amount) as turnover
    ,sum(item_qty) as qty
from 
    (select * from cndp_df.dpd_tpc_omni_transaction_ticket_member_item where is_member = 1 
    and item_qty > 0 
    and date(pay_time) between '{PERIOD_STARTING_DATE}' and '{PERIOD_ENDING_DATE}' 
    ---and age_group_new < '36-40'
    and super_model_code not in ('9900327106','9900154253','9900343617','9900331330')
    and family_id <> '35328'
    and model_code is not null) sal
inner join 
    study_mem
on 
    sal.member_card = study_mem.member_card
group by 1,2,3,4,5,6)
--select family_name_en,rn,count(distinct(member_card)) as mem_cnt
select 
    a.member_card
    ,a.rn
    ,date(a.pay_time) as date1
    ,a.purchase_department_name_en as sports_1
    --,a.model_name_en as sports_1
    ,b.rn as rn2
    ,date(b.pay_time) as date2
    --,b.purchase_department_name_en as sports_2

--     ,case when b.model_code in ('8615432',
-- '8601260',
-- '8919753',
-- '8927722',
-- '8862064',
-- '8862068',
-- '8919267',
-- '8919744',
-- '8731931') then 'landing page models'  else 'others' end as sports_2_name
    ,case when b.model_code in ({MODEL_LIST})
    then 'landing page models'  else 'others' end as sports_2_name

--c.rn as rn3,date(c.pay_time) as date3,c.pay_season as sn_3,c.purchase_department_name_en as sports_3,
    ,a.turnover as to1
    , b.turnover as to2
--,c.turnover as to3
from 
    detail a 
left join 
    detail b 
on 
    a.member_card = b.member_card
    and a.rn + 1 = b.rn
where 
a.rn = 1) --------🌼🌼🌼🌼🌼🌼这里1-2写1，2-3写2
select sports_1,sports_2_name,count(distinct(member_card)) as mem_cnt ,
avg(datediff(date2,date1)) as gap_days
from
(select *
from freq_uplift)
group by 1,2
order by mem_cnt desc;