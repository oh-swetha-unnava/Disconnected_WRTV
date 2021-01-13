DROP VIEW SUNNAVA.DISCONNECTED_WRTV;
CREATE OR REPLACE VIEW SUNNAVA.DISCONNECTED_WRTV AS
SELECT DEVICE_TYPE,DATE,COUNT(DISTINCT ASSET_ID) AS OPEN_CASES, SUM(NEW_CASE) AS NEW_CASES,
COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END) AS PREVIOUSLY_OPENED_CASES,
SUM(CLOSED_CASE) AS CLOSED_CASES
FROM (
	SELECT A.*,
	CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 0
	WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
	OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1 THEN 1 ELSE 0 END AS NEW_CASE,
	CASE
WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL) THEN 1
WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1 THEN 1
ELSE 0 END AS CLOSED_CASE
	FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
	ORDER BY 4 DESC)
GROUP BY 1,2;


----- Jonathans View ------
	DROP VIEW SUNNAVA.DISCONNECTED_WRTV_v3;
	CREATE OR REPLACE VIEW SUNNAVA.DISCONNECTED_WRTV_v3 AS
	with disconnect_wrtv as(
			SELECT A.*,
			CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 0
			WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
			OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1 THEN 1 ELSE 0 END AS NEW_CASE,
			CASE WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL)
					 THEN 1
					 WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1 THEN 1
					 ELSE 0 END AS CLOSED_CASE
			FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
			ORDER BY 4 DESC),

	closed_cases as (
	        select DEVICE_TYPE,DATE,
	        COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END) AS starting_balance_2,
	        COUNT(DISTINCT CASE WHEN CLOSED_CASE = 1 THEN ASSET_ID ELSE NULL END) AS closed_CASES
	        from disconnect_wrtv
	        group by 1,2
	),

	open_cases as (
				select DEVICE_TYPE,DATE2 as date,
				COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END) AS starting_balance,
				COUNT(DISTINCT CASE WHEN NEW_CASE = 1 THEN ASSET_ID ELSE NULL END) AS new_CASES
				from (select a.*,DATE-1 as date2
				      from disconnect_wrtv a)
				group by 1,2

	),

	dates as(
			select distinct  date from open_cases
			union
			select distinct date from closed_cases),

	all_cases as (
			select ams_id,asset_id,cmh_id,date_2 as date, device_type,new_case,0 as closed_case
			from (select a.*,DATE-1 as date_2
			      from disconnect_wrtv a)
			union
			select ams_id,asset_id,cmh_id, date, device_type,
						case when new_case = 1 then 0 else new_case end as new_case,closed_case
			from disconnect_wrtv a )

	select a.DEVICE_TYPE,a.date,
	COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END) AS starting_balance,
	COUNT(DISTINCT CASE WHEN NEW_CASE = 1 THEN ASSET_ID ELSE NULL END) AS new_CASES,
	COUNT(DISTINCT CASE WHEN CLOSED_CASE = 1 THEN ASSET_ID ELSE NULL END) AS closed_CASES,
	COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END)
	+ COUNT(DISTINCT CASE WHEN NEW_CASE = 1 THEN ASSET_ID ELSE NULL END)
	- COUNT(DISTINCT CASE WHEN CLOSED_CASE = 1 THEN ASSET_ID ELSE NULL END) as net_cases
	from all_cases a
	where a.DATE !=TO_CHAR(GETDATE(), 'YYYY-MM-DD') -- As Jonathan doesnt want to show todays report as we arent aware of # closed cases
	group by 1,2
	order by  a.device_type, a.date desc;

CREATE OR REPLACE VIEW sunnava.disconnected_screen_issue_wrtv AS
WITH disconnected_totals AS
(SELECT date, b.ranking, count(distinct a.ams_id) as total_disconnectd, count(distinct (case when device_type = 'AMP' THEN a.ams_id else null end)) as num_amp, count(distinct (case when device_type = 'LMP' THEN a.ams_id else null end)) as num_lmp
FROM campaign_delivery.devices_persistent_screen_issue_daily a
LEFT JOIN ams.assets c on a.ams_id = c.ams_id
LEFT JOIN salesforce.accounts b on a.cmh_id = b.cmh_id
where STATUS = 'Installed'
GROUP BY 1,2
),
total_wrtvs as
(SELECT DISTINCT (A.EXPORT_DATE -1)::DATE AS DATE, B.RANKING, COUNT(DISTINCT AMS_ID) AS TOTAL_WRS, COUNT(DISTINCT(CASE WHEN E.NAME = 'LINUX_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_LMP, COUNT(DISTINCT(CASE WHEN E.NAME = 'ANDROID_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_AMP
FROM AMS.ASSETS_HISTORY A
LEFT JOIN SALESFORCE.ACCOUNTS B ON A.CMH_ID = B.CMH_ID
LEFT JOIN ASSET_STATUS_ENGINE.ASSET C ON UPPER(A.ASSET_TAG) = UPPER(C.FIELD_SERVICES_TAG)
LEFT JOIN ASSET_STATUS_ENGINE.SKU D ON C.SKU_ID = D.ID
LEFT JOIN ASSET_STATUS_ENGINE.SKU_TYPE E ON D.SKU_TYPE_ID = E.ID
WHERE PRODUCT = 'Waiting Room Screen'
AND A.STATUS = 'Installed'
AND ranking IS NOT NULL
GROUP BY 1,2
)

SELECT a.*,
  b.total_disconnectd, num_amp, num_lmp
FROM total_wrtvs a
LEFT JOIN disconnected_totals b ON a.ranking = b.ranking AND a.date = b.date
WHERE a.date >= '2020-10-08';

CREATE OR REPLACE VIEW sunnava.disconnected_screen_issue_wrtv_agg AS
select date, to_char(a.date,'MON-YY-DD') as date2,rank_grp,sum(total_disconnectd),sum(total_wrs), (cast(sum(total_disconnectd) as float)/sum(total_wrs) )* 100 as percent_
from (
select date,ranking, case when ranking <5 then '1-5' else '6-10' end as rank_grp,
total_wrs,total_lmp,total_amp,total_disconnectd
from sunnava.disconnected_screen_issue_wrtv) a
--left join (select date,sum(total_disconnectd) as ttl from sunnava.disconnected_screen_issue_wrtv group by 1) b
--on a.date = b.date
group by 1,2,3 ;


---------------- ADDING CLINIC RANK ---------------

DROP VIEW SUNNAVA.DISCONNECTED_WRTV_v4;
CREATE OR REPLACE VIEW SUNNAVA.DISCONNECTED_WRTV_v4 AS
	with disconnect_wrtv as(
			SELECT A.*,
			CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 0
			WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
			OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1 THEN 1 ELSE 0 END AS NEW_CASE,
			CASE WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL)
					 THEN 1
					 WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1 THEN 1
					 ELSE 0 END AS CLOSED_CASE
			FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
			ORDER BY 4 DESC),

	clinic_ids as (
					select asset_id, max(ams_id) as ams_id, max(cmh_id) as cmh_id
					from disconnect_wrtv
					group by asset_id
	),

	closed_cases as (
	        select DEVICE_TYPE,DATE,
	        COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END) AS starting_balance_2,
	        COUNT(DISTINCT CASE WHEN CLOSED_CASE = 1 THEN ASSET_ID ELSE NULL END) AS closed_CASES
	        from disconnect_wrtv
	        group by 1,2
	),

	open_cases as (
				select DEVICE_TYPE,DATE2 as date,
				COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END) AS starting_balance,
				COUNT(DISTINCT CASE WHEN NEW_CASE = 1 THEN ASSET_ID ELSE NULL END) AS new_CASES
				from (select a.*,DATE-1 as date2
				      from disconnect_wrtv a)
				group by 1,2

	),

	dates as(
			select distinct  date from open_cases
			union
			select distinct date from closed_cases),

	all_cases as (
			select ams_id,asset_id,cmh_id,date_2 as date, device_type,new_case,0 as closed_case
			from (select a.*,DATE-1 as date_2
			      from disconnect_wrtv a)
			union
			select ams_id,asset_id,cmh_id, date, device_type,
						case when new_case = 1 then 0 else new_case end as new_case,closed_case
			from disconnect_wrtv a )

	select *
	from (select a.DEVICE_TYPE,a.date,b.ranking::varchar as clinic_ranking,
				COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN a.ASSET_ID ELSE NULL END) AS starting_balance,
				COUNT(DISTINCT CASE WHEN NEW_CASE = 1 THEN a.ASSET_ID ELSE NULL END) AS new_CASES,
				COUNT(DISTINCT CASE WHEN CLOSED_CASE = 1 THEN a.ASSET_ID ELSE NULL END) AS closed_CASES,
				COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN a.ASSET_ID ELSE NULL END)
				+ COUNT(DISTINCT CASE WHEN NEW_CASE = 1 THEN a.ASSET_ID ELSE NULL END)
				- COUNT(DISTINCT CASE WHEN CLOSED_CASE = 1 THEN a.ASSET_ID ELSE NULL END) as net_cases
				from all_cases a
				left join clinic_ids c on a.asset_id = c.asset_id
				LEFT JOIN salesforce.accounts b on c.cmh_id = b.cmh_id
				where a.DATE !=TO_CHAR(GETDATE(), 'YYYY-MM-DD') -- As Jonathan doesnt want to show todays report as we arent aware of # closed cases
				group by 1,2,3)a
	order by  a.date desc,a.device_type;



CREATE OR REPLACE VIEW SUNNAVA.current_disconnected_wrtv AS
select ranking, sum(total_disconnects) as total_disconnects,
sum(previously_opened_cases) as previously_opened_cases,
sum(new_cases) as new_cases,
sum(lmp_total_disconnects) as lmp_total_disconnects,
sum(lmp_new_cases) as lmp_new_cases,
sum(lmp_previously_opened_cases) as lmp_previously_opened_cases,
sum(amp_total_disconnects) as amp_total_disconnects,
sum(amp_previously_opened_cases) as amp_previously_opened_cases,
sum(amp_new_cases) as amp_new_cases,
sum(closed_cases) as closed_cases,
sum(lmp_closed_cases) as lmp_closed_cases,
sum(amp_closed_cases) as amp_closed_cases
from(
select ranking, case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then total_disconnects end as total_disconnects,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then previously_opened_cases end as previously_opened_cases,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then new_cases end as new_cases,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then lmp_total_disconnects end as lmp_total_disconnects,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then lmp_previously_opened_cases end as lmp_previously_opened_cases,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then lmp_new_cases end as lmp_new_cases,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then amp_total_disconnects end as amp_total_disconnects,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then amp_previously_opened_cases end as amp_previously_opened_cases,
case when date = TO_CHAR(GETDATE(), 'YYYY-MM-DD') then amp_new_cases end as amp_new_cases,
case when date = TO_CHAR(GETDATE()-1, 'YYYY-MM-DD') then closed_cases end as closed_cases,
case when date = TO_CHAR(GETDATE()-1, 'YYYY-MM-DD') then lmp_closed_cases end as lmp_closed_cases,
case when date = TO_CHAR(GETDATE()-1, 'YYYY-MM-DD') then amp_closed_cases end as amp_closed_cases
from (
	select date, ranking,
	count(distinct ams_id) as total_disconnects,
	count( distinct case when new_case = 0 then ams_id end) as previously_opened_cases,
	sum(new_case) as new_cases,
	sum(closed_case) as closed_cases,

	count(distinct case when device_type = 'LMP' then ams_id end) as lmp_total_disconnects,
	count( distinct case when new_case = 0 and device_type = 'LMP' then ams_id end) as lmp_previously_opened_cases,
	sum(case when device_type = 'LMP' then new_case end) as lmp_new_cases,
	sum(case when device_type = 'LMP' then closed_case end ) as lmp_closed_cases,

	count(distinct case when device_type = 'AMP' then ams_id end) as amp_total_disconnects,
	count( distinct case when new_case = 0 and device_type = 'AMP' then ams_id end) as amp_previously_opened_cases,
	sum(case when device_type = 'AMP' then new_case end) as amp_new_cases,
	sum(case when device_type = 'AMP' then closed_case end ) as amp_closed_cases
	From (
			SELECT date, b.ranking,  device_type,a.ams_id,
			CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 0
	WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
	OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1 THEN 1 ELSE 0 END AS NEW_CASE,
				CASE
			WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL) THEN 1
			WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1 THEN 1
			ELSE 0 END AS CLOSED_CASE
			FROM campaign_delivery.devices_persistent_screen_issue_daily a
			LEFT JOIN ams.assets c on a.ams_id = c.ams_id
			LEFT JOIN salesforce.accounts b on a.cmh_id = b.cmh_id
			where STATUS = 'Installed')
	group by 1,2
	order by 1 desc, 2)
where date in (TO_CHAR(GETDATE()-1, 'YYYY-MM-DD'),TO_CHAR(GETDATE(), 'YYYY-MM-DD')))
group by 1
order by 1;

GRANT SELECT ON SUNNAVA.DISCONNECTED_WRTV TO GROUP REPORTING_ROLE;

----- QC/DEBUGGING

select distinct
  a."asset name", A."completed date",
  b.cmh_id,
  c.cmh_id,
  d.id,
  d.client_id,
  d.asset_id,
  d.type
from sunnava.closed_wrtv_cases_1203 a
  join salesforce.cases b on b.case_number = a."case: case number"
  join mdm.clinics c on c.cmh_id = b.cmh_id
  join mdm.devices d on d.clinic_table_id = c.id and d.type LIKE'%MediaPlayer'
where --d.asset_id in (select asset_id from campaign_delivery.devices_persistent_screen_issue_daily )AND
B.CMH_ID IN (486676,
396115,
62810,
504349,
537322)
order by 1,2,3,4

create table sunnava.closed_wrtv_cases_1203_v2 as
select distinct a.*,b.cmh_id, c.ams_id,c.asset_tag
from sunnava.closed_wrtv_cases_1203 a
left join salesforce.cases b on a."case: case number"=b.case_number
left join AMS.ASSETS_HISTORY c on b.cmh_id = c.cmh_id
where
"asset name" = asset_tag;

/*
--- TRY OUT
SELECT A.*,
LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),
DATEDIFF(HOUR,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE))/24 AS X,
CASE WHEN DATEDIFF(HOUR,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE))/24 IS NULL OR DATEDIFF(HOUR,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE))/24 != -1 THEN 1 ELSE 0 END AS NEW_CASE,
LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) ,
DATEDIFF(HOUR,DATE,LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE))/24 AS Y,
CASE WHEN DATEDIFF(HOUR,DATE,LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE))/24 IS NULL OR DATEDIFF(HOUR,DATE,LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE))/24 != 1 THEN 1 ELSE 0 END AS CLOSED_CASE
FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
ORDER BY 4;
*/

DROP VIEW SUNNAVA.DISCONNECTED_WRTV_v2;
CREATE OR REPLACE VIEW SUNNAVA.DISCONNECTED_WRTV_v2 AS
select * from (
select DEVICE_TYPE,DATE,
lead(NEW_CASES,1) over (partition by DEVICE_TYPE order by date) new_cases,
lead(PREVIOUSLY_OPENED_CASES,1) over (partition by DEVICE_TYPE order by date) starting_balance, CLOSED_CASES,
lead(OPEN_CASES,1) over (partition by DEVICE_TYPE order by date)-closed_cases as net_cases
from (SELECT DEVICE_TYPE,DATE,COUNT(DISTINCT ASSET_ID) AS OPEN_CASES, SUM(NEW_CASE) AS NEW_CASES,
	COUNT(DISTINCT CASE WHEN NEW_CASE = 0 THEN ASSET_ID ELSE NULL END) AS PREVIOUSLY_OPENED_CASES,
	SUM(CLOSED_CASE) AS CLOSED_CASES
	FROM (
		SELECT A.*,
		CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 0
		WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
		OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1 THEN 1 ELSE 0 END AS NEW_CASE,
		CASE
	WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL) THEN 1
	WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1 THEN 1
	ELSE 0 END AS CLOSED_CASE
		FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
		ORDER BY 4 DESC)
	GROUP BY 1,2))
	where DATE !=TO_CHAR(GETDATE(), 'YYYY-MM-DD')
	order by  device_type, date desc;

---- Closed LMP cases in recent 5 days
select distinct ams_id,asset_id,cmh_id,date from
(SELECT A.*,
	CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 0
	WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
	OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1 THEN 1 ELSE 0 END AS NEW_CASE,
	CASE
WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL) THEN 1
WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1 THEN 1
ELSE 0 END AS CLOSED_CASE
	FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
	ORDER BY 4 DESC)
	where
	and device_type = 'LMP'
	and DATE between getdate()-6 and getdate();
