# WINDOW 연습 문제 1
# user들의 다음 접속 월과 다다음 접속 월을 구하라.

SELECT 
user_id,
visit_month,
LEAD (visit_month, 1) OVER (PARTITION BY user_id ORDER BY visit_month) AS next_1month,
LEAD (visit_month, 2) OVER (PARTITION BY user_id ORDER BY visit_month) AS next_2month
FROM `inflearn-bigquery-451604.advanced.analytics_function_01` LIMIT 1000


# WINDOW 연습 문제 2
# user들의 다음 접속 월과 다다음 접속 월, 이전 접속 월을 구하라.

SELECT 
user_id,
visit_month,
LEAD (visit_month, 1) OVER (PARTITION BY user_id ORDER BY visit_month) AS next_1month,
LEAD (visit_month, 2) OVER (PARTITION BY user_id ORDER BY visit_month) AS next_2month,
LAG (visit_month, 1) OVER (PARTITION BY user_id ORDER BY visit_month) AS last_1month,
FROM `inflearn-bigquery-451604.advanced.analytics_function_01` LIMIT 1000


# Frame 연습 문제 1
  # amount_total(우리 회사의 모든 주문량)
  # cumulative_sum(특정 주문 시점에서 누적 주문량)
  # cumulative_sum_by_user(고객별 주문 시점에서 누적 주문량)
  # last_5_orders_avg_amount(최근 직전 5개의 평균 주문량)


SELECT 
*,
SUM(amount) OVER() AS amount_total, # OVER에 인자가 없어도 연산 가능
SUM(amount) OVER(ORDER BY order_id) AS cumulative_sum, # order_id 의 row순으로 연산
SUM(amount) OVER(PARTITION BY user_id ORDER BY order_id) AS cumulative_sum_by_user, 
# order_id 의 row순으로 연산하되, user_id 의 파티션으로 구분 됨
AVG(amount) OVER(ORDER BY order_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last_5_orders_avg_amount
# order_id 의 frame이 앞선 5개 주문과 현재 row의 앞 1개 까지 규정되어 연산
FROM advanced.orders
ORDER BY order_id

*************************************************************

# WINDOW 연습 문제 1
# 사용자별 쿼리를 실행한 총 횟수
# 전체 개수를 알고 싶다면 → PARTITION BY user만 사용, 날짜별 누적 개수를 알고 싶다면 → PARTITION BY user ORDER BY query_date 추가

-- (최적 답안)

SELECT 
*,
COUNT(query_date) OVER(PARTITION BY user) AS total_query_cnt
FROM advanced.query_logs
ORDER BY user, query_date

-- # WINDOW 함수와 GROUP BY ALL 을 함께 쓰면 안되는 이유
-- COUNT()는 원래 윈도우 함수(OVER())로 동작해야 하는데, GROUP BY ALL을 하면서 일반적인 COUNT()처럼 동작해 버립니다.
-- GROUP BY ALL을 하면 query_date가 같은 행들이 한 줄로 합쳐지면서 데이터가 손실될 가능성이 있습니다.
-- 반면, GROUP BY 없이 윈도우 함수를 사용하면 기존 데이터 행 개수를 유지한 채 COUNT() 값을 추가할 수 있습니다.
-- 따라서 GROUP BY ALL을 추가하지 않는 것이 올바른 결과를 얻는 방법입니다.

-- (답안)
SELECT *,
COUNT(user) OVER(PARTITION BY user) AS total_query_cnt
FROM advanced.query_logs
ORDER BY query_date


# WINDOW 연습 문제 2
# 주차별로 팀 내에서 쿼리를 많이 실행한 수와 실행한 수를 활용해 1등 랭킹 구하기

# 1. 연 주차 컬럼 추가 -> 연 주차별 쿼리 수 컬럼 추가 
# 2. 팀 내 주차별 랭킹 추가 -> 주차별 팀내 1위 조건 걸기

-- (최적 답안)
# 사전 집계 (GROUP BY ALL)를 사용하여 중복 연산을 줄이기
# 각 ROW 마다 데이터가 있는 것을 미리 GROUP BY로 정리해서 WINDOW 로 넘어가는 것이 좋다.
WITH base AS (
  SELECT
  user,
  team,
  EXTRACT (WEEK FROM query_date) AS week_number,
  # query_date 없이 user를 COUNT 하면 week_number 대로 집계됨 + GROUP BY ALL
  COUNT(user) AS query_cnt
  FROM advanced.query_logs
  GROUP BY ALL
)

SELECT
*,
RANK() OVER (PARTITION BY team, week_number ORDER BY query_cnt DESC) AS rk,
FROM base
QUALIFY rk = 1
ORDER BY week_number, team


-- (답안)
# PARTIRION BY 에 두 개 이상 인자 들어가는 경우 헷갈림

WITH week_query AS (
SELECT 
*,
EXTRACT (WEEK FROM query_date) AS week_number, # EXTRACT 사용하여 연 주차 컬럼 추가
FROM advanced.query_logs )

, week_cnt AS (
SELECT 
*,
COUNT(user) OVER (PARTITION BY user ORDER BY week_number) AS week_cnt, # 연 주차별 user의 쿼리 수 COUNT
FROM week_query wq )

, team_rank AS (
SELECT 
*,
RANK() OVER (PARTITION BY week_number, team ORDER BY week_cnt DESC) AS team_rank, # 연 주차별, 팀별 쿼리 수 RANK
FROM week_cnt wc)

SELECT
user,
team,
week_number,
week_cnt,
team_rank
FROM team_rank tr
WHERE tr.team_rank = 1
GROUP BY ALL
ORDER BY week_number

# WINDOW 연습 문제 3
# 쿼리를 실행한 시점 기준 1주 전에 쿼리 실행 수를 별도의 컬럼으로 확인

-- (최적 답안)
# 사전 집계 (GROUP BY ALL) 후 LAG 적용 → 성능 최적화
-- WITH base AS (
--   SELECT
--   user,
--   team,
--   EXTRACT (WEEK FROM query_date) AS week_number,
--   # query_date 없이 user를 COUNT 하면 week_number 대로 집계됨 + GROUP BY ALL
--   COUNT(user) AS query_cnt
--   FROM advanced.query_logs
--   GROUP BY ALL
-- )

-- SELECT
-- *,
-- LAG(query_cnt) OVER (PARTITION BY user ORDER BY week_number) AS prev_query_cnt,
-- FROM base
-- ORDER BY team, user


-- (답안)
WITH week_number AS (
SELECT
*,
EXTRACT (WEEK FROM query_date) AS week_number, # 1. weeknumber 컬럼 생성
FROM advanced.query_logs )


, week_cnt AS (
SELECT
*,
COUNT(user) OVER (PARTITION BY user, week_number) AS week_cnt, # 2. 유저별 week count 컬럼 생성
FROM week_number wn)

SELECT
* EXCEPT(query_date),
LAG(wc.week_cnt) OVER(PARTITION BY user ORDER BY week_number) AS last_week_cnt # 3. LAG(가져올 컬럼) OVER (PARTITION BY 기준 ORDER BY 연주차)

FROM week_cnt wc
GROUP BY ALL # 4. 그룹화  

# WINDOW 연습 문제 3
# 일자 별로 유저가 실행한 누적 쿼리 수

SELECT 
*,
SUM(query_cnt) OVER (PARTITION BY user ORDER BY query_date 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_query_cnt, # 일자별 누적 쿼리 수 SUM

FROM (
SELECT 
*,
COUNT(user) OVER (PARTITION BY user ORDER BY query_date) query_cnt, # 일자별 쿼리 수 COUNT
FROM advanced.query_logs 
GROUP BY ALL
ORDER BY query_date)


# 4. 일자 별로 유저가 실행한 누적 쿼리 수 작성
# Deflaut Frame : ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
WITH base AS (
  SELECT  
  user,
  team,
  query_date,
  COUNT(user) AS query_cnt
  FROM advanced.query_logs
  GROUP BY ALL
)

SELECT 
*,
SUM(query_cnt) OVER (PARTITION BY user ORDER BY query_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cnt

FROM base

******************************************************************

# 5. 주문 횟 수가 없는 NULL 값에 이전 날짜 값을 채워주기 
# 이전 날짜도 null 이면 어떻게 해야하지?


WITH raw_data AS (
SELECT DATE '2024-05-01' AS date, 15 AS number_of_orders UNION ALL SELECT DATE '2024-05-02', 13 UNION ALL
SELECT DATE '2024-05-03', NULL UNION ALL
SELECT DATE '2024-05-04', 16 UNION ALL
SELECT DATE '2024-05-05', NULL UNION ALL
SELECT DATE '2024-05-06', 18 UNION ALL
SELECT DATE '2024-05-07', 20 UNION ALL
SELECT DATE '2024-05-08', NULL UNION ALL
SELECT DATE '2024-05-09', 13 UNION ALL
SELECT DATE '2024-05-10', 14 UNION ALL
SELECT DATE '2024-05-11', NULL UNION ALL
SELECT DATE '2024-05-12', NULL
)

, lag AS (
SELECT
*,
LAG (number_of_orders, 1) OVER (ORDER BY rd.date) AS lag1, # 1. 이전 주문 수 칼럼
LAG (number_of_orders, 2) OVER (ORDER BY rd.date) AS lag2, # 2. 이이전 주문 수 칼럼

FROM raw_data rd )
, orders AS (
SELECT 
*,
IF (number_of_orders IS NOT NULL, l.number_of_orders, l.lag1) AS lag_num_1,
FROM lag l
)

SELECT 
date,
IF (lag_num_1 IS NOT NULL, lag_num_1, lag2 ) AS lag_num_2
FROM orders o


# 6. 2일 전 ~ 현재 데이터의 평균을 구하는 쿼리 작성(이동평균)


WITH raw_data AS (
SELECT DATE '2024-05-01' AS date, 15 AS number_of_orders UNION ALL SELECT DATE '2024-05-02', 13 UNION ALL
SELECT DATE '2024-05-03', NULL UNION ALL
SELECT DATE '2024-05-04', 16 UNION ALL
SELECT DATE '2024-05-05', NULL UNION ALL
SELECT DATE '2024-05-06', 18 UNION ALL
SELECT DATE '2024-05-07', 20 UNION ALL
SELECT DATE '2024-05-08', NULL UNION ALL
SELECT DATE '2024-05-09', 13 UNION ALL
SELECT DATE '2024-05-10', 14 UNION ALL
SELECT DATE '2024-05-11', NULL UNION ALL
SELECT DATE '2024-05-12', NULL
)

, lag AS (
SELECT
*,
LAG (number_of_orders, 1) OVER (ORDER BY rd.date) AS lag1, # 1. 이전 주문 수 칼럼
LAG (number_of_orders, 2) OVER (ORDER BY rd.date) AS lag2, # 2. 이이전 주문 수 칼럼

FROM raw_data rd )
, orders AS (
SELECT 
*,
IF (number_of_orders IS NOT NULL, l.number_of_orders, l.lag1) AS lag_num_1,
FROM lag l
)

, avg_null AS (
SELECT 
date,
IF (lag_num_1 IS NOT NULL, lag_num_1, lag2 ) AS lag_num_2
FROM orders o
)

SELECT 
*,
AVG(lag_num_2) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
FROM avg_null an

**********************************************************



