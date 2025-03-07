******************************************************
1-6. 데이터 PIVOT 연습 문제

PIVOT 개념 정리
MAX(IF(컬럼에 대한 조건 1), 행으로 노출하고 싶은 값, 그 외 NULL)
UNNEST 개념 정리
CROSS JOIN UNNEST(ARRAY OR STRUCT) AS 컬럼 별칭


# Pivot 연습문제 1
# 유저 별로 주문 금액 합계를 PIVOT 하기. (조건, 날짜는 행으로, 유저 Id는 열)
# ㄴ 일자별 유저 주문 금액 합계 확인하기 위한 테이블

# 쿼리

SELECT
order_date,
SUM(IF(user_id = 1, amount, NULL)) AS user_1,
SUM(IF(user_id = 2, amount, NULL)) AS user_2,
SUM(IF(user_id = 3, amount, NULL)) AS user_3,

FROM advanced.orders
GROUP BY order_date

# 유의점
#user_id와 order_date 같은 INT 는 "" or '' 를 사용하지 않고 바로 숫자를 기입한다


# Pivot 연습문제 2
# 날짜별로 유저들의 주문 금액의 합계를 피벗 (조건 : user_id는 행, order_date 는 열)

# 쿼리

SELECT
user_id,
MAX(IF(order_date = '2023-05-01', amount, 0)) AS `2023-05-01`,
MAX(IF(order_date = '2023-05-02', amount, 0)) AS `2023-05-02`,
MAX(IF(order_date = '2023-05-03', amount, 0)) AS `2023-05-03`,
MAX(IF(order_date = '2023-05-04', amount, 0)) AS `2023-05-04`,
MAX(IF(order_date = '2023-05-05', amount, 0)) AS `2023-05-05`

FROM advanced.orders
GROUP BY user_id

# 유의점
# 숫자나 날짜로 컬럼으로 지정할때 backtick `` 사용

# Pivot 연습문제 3
# 사용자별, 날짜별 주문이 있다면 1, 없다면 0으로 피벗 (조건 : user_id는 행, order_date는 열)
# ㄴ 일자별 방문 리텐션 구하는 것과 비슷한 테이블 모양새

# 쿼리
WITH customers AS (
SELECT
order_date,
user_id,
IF (amount > 1, 1, NULL) AS customer
FROM advanced.orders
)


SELECT
user_id,
MAX(IF(order_date = '2023-05-01', 1, 0)) AS `2023-05-01`,
MAX(IF(order_date = '2023-05-02', 1, 0)) AS `2023-05-02`,
MAX(IF(order_date = '2023-05-03', 1, 0)) AS `2023-05-03`,
MAX(IF(order_date = '2023-05-04', 1, 0)) AS `2023-05-04`,
MAX(IF(order_date = '2023-05-05', 1, 0)) AS `2023-05-05`,
FROM customers c
GROUP BY user_id

# 유의점
# amount 를 굳이 1로 변경하는 With 문 사용할 필요 없음. true면 1 false면 0을 바로 쓰면 되기 때문
# 주문 횟수를 구하고 싶다면 MAX 가 아닌 SUM 사용 (일자별 구매 건수 마다 1+1+1+1)


# Pivot 연습문제 4
# 앱 로그 데이터 배열 PIVOT 하기
# user_id = 32888이 카트 추가하기를 누를 때 어떤 음식을 담았나요?


# 22년 8월 1일에 32888 고객이 카트에 담은 음식은?
# user_id = 32888이 event_name = click_cart 했을 때 food_id 의 int_value가 무엇이였나요?

#UNNEST 한 앱 로그 데이터를 WITH 문 처리하여
WITH base AS(
SELECT 
event_date,
event_timestamp,
event_name,
user_id,
user_pseudo_id,
MAX(IF(params.key='firebase_screen',params.value.string_value,NULL)) AS `screen`,
MAX(IF(params.key='food_id',params.value.int_value,NULL)) AS `food_id`,
MAX(IF(params.key='session_id',params.value.string_value,NULL)) AS `session_id`, 
FROM advanced.app_logs
CROSS JOIN UNNEST(event_params) AS params
GROUP BY ALL)

SELECT
*
FROM base
# 조회하고 싶은 조건 필터링
WHERE 
event_date = '2022-08-01' 
AND user_id = 32888
AND event_name = "click_cart"

**************************************************************************************
2025-03-07 

# 피벗 연습 문제 1. 각 퍼널의 유저 수를 집계 (조건 : 2022-08-01 ~ 2022-08-18)

# 풀이 순서 
# 1. UNNEST 2. PIVOT 3. CONCAT 4. CASE WHEN을 활용한 step_number 5.COUTN(DISTINCT user_pseudo_id)
# 헷갈리는 문법 
# CASE WHEN 의 END 자리 + WITH 연속문의 AS 자리 + TIMESTAMP <> DATETIME

WITH basic AS
(SELECT
event_date,
event_timestamp,
event_name,
user_id,
user_pseudo_id,
platform,
MAX(if(event_param.key='firebase_screen',event_param.value.string_value,NULL)) AS screen,
MAX(if(event_param.key='session_id',event_param.value.string_value,NULL)) AS session,
MAX(if(event_param.key='food_id',event_param.value.int_value,NULL)) AS food,

FROM advanced.app_logs
CROSS JOIN UNNEST(event_params) AS event_param
WHERE event_name IN ("screen_view", "click_payment")
AND event_date BETWEEN '2022-08-01' AND '2022-08-18'
GROUP BY ALL

)
,
filering AS (
SELECT
* EXCEPT(event_name, screen, platform),
CONCAT(event_name,"-",basic.screen) AS screen_view,

FROM basic )

SELECT
event_date,
filering.screen_view,
CASE
  WHEN screen_view = 'screen_view-welcome' THEN 1
  WHEN screen_view = 'screen_view-home' THEN 2
  WHEN screen_view = 'screen_view-food_category' THEN 3
  WHEN screen_view = 'screen_view-restaurant' THEN 4
  WHEN screen_view = 'screen_view-cart' THEN 5
  WHEN screen_view = 'click_payment-cart' THEN 6
  ELSE NULL
  END AS step_number,
COUNT (DISTINCT user_pseudo_id) AS cnt
FROM filering 
GROUP BY ALL
HAVING step_number IS NOT NULL
ORDER BY event_date, step_number 
