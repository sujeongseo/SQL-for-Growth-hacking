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