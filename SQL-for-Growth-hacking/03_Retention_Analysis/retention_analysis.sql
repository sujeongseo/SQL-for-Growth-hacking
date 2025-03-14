# Retention 연습 문제 1. 
# DAY N 리텐션 구하기
-- 중복 제거 주의

-- 1. event_date 구하기 (TIMESTAMP/DATETIME/DATE)
WITH base AS
  (SELECT 
    DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')) AS event_date,
    user_pseudo_id,
  FROM advanced.app_logs
  GROUP BY ALL 
  )

-- 2. first_event_date 구하기
, first_event AS
  (SELECT
    MIN(event_date) OVER (PARTITION BY user_pseudo_id) AS first_event_date,
    event_date,
    user_pseudo_id,
  FROM base
  GROUP BY ALL
  )

-- 3. date_diff 구하기 4. user 수 구하기
, cnt AS (SELECT 
DATE_DIFF(event_date, first_event_date, DAY) AS diff_of_day,
COUNT(user_pseudo_id) AS user_cnt
FROM first_event
GROUP BY ALL
ORDER BY 1
)

-- 5. FIRST VALUE 열 만들기
, retention AS
  (SELECT
  *,
  LAG(user_cnt) OVER (ORDER BY diff_of_day) AS before_cnt,
  FIRST_VALUE(user_cnt) OVER (ORDER BY diff_of_day) AS total_cnt
  FROM cnt
  )

SELECT
*,
SAFE_DIVIDE(user_cnt,total_cnt) AS total_cvr,
SAFE_DIVIDE(user_cnt,before_cnt) AS funnel_cvr,

FROM retention



# Weekly Retention 구하기

-- DATE > WEEK 변환
  WITH base AS (
  SELECT 
  EXTRACT(WEEK FROM DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')) AS event_week,
  user_pseudo_id,
  FROM advanced.app_logs
  GROUP BY ALL
  )
-- FIRST WEEK 추출

, first_week AS
  (SELECT
  MIN(event_week) OVER(PARTITION BY user_pseudo_id) AS first_event_week,
  *,
  FROM base 
  GROUP BY ALL
  )
, cnt AS
  (SELECT
  event_week - first_event_week AS diff_of_week,
  COUNT(user_pseudo_id) AS user_cnt,
  FROM first_week
  GROUP BY ALL
  )
, retention AS
  (SELECT 
  diff_of_week,
  user_cnt,
  FIRST_VALUE(user_cnt) OVER(ORDER BY diff_of_week) AS total_cnt,
  FROM cnt
  )

SELECT
*,
SAFE_DIVIDE(user_cnt, total_cnt) AS cvr
FROM retention
ORDER BY 1