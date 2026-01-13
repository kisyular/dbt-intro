SELECT COUNT(*) AS violation_count,
FROM {{ ref('first_model') }}