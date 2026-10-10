-- Тест: churn у fact_pull_request завжди = additions + deletions.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.

SELECT *
FROM {{ ref('fact_pull_request') }}
WHERE churn != additions + deletions
