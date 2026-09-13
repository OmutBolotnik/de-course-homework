-- Тест: немає PR, де merged_at < opened_at або closed_at < opened_at.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.

SELECT *
FROM {{ ref('pull_requests') }}
WHERE (merged_at IS NOT NULL AND merged_at < opened_at)
   OR (closed_at IS NOT NULL AND closed_at < opened_at)
