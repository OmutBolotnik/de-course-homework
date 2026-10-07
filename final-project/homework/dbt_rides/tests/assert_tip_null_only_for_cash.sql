-- NULL-чайові означають «готівка, чайові невідомі», а не «0». Тож для завершених поїздок,
-- де спосіб оплати вже відомий: tip_amount IS NULL ⇔ payment_method = 'cash'.
-- Ловить coalesce(tip, 0) у моделі (готівка отримала б 0) і втрату чайових карткою / гаманцем.
-- Поїздки без payment_captured (оплата ще не приїхала) пропускаємо: там спосіб оплати невідомий.
SELECT ride_id, payment_method, tip_amount
FROM {{ ref('rides') }}
WHERE status = 'completed'
  AND payment_method IS NOT NULL
  AND (tip_amount IS NULL) <> (payment_method = 'cash')
