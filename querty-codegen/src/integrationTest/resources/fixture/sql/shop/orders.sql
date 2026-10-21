-- :name count-orders :one
-- :type customer_id CustomerId
-- :not-null n
SELECT count(*) AS n FROM orders WHERE customer_id = :customer_id

-- :name find-order :optional
-- :doc Load one order by id
-- :type id OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus enum
SELECT {{order-columns}}
FROM orders o
WHERE o.id = :id

-- :name customer-orders :many
-- :type customer_id CustomerId
-- :type status OrderStatus enum
-- :nullable status
SELECT o.id, o.status, c.name AS customer_name
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE o.customer_id = :customer_id
  AND (:status::text IS NULL OR o.status = :status)
ORDER BY o.placed_at DESC
LIMIT :limit

-- :name save-order :affected
-- :type id OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus enum
-- :nullable reason
INSERT INTO orders (id, customer_id, status, reason, placed_at)
VALUES (:id, :customer_id, :status, :reason, :placed_at)
ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, reason = EXCLUDED.reason
