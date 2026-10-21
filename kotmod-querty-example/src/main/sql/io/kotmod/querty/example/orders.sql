-- :snippet order-columns
o.id,
o.customer_id,
o.status,
o.item,
o.reason,
o.placed_at

-- :name find-order :optional
-- :doc Loads one order by id.
-- :type id OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus enum
SELECT {{order-columns}}
FROM orders o
WHERE o.id = :id

-- :name save-order :affected
-- :doc Inserts an order, or replaces the stored one with the same id.
-- :type id OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus enum
-- :nullable reason
INSERT INTO orders (id, customer_id, status, item, reason, placed_at)
VALUES (:id, :customer_id, :status, :item, :reason, :placed_at)
ON CONFLICT (id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    status = EXCLUDED.status,
    item = EXCLUDED.item,
    reason = EXCLUDED.reason,
    placed_at = EXCLUDED.placed_at

-- :name customer-orders :many
-- :doc A customer's orders, newest first, with the customer's name and email.
-- :type id OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus enum
-- :not-null customer_name
SELECT {{order-columns}}, c.name AS customer_name, c.email AS customer_email
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE o.customer_id = :customer_id
ORDER BY o.placed_at DESC, o.id

-- :name search-orders :many
-- :doc Orders filtered by status and by a substring of the item, newest first, a page at a time.
-- :type id OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus enum
-- :nullable status
-- :nullable item_contains
SELECT {{order-columns}}
FROM orders o
WHERE (:status::text IS NULL OR o.status = :status)
  AND (:item_contains::text IS NULL OR o.item ILIKE '%' || :item_contains || '%')
ORDER BY o.placed_at DESC, o.id
LIMIT :limit OFFSET :offset

-- :name orders-by-ids :many
-- :doc The orders with the given ids, in id order.
-- :type id OrderId
-- :type ids OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus enum
SELECT {{order-columns}}
FROM orders o
WHERE o.id = ANY(:ids)
ORDER BY o.id
