-- :name save-order-status :affected
-- :doc Stores an order's status unless the stored one came from a later event, so a stale re-run can't overwrite it.
INSERT INTO order_status (id, status, sequence)
VALUES (:id, :status, :sequence)
ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, sequence = EXCLUDED.sequence
WHERE order_status.sequence < EXCLUDED.sequence

-- :name find-order-status :optional
-- :doc Loads an order's projected status by order id.
SELECT id, status, sequence
FROM order_status
WHERE id = :id
