-- :name find-customer :optional
-- :doc Loads one customer by id.
-- :type id CustomerId
SELECT id, name, email, registered_at
FROM customers
WHERE id = :id

-- :name save-customer :affected
-- :doc Inserts a customer, or replaces the stored one with the same id.
-- :type id CustomerId
INSERT INTO customers (id, name, email, registered_at)
VALUES (:id, :name, :email, :registered_at)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, registered_at = EXCLUDED.registered_at
