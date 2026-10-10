\set id random(1, :rows)
UPDATE orders SET status = 'shipped', amount = amount + 1, updated_at = now() WHERE id = :id;
