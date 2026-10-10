\set id random(1, :rows)
DELETE FROM orders WHERE id = :id;
