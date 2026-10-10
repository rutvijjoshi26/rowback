\set c random(1, 50000)
INSERT INTO orders (customer_id, amount, status, note) VALUES (:c, 19.99, 'paid', 'checkout from web, gift wrap requested');
