select * from users;
delete from users;

select * from orders;
delete from orders;


SELECT coalesce(MAX(user_id), 0) 
  FROM users;
 
explain
select email
  from users
  where last_name = 'Smith'
  

create index indx_users_last_name ON users(last_name)
DROP INDEX IF EXISTS indx_users_last_name CASCADE;

SELECT * FROM users WHERE age > 30;
CREATE INDEX idx_age ON users (age);

DROP INDEX IF EXISTS idx_age CASCADE;


SELECT email, age
from users
	join orders
		on users.user_id = orders.user_id
where age = 25













