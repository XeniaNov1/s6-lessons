SELECT count(g.id)
FROM STV2025021827__STAGING.groups AS g 
join STV2025021827__STAGING.users AS u on g.admin_id=u.id
WHERE u.id is null