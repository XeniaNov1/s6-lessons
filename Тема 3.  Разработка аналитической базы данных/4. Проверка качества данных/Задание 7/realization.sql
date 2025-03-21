(SELECT count(g.admin_id), 'missing group admin info' as info
FROM STV2025021827__STAGING.groups g
join STV2025021827__STAGING.users u on g.admin_id = u.id
WHERE u.id is null)
UNION ALL
(SELECT COUNT(d.message_from), 'missing sender info'
FROM STV2025021827__STAGING.dialogs d
join STV2025021827__STAGING.users u on d.message_from = u.id
WHERE u.id is null)
UNION ALL
(SELECT COUNT(d.message_to), 'missing receiver info'
FROM STV2025021827__STAGING.dialogs d
join STV2025021827__STAGING.users u on d.message_to = u.id
WHERE u.id is null)
UNION ALL 
(SELECT COUNT(1), 'norm receiver info'
FROM STV2025021827__STAGING.dialogs d
join STV2025021827__STAGING.users u on d.message_to = u.id
WHERE u.id is not null);