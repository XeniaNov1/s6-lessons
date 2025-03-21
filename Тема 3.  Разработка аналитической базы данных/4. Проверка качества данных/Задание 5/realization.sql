(SELECT 
    max(u.registration_dt) < now() as 'no future dates',
    min(u.registration_dt) >= '2020-09-03' as 'no false-start dates',
    'users' as dataset
FROM STV2025021827__STAGING.users u)
UNION ALL
(SELECT
    max(g.registration_dt) < now() as 'no future groups',
    min(g.registration_dt) >= '2020-09-03' as 'no false-start groups',
    'groups'
FROM STV2025021827__STAGING.groups g)
UNION ALL
(SELECT
    max(d.message_ts) < now() as 'no future dialogs',
    min(d.message_ts) >= '2020-09-03' as 'no false-start dialogs',
    'dialogs'
FROM STV2025021827__STAGING.dialogs d);