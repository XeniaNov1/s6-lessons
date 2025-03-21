(SELECT 
    min(u.registration_dt) as datestamp,
    'earliest user registration' as info
FROM STV2025021827__STAGING.users u)
UNION ALL
(SELECT
    max(u.registration_dt),
    'latest user registration'
FROM STV2025021827__STAGING.users u)
UNION ALL
(SELECT 
    min(registration_dt),
    'earliest group creation'
    from STV2025021827__STAGING.groups)
UNION ALL
(SELECT 
    max(registration_dt),
    'latest group creation'
    from STV2025021827__STAGING.groups)
UNION ALL
(SELECT 
    min(message_ts),
    'earliest dialog message'
    from STV2025021827__STAGING.dialogs)
UNION ALL
(SELECT 
    max(message_ts),
    'latest dialog message'
    from STV2025021827__STAGING.dialogs)
;