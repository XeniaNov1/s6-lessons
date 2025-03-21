SET SESSION AUTOCOMMIT TO off;

DELETE FROM stv2025021827.members WHERE age > 45;

SELECT node_name, projection_name, deleted_row_count FROM DELETE_VECTORS
	where node_name like 'users%';

SELECT func(DELETE_VECTORS) FROM DELETE_VECTORS
	where node_name like 'users%';

ROLLBACK;