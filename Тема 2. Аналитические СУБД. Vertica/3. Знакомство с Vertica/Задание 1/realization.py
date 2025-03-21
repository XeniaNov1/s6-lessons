import vertica_python

conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2025021827',       
             'password': 'aXk5eoCl5d56Mgg',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

def try_select(conn_info=conn_info):
    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        cur=conn.cursor()
        cur.execute("select 1 as a1;") 
        #cur.commit()
        res = cur.fetchall()
        return res