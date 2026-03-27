import psycopg2
conn=psycopg2.connect(dbname='quantlens', user='postgres', password='admin123', host='127.0.0.1')
cur=conn.cursor()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ml_training_data_v2';")
print("Columns in ml_training_data_v2:")
for row in cur.fetchall():
    print(row)
