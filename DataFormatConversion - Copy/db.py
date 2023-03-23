# import sqlite3
# conn = sqlite3.connect('meta.db')
# cursor = conn.cursor()

# def createMetaTable():
#     conn.execute('''
#     CREATE TABLE IF NOT EXISTS META
#     (
#         id integer primary key,
#         name text,
#         value real 
#     )
#     ''')
#     conn.commit()

# def singleInsertIntoMetaTable(id, name, value):
#     data = {"id": id, "name": name, "value": value}
#     conn.execute('''
#     INSERT INTO META (id,name,value) VALUES (:id,:name,:value)
#     ON CONFLICT(id) DO UPDATE SET
#     name=:name, value=:value
#     WHERE id=:id
#     ''', data)

#     conn.commit()

# def getMetaData(id):
#     cursor.execute('''
#     SELECT name, value from META where id = ?    
#     ''',(id,))
#     return cursor.fetchone()

# #test
# createMetaTable()
# singleInsertIntoMetaTable(204605,"female","1.5")
# singleInsertIntoMetaTable(204605,"female333","1.5")
# record = getMetaData(204605)
# print(record)

# conn.close()




import sqlite3
conn = sqlite3.connect('meta.db')

def createMetaTable():
    with conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS META
        (
            id integer primary key,
            name text,
            value real 
        )
        ''')

#With the with statement, you don't need to explicitly call commit() or close() on the connection, 
# as these actions are automatically handled by the context manager.


def singleInsertIntoMetaTable(id, name, value):
    data = (id, name, value)
    with conn:
        conn.execute('''
        INSERT INTO META (id,name,value) VALUES (?,?,?)
        ON CONFLICT(id) DO UPDATE SET
        name=?, value=?
        WHERE id=?
        ''', data)

def getMetaData(id):
    with conn:
        cursor = conn.execute('SELECT name, value from META where id = ?',(id,))
        return cursor.fetchall()

#test
createMetaTable()
singleInsertIntoMetaTable(204605,"female","1.5")
singleInsertIntoMetaTable(204605,"female333","1.5")
record = getMetaData(204605)
print(record)