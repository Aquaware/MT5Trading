

import psycopg2
import pytz

class Structure:
    def __init__(self, name, primary_keys, column_dic):
        self.name = name
        self.column_dic = {}
        self.column_dic['id'] = 'serial'
        self.column_dic.update(column_dic)
        columns = list(column_dic.keys())
        self.columns = columns
        self.all_columns = ['id'] + columns
        self.primary_keys = primary_keys
        self.primary_key_indices = []
        for i in range(len(columns)):
            for primary_key in primary_keys:
                if columns[i] == primary_key:
                    self.primary_key_indices.append(i)
        pass
    
    def typeOf(self, column):
        return self.column_dic[column]
    
    def createSql(self, ignore_primary_keys=False):
        s = "CREATE TABLE IF NOT EXISTS " + self.name + " (id serial,"
        for column in self.columns:
            s += column + " " + self.typeOf(column) + ","
        s = s[0:-1]
        if len(self.primary_keys) > 0 and ignore_primary_keys==False:
            s += ", PRIMARY KEY (" 
            for primary_key in self.primary_keys:
                s += primary_key + ","
            s = s[0:-1]
            s += ")"
        s += " )"
        return s
# ------
            
    
class Postgres(object):
    
    def __init__(self, dbname, user, password, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
        pass
        
    def connect(self):
        try:
            statement = "dbname='" + self.dbname + "' user='" + self.user + "' password='" + self.password + "' port=" + self.port
            connection = psycopg2.connect(statement)
            return connection
        except psycopg2.Error as e:
            print('Connection Error ', statement)
            print(e.diag.message_detail)
            print(e.pgerror)
            return None
                
    def sql(self, statement):
        conn = self.connect()
        if conn is None:
            return False
        try:
            cur = conn.cursor()
            cur.execute(statement)
            #cur.fetchall()
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            print('SQL Error ', statement)
            self.debug(e)
            return False
        
    def create(self, table, ignore_primary_keys=False):
        return self.sql(table.createSql(ignore_primary_keys=ignore_primary_keys))
            
    def update(self, table, values):
        try:
            condition = {}
            for (primary_key, primary_key_index) in zip(table.primary_keys, table.primary_key_indices):
                condition[primary_key] = values[primary_key_index]
            v = self.fetch(table, condition)
            if len(v) == 0:
                return self.insert(table, [values])
            else:
                idno = v[0]
                s = "UPDATE " + table.name + " SET " 
                for i in range(len(table.columns)):
                    if values[i] is not None:
                        column = table.columns[i]
                        is_primary = False
                        for index in table.primary_key_indices:
                            if i == index:
                                is_primary = True
                                break
                        if is_primary == False:
                            s += column + "='" + str(values[i]) + "' ,"
                s = s[0:-1]
                s += self.whereStatement(condition)
                self.sql(s)
                return True
        except:
            return False
    
    def cursor(self):
        con = self.connect()
        if con is None:
            return (None, None)
        cursor = con.cursor()
        return (con, cursor)
    
    def insert(self, table, value_list):
        con, cursor = self.cursor()
        if cursor is None:
            return False
        
        if len(value_list) == 0:
            return False
        
        for value in value_list:
            try:
                d = []
                s = " VALUES("
                for v in value:
                    d.append(str(v))
                    s += "%s,"
                s = s[0:-1]
                s += ")"
                    
                param = table.name + "("
                for column in table.columns:
                    param += (column + ",")
                param = param[0:-1]
                param += ") "
                cursor.execute( "INSERT INTO " +  param + s , d)
            except psycopg2.IntegrityError as e:            
                #print( '=== エラー　一意性制約違反===')
                #print( 'type:' + str(type(e)))
                #print( 'args:' + str(e.args))
                #print( ':' + str(e))
                con.commit()
                con.close()
                con, cursor = self.cursor()
                if cursor is None:
                    return False
                continue
            except Exception as e:
                self.debug(e)
                continue
        con.commit()
        con.close()
        return True
    
    def debug(self, e):
        print('=== エラー　その他===')
        print( 'type:' + str(type(e)))
        print( 'args:' + str(e.args))
        print( 'e:' + str(e))
        pass
    
    def whereStatement(self, where):
        s = " WHERE "
        keys = list(where.keys())
        for key in keys:
            s += key + " = '" + str(where[key]) + "' AND "
        s = s[0:-4]    
        return s
    
    def fetch(self, table, where = None):
        con = self.connect()
        if con is None:
            return []
        
        cursor = con.cursor()
        try:
            s = "SELECT * FROM " + table.name 
            if where is not None:
                s += self.whereStatement(where)
            cursor.execute(s)
            value = cursor.fetchall()
            con.commit()
            con.close()
            return value
        except Exception as e:
            self.debug(e)
            return []
        
    def fetchAll(self, table, asc_order_column):
        con = self.connect()
        if con is None:
            return None
        cursor = con.cursor()
        try:
            s = "SELECT * FROM " + table.name
            if asc_order_column is not None:
                s += " ORDER BY " + asc_order_column + " ASC"
            cursor.execute(s)
            values = cursor.fetchall()
            con.commit()
            con.close()
            out = []
            for value in values:
                d = []
                for i in range(1, len(value)):
                    d.append(value[i])
                out.append(d)
            return out
        except:
            con.close()
            return None
        
    def fetchItemsWhere(self, table, where_statement, asc_order_column):
        con = self.connect()
        if con is None:
            return None
        cursor = con.cursor()
        try:
            s = "SELECT * FROM " + table.name
            if where_statement is not None:
                if len(where_statement) > 0:
                    s += " WHERE " + where_statement
            if asc_order_column is not None:
                s += " ORDER BY " + asc_order_column + " ASC"
            cursor.execute(s)
            values = cursor.fetchall()
            con.commit()
            con.close()
            out = []
            for value in values:
                d = []
                for i in range(1, len(value)):
                    d.append(value[i])
                out.append(d)
            return out
        except Exception as e:
            self.debug(e)
            con.close()
            return None        
        
    def remove(self, table_name):
        s = 'DROP TABLE IF EXISTS ' + table_name
        self.sql(s)
        return
    
    def isTable(self, table_name):
        con = self.connect()
        if con is None:
            return False
        cursor = con.cursor()
        try:
            s = "SELECT * from " + table_name
            cursor.execute(s)
            value = cursor.fetchall()
            con.commit()
            con.close()
            return True
        except Exception as e:
            #self.debug(e)
            return False
        
    def time2pyTime(self, time_list):
        time = []
        for t in time_list:
            #t0 = datetime.datetime.strptime(tstr, TIME_FORMAT)
            t1 = t.astimezone(pytz.timezone('Asia/Tokyo'))
            time.append(t1)
        return time
        
# -----
def deleteAll(dbname, table_name_list):
    db = Postgres(dbname)
    for table_name in table_name_list:
        db.remove(table_name)
    return    
# -----
    
if __name__ == '__main__':
    db = Postgres('XMMarket', 'postgres', 'bakabon', '5433')
    r = db.isTable('manage1')     