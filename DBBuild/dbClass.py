import psycopg2
import pymysql
import pandas as pd
import functools



class myDB:
    def __init__(self,dbtype,dbName,tableName="APAC Spend"):
        self.dbtype=dbtype
        if self.dbtype=='postgreSQL':
            self.host = 'localhost'
            self.database = dbName
            self.tableName=tableName
            self.user = 'postgres'
            self.password = '123asa'
            self.port=5432
        elif self.dbtype=='mySQL':
            self.host='localhost'
            self.database=dbName
            self.tableName=tableName
            self.user='root'
            self.password='123asa'
            self.port=3306
        else:
            raise ValueError("Sorry, we only support mySQL and postgreSQL now")
        self.connection = None
        self.cursor=None

    def connect(self):
        try:
            if self.connection is None:
                if self.dbtype == 'postgreSQL':
                    self.connection = psycopg2.connect(
                        host=self.host,
                        database=self.database,
                        user=self.user,
                        password=self.password,
                        port=self.port
                    )
                elif self.dbtype == 'mySQL':
                    self.connection = pymysql.connect(
                        host=self.host,
                        database=self.database,
                        user=self.user,
                        passwd=self.password,
                        port=self.port
                    )
            print(f"Successfully connected to {self.dbtype} database '{self.database}'")
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(f"Failed to connect to {self.dbtype} database '{self.database}': {e}")
    
    def db_table(func):
        @functools.wraps(func)
        def wrapper(self,*args,**kwargs):
            df = kwargs.get('df')
            sqlInsertHead = kwargs.get('sqlInsertHead')
            batch_size = kwargs.get('batch_size')
            if df is None or sqlInsertHead is None or batch_size is None:
                raise ValueError("Your function are missing paras")
            if self.dbtype=='mySQL':
                if sqlInsertHead.count('`')<2:
                    raise ValueError("Check your syntax for mySQL pls")
            elif self.dbtype=='postgreSQL':
                if sqlInsertHead.count('`')>0:
                    sqlInsertHead=sqlInsertHead.replace('`','"')
            cursor=self.cursor
            connection=self.connection

            if self.tableName=="APAC Spend":
                insertType = kwargs.get('insertType')
                if insertType is None :
                    raise ValueError("Your function are missing paras")
                if not isinstance(df, pd.DataFrame):
                    raise ValueError("The provided data must be a pandas DataFrame")
                if df.empty:
                    raise ValueError("The provided DataFrame is empty")
                APAC_SPEND_MAX_QUERY="""
                    SELECT MAX("Ordering date") FROM "APAC Spend"
                                    """
                APAC_SPEND_MIN_QUERY="""
                    SELECT MIN("Ordering date") FROM "APAC Spend"
                                    """
                source_latest_date=df['Ordering date'].max()
                source_oldest_date=df['Ordering date'].min()
                print(f"Source data lastest date is {source_latest_date}")
                print(f"Source data oldest date is {source_oldest_date}")
                query1 = """
                    DELETE FROM "APAC Spend" WHERE "Ordering date" = %s
                        """
                if self.dbtype=='mySQL':
                    APAC_SPEND_MAX_QUERY=APAC_SPEND_MAX_QUERY.replace('"','`')
                    APAC_SPEND_MIN_QUERY=APAC_SPEND_MIN_QUERY.replace('"','`')
                    query1=query1.replace('"','`')
                cursor.execute(APAC_SPEND_MAX_QUERY)
                db_latest_date=cursor.fetchone()[0]
                cursor.execute(APAC_SPEND_MIN_QUERY)
                db_oldest_date=cursor.fetchone()[0]
                if db_latest_date is None:
                    db_latest_date=pd.Timestamp('2000-01-01')
                else:
                    db_latest_date = pd.Timestamp(db_latest_date)
                if db_oldest_date is None:
                    db_oldest_date=pd.Timestamp('2099-01-01')
                else:
                    db_oldest_date = pd.Timestamp(db_oldest_date)
                print(f"DB data lastest date is {db_latest_date}")
                print(f"DB data oldest date is {db_oldest_date}")
                
                if insertType=='New':
                    if source_latest_date<db_latest_date:
                        print("No need to update")
                        return
                    elif source_oldest_date<=db_latest_date:
                        df_overmap=df[df['Ordering date'] == db_latest_date]
                        df = df[df['Ordering date'] >= db_latest_date]
                        if df_overmap is not None and not df_overmap.empty:
                            cursor.execute(query1, (db_latest_date,))
                            connection.commit()
                            print(f"Delete data where Ordering date is {db_latest_date}")
                elif insertType=='History':
                    if source_oldest_date>db_oldest_date:
                        print("No need to update")
                        return
                    elif source_latest_date>=db_oldest_date:
                        df_overmap=df[df['Ordering date'] == db_oldest_date]
                        df = df[df['Ordering date'] <= db_oldest_date]
                        if df_overmap is not None and not df_overmap.empty:
                            cursor.execute(query1, (db_oldest_date,))
                            connection.commit()
                            print(f"Delete data where Ordering date is {db_oldest_date}")
            elif self.tableName=="POA Saving Tracker":  
            #    
                if not isinstance(df, pd.DataFrame):
                    raise ValueError("The provided data must be a pandas DataFrame")
                if df.empty:
                    raise ValueError("The provided DataFrame is empty")
                POA_SAVING_POCHECK_QUERY="""
                    SELECT * FROM "POA Saving Tracker" WHERE "PO/OA number"=%s
                                    """
                if self.dbtype=='mySQL':
                    POA_SAVING_POCHECK_QUERY=POA_SAVING_POCHECK_QUERY.replace('"','`')
                
                po_to_drop=[]
                for poValue in df.iloc[:, 0]:
                    cursor.execute(POA_SAVING_POCHECK_QUERY,(poValue,))
                    result_saving=cursor.fetchone()
                    if result_saving is not None:
                        po_to_drop.append(poValue)
                df = df[~df['PO/OA number'].isin(po_to_drop)]

            data=[tuple(row) for index,row in df.iterrows()] 
            print(len(data))       
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                try:
                    if batch_size == 1:
                        for row in batch:
                            cursor.execute(sqlInsertHead, row)
                    else:
                        cursor.executemany(sqlInsertHead, batch)
                    connection.commit()
                    print(f"Batch {i // batch_size + 1} inserted successfully")
                except Exception as e:
                    print(f"Error: {e}")
                    print(f"Batch: {batch}")

            return func(self,*args,**kwargs)


        return wrapper

    @db_table
    def db_insert(self, *args, **kwargs):
        table=self.tableName
        print(f"Successfully insert the data to {table}")
    
   

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed")

