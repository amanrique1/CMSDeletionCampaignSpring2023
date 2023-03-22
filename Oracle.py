import cx_Oracle
import pandas as pd

class Oracle:
    cursor = None
    conn = None

    def __init__(self,host,port,service,username,password):
        dsn = cx_Oracle.makedsn(host,port,service_name= service)

        self.conn = cx_Oracle.connect(
            user=username,
            password=password,
            dsn=dsn
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def query(self, query, with_df=True):
        self.cursor.execute(query)
        if with_df:
            result = self.cursor.fetchall()
            cols = [i[0] for i in self.cursor.description]
            df = pd.DataFrame(result, columns=cols)

            for col in self.cursor.description:
                if col[1] == cx_Oracle.DB_TYPE_RAW:
                    df[col[0]] = df[col[0]].apply(lambda x: x.hex() if x!=None else None)
            return(df)
        else:
            return None

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.conn.close()
