import cx_Oracle
import pandas as pd

class Oracle:
    cursor = None
    conn = None

    def __init__(self):
        host = 'localhost'
        port = 1521
        dbname = 'xe'
        username = 'system'
        password = 'oracle'
        dsn = cx_Oracle.makedsn(
            secretDict['host'],
            secretDict['port'],
            service_name=secretDict['dbname']
        )

        self.conn = cx_Oracle.connect(
            user=secretDict['username'],
            password=secretDict['password'],
            dsn=dsn
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def consulta(self, query, with_df=True):
        self.cursor.execute(query)
        if with_df:
            result = self.cursor.fetchall()
            col_names = []
            for elt in self.cursor.description:
                col_names.append(elt[0])

            df = pd.DataFrame(result, columns=col_names)
            return(df)
        else:
            return None

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.conn.close()
