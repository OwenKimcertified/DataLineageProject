from contextlib import contextmanager
import pymysql, logging

class MasterConnector:
    def __init__(self, host, user, password, port):
        self.conn_cfg = dict(
            host=host,
            port=int(port),
            user=user,
            password=password,
            autocommit=False)
          
        self.conn = None

    @contextmanager
    def _cursor(self, db=None):
        if not self.conn.open:
            self.conn = pymysql.connect(**self.conn_cfg)
        if db:
            self.conn.select_db(db)
        cur = self.conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    @contextmanager
    def execute(self, sql, params=None, db=None):
        try:
            with self._cursor(db) as cur:
                cur.execute(sql, params)
                yield cur          
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    @contextmanager
    def executemany(self, sql, params=None, db=None):
        try:
            with self._cursor(db) as cur:
                cur.executemany(sql, params)
                yield cur           
            self.conn.commit()
        except Exception as e:
            logging.error(f"Batch transaction failed. Rolling back: {e}")
            self.conn.rollback()
            raise
        
    def close(self):
        if self.conn and self.conn.open:
            self.conn.close()
        logging.info("connection close")