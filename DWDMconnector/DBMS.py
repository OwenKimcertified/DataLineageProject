from dotenv import load_dotenv
from contextlib import contextmanager

import pymysql, os

load_env = load_dotenv()

# .env key
masterHost = os.getenv("MASTER_HOST")
masterPort = os.getenv("MASTER_PORT")
masterUser = os.getenv("MASTER_USER")
masterPass = os.getenv("MASTER_PASS")

class MasterConnector:
    def __init__(self, host=masterHost, user=masterUser, password=masterPass,port=masterPort):
        
        self.conn = pymysql.connect(
                                    host     = host,
                                    port     = int(port),         
                                    user     = user,
                                    password = password)
        
    @contextmanager
    def execute(self, sql, params=None, db=None):
        __cursor = None
        try:
            # 만약 연결이 안되어 있으면
            if not self.conn.open:
                self.conn = pymysql.connect(
                                    host     = self.conn.host,
                                    port     = self.conn.port,         
                                    user     = self.conn.user,
                                    password = self.conn.password)
            
            if db:
                self.conn.select_db(db)
            
            # 커서 생성 하고,    
            __cursor = self.conn.cursor()
            
            # 커서로 쿼리 실행 후
            __cursor.execute(sql, params)
            
            # 쿼리 결과 반환 (__enter__, __exit__ 구별)
            yield __cursor
            self.conn.commit()
        
        # ACID
        except Exception as e:
            if self.conn and self.conn.open:
                self.conn.rollback()
            raise e
        
        finally:
            if __cursor:
                __cursor.close()

    @contextmanager
    def executemany(self, sql: str, params_list: list, db: str = None):
        try:
            with self._get_cursor(db=db) as cursor:
                cursor.executemany(sql, params_list)
                yield cursor # executemany는 보통 결과를 반환하지 않지만, 일관성을 위해 yield
            self._conn.commit() # 성공 시 커밋
        except Exception as e:
            print(f"Batch transaction failed. Rolling back: {e}")
            if self._conn and self._conn.open:
                self._conn.rollback() # 실패 시 롤백
            raise e # 예외 다시 발생

    def close(self):
        self.conn.close()
        self._conn = None
        return print("connection close")