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
    
    def close(self):
        return self.conn.close()