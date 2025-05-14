import logging
from DWDMconnector.DBMS import MasterConnector 
from Event.publishMessage import LineagePublisher
from typing import Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataLoader:
    """
    데이터 로딩 서비스: S3에서 DW로 데이터를 로딩하고 리니지 정보를 발행합니다.
    """
    def __init__(self, db_connector: MasterConnector, lineage_publisher: LineagePublisher, dw_database: str):
        """
        :param db_connector: DW (MySQL Master) 연결 객체
        :param lineage_publisher: 리니지 발행 객체
        :param dw_database: 대상 DW 데이터베이스 이름
        """
        self._db_connector = db_connector
        self._lineage_publisher = lineage_publisher
        self._dw_database = dw_database

    def load_and_track_lineage(self,
                               query,
                               params: Any,
                               destination_table,
                               source_info: list,
                               operation_type="INSERT",
                               batch_size=None):
        """
        데이터를 DW에 로드하고 성공 시 리니지 정보를 발행

        :param query: 실행할 SQL 쿼리 문자열 (예: INSERT INTO table (...) VALUES (...))
        :param params: 쿼리에 바인딩될 파라미터 값.
                           단일 쿼리: 튜플 또는 딕셔너리.
                           다중(배치) 쿼리: 튜플 또는 딕셔너리의 리스트.
        :param destination_table: 데이터를 로드할 DW 테이블 이름
        :param source_info: 데이터의 소스 정보 (S3 경로, 원본 테이블 등) 목록
        :param operation_type: 수행된 작업 유형 ('INSERT', 'MERGE', 'UPDATE' 등)
        :param batch_size: executemany 사용 시 배치 크기 (None이면 단일 execute 또는 executemany 자동 판단)
        """
        try:
            logging.info(f"Starting data load into {self._dw_database}.{destination_table} from sources: {source_info}")

            # SQL 파라미터 유형에 따라 execute 또는 executemany 결정
            if isinstance(params, list) and len(params) > 0 and isinstance(params[0], (list, tuple, dict)):
                # 리스트 형태이고 각 요소가 리스트, 튜플, 딕셔너리인 경우 -> 배치 실행
                logging.info(f"Executing batch query: {query[:10]}...")
                if batch_size and isinstance(params, list):
                     # 배치 크기 지정된 경우 분할 실행
                     for i in range(0, len(params), batch_size):
                         batch = params[i:i + batch_size]
                         with self._db_connector.executemany(query, batch, db=self._dw_database) as cursor:
                              rows_affected += cursor.rowcount
                         logging.info(f"Processed batch {int(i/batch_size)+1}. Total affected: {rows_affected}")
                else:
                    # 배치 크기 지정 안된 경우 한 번에 executemany
                    with self._db_connector.executemany(query, params, db=self._dw_database) as cursor:
                         rows_affected = cursor.rowcount
                logging.info(f"Batch execution complete. Total rows affected: {rows_affected}")
                details = {"rows_affected": rows_affected, "operation": operation_type}

            else:
                # 단일 실행
                logging.info(f"Executing single query: {query[:10]}...")
                with self._db_connector.execute(query, params, db=self._dw_database) as cursor:
                    rows_affected = cursor.rowcount if cursor else 0
                logging.info(f"Single execution complete. Rows affected: {rows_affected}")
                details = {"rows_affected": rows_affected, "operation": operation_type}


            logging.info(f"Data successfully loaded into {destination_table}.")

            # 데이터 로딩 성공 후 리니지 발행
            self._lineage_publisher.publish_load_lineage(
                sources=source_info,
                destination_db=self._dw_database,
                destination_table=destination_table,
                operation_type=operation_type,
                details=details
            )
            logging.info("Lineage message published.")

        except Exception as e:
            logging.error(f"Data load failed for {destination_table}: {e}")
            # 데이터 로드 실패 시 리니지 발행은 일반적으로 하지 않음.
            # 실패 이력 관리가 필요하면 별도의 실패 리니지 메시지를 발행할 수 있음.
            raise # 예외를 다시 발생시켜 호출자가 처리하도록 함.

    def close(self):
        """사용된 커넥터들을 종료"""
        self._db_connector.close()
        self._lineage_publisher.close()
        logging.info("Data loader resources closed.")