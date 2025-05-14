from dotenv import load_dotenv

import os, logging

load_env = load_dotenv(override=True)

from DWDMconnector.DBMS import MasterConnector
from Event.publishMessage import KafkaLineagePublisher
from Service.DW_ETL import DataLoader

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_HOST = os.getenv("MASTER_HOST")
DB_PORT = os.getenv("MASTER_PORT")
DB_USER = os.getenv("MASTER_USER")
DB_PASS = os.getenv("MASTER_PASS")
DW_DATABASE = os.getenv("DW_DATABASE")
DW_TABLE = os.getenv("DW_TABLE")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")
LINEAGE_TOPIC = os.getenv("LINEAGE_KAFKA_TOPIC")

S3_KEY = os.getenv("AWS_ACCESS_KEY")
S3_SECRET = os.getenv("AWS_SECRET_KEY")
S3_REGION = os.getenv("REGION")
S3_BUCKET = os.getenv("BUCKET_NAME")


if not all([DB_HOST, DB_USER, DB_PASS, DW_DATABASE, DW_TABLE, KAFKA_SERVERS, LINEAGE_TOPIC]):
    logging.error("env file 확인바람.")
    exit(1)

kafka_bootstrap_servers_list = KAFKA_SERVERS

# 데이터 파싱 코드 필요.

insert_query = f"INSERT INTO {DW_DATABASE}.{DW_TABLE} (id, name, value) VALUES (%s, %s, %s)"

create_table_query = f"""
CREATE TABLE IF NOT EXISTS `{DW_DATABASE}`.`{DW_TABLE}` (
    id    INT          NOT NULL PRIMARY KEY,
    name  VARCHAR(255) NOT NULL,
    value DECIMAL(10,2)
) ENGINE = InnoDB;
"""

merge_query = f"""
INSERT INTO {DW_DATABASE}.{DW_TABLE} (id, name, value)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
name = VALUES(name), value = VALUES(value)
"""


source_files = [
    "s3 주소"
]

db_connector = None
lineage_publisher = None
data_loader = None

try:
    logging.info("DB 연결 시도..")
    db_connector = MasterConnector(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
    logging.info("DB 연결 성공")
    
    logging.info("리니지 퍼블리셔 연결 시도")
    lineage_publisher = KafkaLineagePublisher(bootstrap_servers=kafka_bootstrap_servers_list, topic=LINEAGE_TOPIC)
    logging.info("리니지 퍼블리셔 연결 성공")
    if not lineage_publisher._producer: 
         raise ConnectionError("리니지 퍼블리셔 연결 실패")

    logging.info("데이터 파싱 시도")
    if DataLoader(db_connector=db_connector,
                             lineage_publisher=lineage_publisher,
                             dw_database=DW_DATABASE):
        
        data_loader = DataLoader(db_connector=db_connector,
                                lineage_publisher=lineage_publisher,
                                dw_database=DW_DATABASE)
        logging.info("데이터 파싱 성공")
        
    else:
        logging.info("데이터 파싱 실패")

    logging.info("\n예시 배치 데이터")
    data_loader.load_and_track_lineage(
        query=create_table_query, #
        params=None, 
        destination_table=DW_TABLE,
        source_info=source_files,
        operation_type="INSERT" 
    )

    source_info_single = "s3 주소"
    data_loader.load_and_track_lineage(
        query=insert_query,
        params=None,
        destination_table=DW_TABLE,
        source_info=source_info_single,
        operation_type="INSERT"
    )


except Exception as e:
    logging.error(f"데이터 로드 중 에러 발생: {e}")
    
finally:
    logging.info("\n--- 커넥션 푸는 중 ---")
    if data_loader:
        data_loader.close()

    elif db_connector:
        db_connector.close()
        
    if lineage_publisher:
        lineage_publisher.close() 
        
    logging.info("모든 커넥션 해제")