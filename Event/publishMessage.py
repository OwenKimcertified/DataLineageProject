from abc import ABC, abstractmethod
from kafka import KafkaProducer
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LineagePublisher(ABC):
    @abstractmethod
    def publish_load_lineage(self,
                             sources: list[str],
                             destination_db: str,
                             destination_table: str,
                             operation_type: str, # 예: 'INSERT', 'MERGE', 'UPDATE'
                             details: dict = None):
        """
        테이블 생성 시 리니지 정보에 대한 메세지 발행.

        sources: 데이터 로딩의 소스 목록 (S3 경로, 다른 테이블 이름 등)
        destination_db: 대상 데이터베이스 이름
        destination_table: 대상 테이블 이름
        operation_type: 작업 유형 ('INSERT', 'MERGE' 등)
        details: 추가 상세 정보 (예: 처리된 레코드 수, 쿼리 해시)
        """
        pass

    @abstractmethod
    def close(self):
        """발행자 종료"""
        pass


class KafkaLineagePublisher(LineagePublisher):
    def __init__(self, bootstrap_servers: str, topic: str):
        self._topic = topic
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                retries=3, # 재시도 횟수 설정
                # security_protocol="SASL_SSL", # 필요에 따라 보안 설정 추가
                # sasl_mechanism="PLAIN",
                # sasl_plain_username="user",
                # sasl_plain_password="password",
            )
            logging.info(f"Kafka producer connected to {bootstrap_servers}")
        except Exception as e:
            logging.error(f"Failed to connect to Kafka: {e}")
            self._producer = None # 연결 실패 시 None으로 설정
            # 필요에 따라 예외를 다시 발생시키거나, 로딩 프로세스를 중단할 수 있음

    def publish_load_lineage(self,
                             sources: list[str],
                             destination_db: str,
                             destination_table: str,
                             operation_type: str,
                             details: dict = None):
        """
        데이터 로딩 작업에 대한 리니지 정보를 Kafka 메시지로 발행합니다.
        """
        if not self._producer:
            logging.error("Kafka producer is not initialized. Cannot publish lineage.")
            return

        lineage_event = {
            "timestamp": int(time.time() * 1000), # 밀리초 유닉스 타임스탬프
            "operation": operation_type,
            "source_type": "MIXED", # 소스 타입이 혼합될 수 있음을 표현 (S3, TABLE 등)
            "sources": sources, # 어떤 S3 파일/경로 또는 어떤 테이블에서 왔는지 목록
            "destination_type": "DW_TABLE",
            "destination_db": destination_db,
            "destination_table": destination_table,
            "details": details if details is not None else {},
            "process_id": f"data_load_job_{int(time.time())}", # 작업 식별자 (예시)
            # 추가 필드: user, service_name, status ('SUCCESS', 'FAILURE' - 여기서는 성공 시에만 발행)
        }

        try:
            # 비동기 전송, get()을 호출하여 전송 결과를 기다릴 수도 있지만,
            # 리니지는 비동기적 추적이 더 흔함
            future = self._producer.send(self._topic, value=lineage_event)
            logging.info(f"Published lineage message to topic {self._topic}")
        except Exception as e:
            logging.error(f"Failed to publish lineage message to Kafka: {e}")
            # 모니터링 

    def close(self):
        """Kafka producer를 안전하게 종료"""
        if self._producer:
            try:
                self._producer.flush() # 모든 queued 메시지가 전송될 때까지 대기
                self._producer.close()
                logging.info("Kafka producer closed.")
            except Exception as e:
                logging.error(f"Error closing Kafka producer: {e}")