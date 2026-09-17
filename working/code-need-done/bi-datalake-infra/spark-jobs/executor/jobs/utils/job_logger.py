import psycopg2
from psycopg2 import pool
from datetime import datetime
import uuid
import os
import time


class PostgresPool:
    _pool = None

    @classmethod
    def get_pool(cls):
        if cls._pool is None:
            cls._pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                host=os.getenv("POSTGRES_HOST"),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                dbname=os.getenv("POSTGRES_DB", "datalake_ops"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                connect_timeout=5
            )
        return cls._pool
    

class JobLogger:
    def __init__(self):
        self.pool = PostgresPool.get_pool()
        self.conn = None
        self.run_id = None

    # -------------------- GET CONNECTION --------------------
    def _get_conn(self):
        if self.conn is None or self.conn.closed != 0:
            self.conn = self.pool.getconn()
            self.conn.autocommit = True
        return self.conn

    # -------------------- RELEASE --------------------
    def _release_conn(self):
        if self.conn:
            self.pool.putconn(self.conn)
            self.conn = None

    # -------------------- EXECUTE WITH RETRY --------------------
    def _execute(self, query, params=None, retry=2):
        for attempt in range(retry):
            conn = None
            try:
                conn = self.pool.getconn()
                conn.autocommit = True

                with conn.cursor() as cur:
                    cur.execute(query, params)

                return

            except Exception as e:
                print(f"⚠️ DB error (attempt {attempt+1}): {e}")
                time.sleep(1)

                if attempt == retry - 1:
                    raise

            finally:
                if conn:
                    self.pool.putconn(conn)

    # -------------------- JOB APIs --------------------
    def start_job(self, job_name):
        self.run_id = f"{job_name}_{uuid.uuid4().hex[:8]}"
        self.job_name = job_name
        print(f"Update start status {self.run_id}" )

        self._execute("""
            INSERT INTO job_control.job_runs
            (job_name, run_id, status, start_time)
            VALUES (%s, %s, %s, %s)
        """, (
            job_name,
            self.run_id,
            "RUNNING",
            datetime.now()
        ))

        return self.run_id

    def success(self):
        print(f"Update success status {self.run_id}" )
        self._execute("""
            UPDATE job_control.job_runs
            SET status = %s,
                end_time = %s,
                duration_seconds = EXTRACT(EPOCH FROM (%s - start_time))
            WHERE run_id = %s
        """, (
            "SUCCESS",
            datetime.now(),
            datetime.now(),
            self.run_id
        ))

    def fail(self, error_message):
        print(f"Update fail status {self.run_id}" )
        self._execute("""
            UPDATE job_control.job_runs
            SET status = %s,
                end_time = %s,
                error_message = %s,
                duration_seconds = EXTRACT(EPOCH FROM (%s - start_time))
            WHERE run_id = %s
        """, (
            "FAILED",
            datetime.now(),
            str(error_message)[:3000],
            datetime.now(),
            self.run_id
        ))

    def close(self):
        self._release_conn()