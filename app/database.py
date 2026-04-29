"""
AstraDB (Cassandra) – Singleton Connection & Prepared Statements
"""
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import ConsistencyLevel


class DB:
    _cluster = None
    _session = None
    _stmts: dict = {}

    @classmethod
    def connect(cls):
        bundle  = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH", "./secure-connect.zip")
        cid     = os.getenv("ASTRA_DB_CLIENT_ID")
        csecret = os.getenv("ASTRA_DB_CLIENT_SECRET")
        ks      = os.getenv("ASTRA_DB_KEYSPACE", "learning_tracker")

        auth = PlainTextAuthProvider(username=cid, password=csecret)
        cls._cluster = Cluster(
            cloud={"secure_connect_bundle": bundle},
            auth_provider=auth,
        )
        cls._session = cls._cluster.connect(ks)
        cls._session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
        cls._prepare()
        print(f"[DB] Connected to AstraDB – keyspace: {ks}")

    @classmethod
    def _prepare(cls):
        s = cls._session

        # INSERT event (ConsistencyLevel.ANY → tối đa throughput)
        stmt = s.prepare("""
            INSERT INTO learning_events
              (student_id, event_time, event_id, event_type, course_id,
               lesson_id, duration_sec, score, progress_pct, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        stmt.consistency_level = ConsistencyLevel.ANY
        cls._stmts["insert_event"] = stmt

        # SELECT history by student (Partition Key lookup)
        stmt2 = s.prepare("""
            SELECT event_time, event_id, event_type, course_id,
                   duration_sec, score, progress_pct, metadata
            FROM learning_events
            WHERE student_id = ?
              AND event_time > ?
            ORDER BY event_time DESC
            LIMIT ?
        """)
        stmt2.consistency_level = ConsistencyLevel.LOCAL_ONE
        cls._stmts["get_history"] = stmt2

        # SELECT summary
        cls._stmts["get_summary"] = s.prepare("""
            SELECT month_bucket, total_events, total_video_sec, total_quiz_done,
                   avg_quiz_score, active_days, video_skip_pct, risk_score, risk_label
            FROM student_monthly_summary
            WHERE student_id = ?
            ORDER BY month_bucket DESC
            LIMIT 6
        """)

        # UPSERT summary counters
        cls._stmts["upsert_summary"] = s.prepare("""
            UPDATE student_monthly_summary
            SET total_events    = total_events + ?,
                total_video_sec = total_video_sec + ?,
                total_quiz_done = total_quiz_done + ?,
                last_active     = ?
            WHERE student_id = ? AND month_bucket = ?
        """)

        # SELECT high risk
        cls._stmts["get_high_risk"] = s.prepare("""
            SELECT student_id, risk_score, detected_at, inactive_days,
                   video_skip_pct, recommendation
            FROM high_risk_students
            WHERE course_id = ? AND risk_label = ?
            LIMIT 50
        """)

    @classmethod
    def session(cls):
        return cls._session

    @classmethod
    def stmt(cls, name: str):
        return cls._stmts[name]

    @classmethod
    def close(cls):
        if cls._cluster:
            cls._cluster.shutdown()
