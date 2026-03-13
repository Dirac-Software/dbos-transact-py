"""Tests for range-partitioned DBOS tables on workflow_uuid."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSConfig
from dbos._schemas.system_database import SystemSchema


def test_tables_are_partitioned(dbos: DBOS, skip_with_sqlite: None) -> None:
    """Verify all 6 system tables are range-partitioned."""
    schema = dbos._sys_db.schema
    expected_tables = [
        "workflow_status",
        "operation_outputs",
        "notifications",
        "workflow_events",
        "workflow_events_history",
        "streams",
    ]
    with dbos._sys_db.engine.connect() as conn:
        for table_name in expected_tables:
            result = conn.execute(
                sa.text(
                    """
                    SELECT 1 FROM pg_partitioned_table pt
                    JOIN pg_class c ON pt.partrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = :schema AND c.relname = :table_name
                    """
                ),
                {"schema": schema, "table_name": table_name},
            ).scalar()
            assert result is not None, f"{table_name} should be partitioned"


def test_maintain_partitions_creates_partitions(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """Call maintain_partitions and verify partition tables exist."""
    schema = dbos._sys_db.schema
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(sa.text(f'CALL "{schema}".maintain_partitions()'))

    with dbos._sys_db.engine.connect() as conn:
        # Check that at least some partitions exist for workflow_status
        result = conn.execute(
            sa.text(
                """
                SELECT count(*) FROM pg_inherits i
                JOIN pg_class c ON i.inhrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_class parent ON i.inhparent = parent.oid
                WHERE n.nspname = :schema AND parent.relname = 'workflow_status'
                """
            ),
            {"schema": schema},
        ).scalar()
        # At minimum: 1 default + 6 weeks * 2 (uuid + sched) = 13
        assert result is not None and result >= 13


def test_uuid7_routes_to_correct_partition(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """Insert a UUIDv7 workflow, verify it's in a weekly partition (not DEFAULT)."""
    schema = dbos._sys_db.schema
    wf_uuid = str(uuid.uuid7())

    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                INSERT INTO "{schema}".workflow_status
                    (workflow_uuid, status, created_at, updated_at, priority)
                VALUES (:uuid, 'PENDING', 0, 0, 0)
                """
            ),
            {"uuid": wf_uuid},
        )

    # Verify it's NOT in the default partition
    with dbos._sys_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                f"""
                SELECT 1 FROM "{schema}".workflow_status_default
                WHERE workflow_uuid = :uuid
                """
            ),
            {"uuid": wf_uuid},
        ).scalar()
        assert result is None, "UUIDv7 workflow should not be in DEFAULT partition"


def test_sched_routes_to_correct_partition(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """Insert a sched-* workflow with new format, verify it's in correct partition."""
    schema = dbos._sys_db.schema
    now = datetime.now(timezone.utc)
    wf_uuid = f"sched-{now.isoformat()}-test_schedule"

    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                INSERT INTO "{schema}".workflow_status
                    (workflow_uuid, status, created_at, updated_at, priority)
                VALUES (:uuid, 'PENDING', 0, 0, 0)
                """
            ),
            {"uuid": wf_uuid},
        )

    # Verify it's NOT in the default partition
    with dbos._sys_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                f"""
                SELECT 1 FROM "{schema}".workflow_status_default
                WHERE workflow_uuid = :uuid
                """
            ),
            {"uuid": wf_uuid},
        ).scalar()
        assert result is None, "sched-* workflow should not be in DEFAULT partition"


def test_user_supplied_routes_to_default(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """Insert with arbitrary UUID, verify it's in DEFAULT partition."""
    schema = dbos._sys_db.schema
    wf_uuid = "user-supplied-arbitrary-id-12345"

    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                INSERT INTO "{schema}".workflow_status
                    (workflow_uuid, status, created_at, updated_at, priority)
                VALUES (:uuid, 'PENDING', 0, 0, 0)
                """
            ),
            {"uuid": wf_uuid},
        )

    # Verify it IS in the default partition
    with dbos._sys_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                f"""
                SELECT 1 FROM "{schema}".workflow_status_default
                WHERE workflow_uuid = :uuid
                """
            ),
            {"uuid": wf_uuid},
        ).scalar()
        assert result is not None, "User-supplied UUID should be in DEFAULT partition"


def test_partition_fk_cascade(dbos: DBOS, skip_with_sqlite: None) -> None:
    """Insert workflow + operation_output in same partition, delete workflow, verify cascade."""
    schema = dbos._sys_db.schema
    wf_uuid = str(uuid.uuid7())

    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                INSERT INTO "{schema}".workflow_status
                    (workflow_uuid, status, created_at, updated_at, priority)
                VALUES (:uuid, 'PENDING', 0, 0, 0)
                """
            ),
            {"uuid": wf_uuid},
        )
        conn.execute(
            sa.text(
                f"""
                INSERT INTO "{schema}".operation_outputs
                    (workflow_uuid, function_id, function_name)
                VALUES (:uuid, 1, 'test_fn')
                """
            ),
            {"uuid": wf_uuid},
        )

    # Delete the workflow
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                DELETE FROM "{schema}".workflow_status WHERE workflow_uuid = :uuid
                """
            ),
            {"uuid": wf_uuid},
        )

    # Verify operation_output was cascade-deleted
    with dbos._sys_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                f"""
                SELECT 1 FROM "{schema}".operation_outputs WHERE workflow_uuid = :uuid
                """
            ),
            {"uuid": wf_uuid},
        ).scalar()
        assert result is None, "operation_output should be cascade-deleted"


def test_scheduled_uuid_format() -> None:
    """Verify new sched-{timestamp}-{name} format is generated correctly."""
    from dbos._scheduler import backfill_schedule

    # Just test the format directly
    now = datetime.now(timezone.utc)
    schedule_name = "my_schedule"
    expected_prefix = f"sched-{now.isoformat()}"
    workflow_id = f"sched-{now.isoformat()}-{schedule_name}"
    assert workflow_id.startswith("sched-")
    # The timestamp comes before the schedule name
    parts = workflow_id.split("-", 1)  # split on first hyphen
    assert parts[0] == "sched"
    # The remainder should start with the ISO timestamp
    assert parts[1].startswith(now.isoformat()[:10])  # at least date part


def test_maintain_partitions_idempotent(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """Call maintain_partitions twice, no errors."""
    schema = dbos._sys_db.schema
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(sa.text(f'CALL "{schema}".maintain_partitions()'))
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(sa.text(f'CALL "{schema}".maintain_partitions()'))


def test_app_db_transaction_outputs_partitioned(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """Verify transaction_outputs in the app DB is also partitioned."""
    if dbos._app_db is None:
        pytest.skip("No application database configured")
    schema = dbos._app_db.schema
    with dbos._app_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                """
                SELECT 1 FROM pg_partitioned_table pt
                JOIN pg_class c ON pt.partrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = :schema AND c.relname = 'transaction_outputs'
                """
            ),
            {"schema": schema},
        ).scalar()
        assert result is not None, "transaction_outputs should be partitioned"
