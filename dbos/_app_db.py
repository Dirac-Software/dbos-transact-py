from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypedDict

import psycopg
import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from dbos._migration import get_sqlite_timestamp_expr
from dbos._serialization import Serializer

from ._error import DBOSUnexpectedStepError, DBOSWorkflowConflictIDError
from ._logger import dbos_logger
from ._schemas.application_database import ApplicationSchema
from ._sys_db import StepInfo


class TransactionResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # determined by `serialization`
    error: Optional[str]  # determined by `serialization`
    serialization: Optional[str]
    txn_id: Optional[str]
    txn_snapshot: str
    executor_id: Optional[str]
    function_name: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # determined by `serialization`
    error: Optional[str]  # determined by `serialization`
    serialization: Optional[str]


class ApplicationDatabase(ABC):

    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: Dict[str, Any],
        schema: Optional[str],
        serializer: Serializer,
    ) -> "ApplicationDatabase":
        """Factory method to create the appropriate ApplicationDatabase implementation based on URL."""
        if database_url.startswith("sqlite"):
            return SQLiteApplicationDatabase(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                schema=schema,
                serializer=serializer,
            )
        else:
            # Default to PostgreSQL for postgresql://, postgres://, or other URLs
            return PostgresApplicationDatabase(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                schema=schema,
                serializer=serializer,
            )

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        serializer: Serializer,
        schema: Optional[str],
    ):
        # Log application database connection information
        printable_url = sa.make_url(database_url).render_as_string(hide_password=True)
        dbos_logger.info(
            f"Initializing DBOS application database with URL: {printable_url}"
        )
        if not database_url.startswith("sqlite"):
            dbos_logger.info(
                f"DBOS application database engine parameters: {engine_kwargs}"
            )

        # Configure and initialize the application database
        if database_url.startswith("sqlite"):
            self.schema = None
        else:
            self.schema = schema if schema else "dbosdirac"
        ApplicationSchema.transaction_outputs.schema = schema
        self.engine = self._create_engine(database_url, engine_kwargs)
        self._engine_kwargs = engine_kwargs
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.serializer = serializer

    @abstractmethod
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a database engine specific to the database type."""
        pass

    @abstractmethod
    def run_migrations(self) -> None:
        """Run database migrations specific to the database type."""
        pass

    def destroy(self) -> None:
        self.engine.dispose()

    def record_transaction_output(
        self, session: Session, output: TransactionResultInternal
    ) -> None:
        try:
            session.execute(
                sa.insert(ApplicationSchema.transaction_outputs).values(
                    workflow_uuid=output["workflow_uuid"],
                    function_id=output["function_id"],
                    output=output["output"],
                    error=None,
                    txn_id="",
                    txn_snapshot=output["txn_snapshot"],
                    executor_id=(
                        output["executor_id"] if output["executor_id"] else None
                    ),
                    function_name=output["function_name"],
                )
            )
        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    def record_transaction_error(self, output: TransactionResultInternal) -> None:
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    sa.insert(ApplicationSchema.transaction_outputs).values(
                        workflow_uuid=output["workflow_uuid"],
                        function_id=output["function_id"],
                        output=None,
                        error=output["error"],
                        txn_id="",
                        txn_snapshot=output["txn_snapshot"],
                        executor_id=(
                            output["executor_id"] if output["executor_id"] else None
                        ),
                        function_name=output["function_name"],
                    )
                )
        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    @staticmethod
    def check_transaction_execution(
        session: Session, workflow_id: str, function_id: int, function_name: str
    ) -> Optional[RecordedResult]:
        rows = session.execute(
            sa.select(
                ApplicationSchema.transaction_outputs.c.output,
                ApplicationSchema.transaction_outputs.c.error,
                ApplicationSchema.transaction_outputs.c.function_name,
            ).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == workflow_id,
                ApplicationSchema.transaction_outputs.c.function_id == function_id,
            )
        ).all()
        if len(rows) == 0:
            return None
        output, error, recorded_function_name = rows[0][0], rows[0][1], rows[0][2]
        if function_name != recorded_function_name:
            raise DBOSUnexpectedStepError(
                workflow_id=workflow_id,
                step_id=function_id,
                expected_name=function_name,
                recorded_name=recorded_function_name,
            )
        result: RecordedResult = {
            "output": output,
            "error": error,
            "serialization": None,
        }
        return result

    def garbage_collect(
        self, cutoff_epoch_timestamp_ms: int, pending_workflow_ids: list[str]
    ) -> None:
        with self.engine.begin() as c:
            delete_query = sa.delete(ApplicationSchema.transaction_outputs).where(
                ApplicationSchema.transaction_outputs.c.created_at
                < cutoff_epoch_timestamp_ms
            )

            if len(pending_workflow_ids) > 0:
                delete_query = delete_query.where(
                    ~ApplicationSchema.transaction_outputs.c.workflow_uuid.in_(
                        pending_workflow_ids
                    )
                )

            c.execute(delete_query)

    def delete_transaction_outputs(self, workflow_ids: list[str]) -> None:
        """Delete transaction outputs for the specified workflows."""
        with self.engine.begin() as c:
            c.execute(
                sa.delete(ApplicationSchema.transaction_outputs).where(
                    ApplicationSchema.transaction_outputs.c.workflow_uuid.in_(
                        workflow_ids
                    )
                )
            )

    @abstractmethod
    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation."""
        pass

    @abstractmethod
    def _is_serialization_error(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a serialization/concurrency error."""
        pass


class PostgresApplicationDatabase(ApplicationDatabase):
    """PostgreSQL-specific implementation of ApplicationDatabase."""

    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a PostgreSQL engine."""
        app_db_url = sa.make_url(database_url).set(drivername="postgresql+psycopg")

        if engine_kwargs is None:
            engine_kwargs = {}

        return sa.create_engine(
            app_db_url,
            **engine_kwargs,
        )

    def run_migrations(self) -> None:
        # Check if the database exists
        app_db_url = self.engine.url
        try:
            postgres_db_engine = sa.create_engine(
                app_db_url.set(database="postgres"),
                **self._engine_kwargs,
            )
            with postgres_db_engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT")
                if not conn.execute(
                    sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                    parameters={"db_name": app_db_url.database},
                ).scalar():
                    conn.execute(sa.text(f"CREATE DATABASE {app_db_url.database}"))
        except Exception:
            dbos_logger.warning(
                f"Could not connect to postgres database to verify existence of {app_db_url.database}. Continuing..."
            )
        finally:
            postgres_db_engine.dispose()

        # Rename old "dbos" schema to "dbosdirac" if applicable
        with self.engine.begin() as conn:
            conn.execute(
                sa.text("""
                    DO $$ BEGIN
                      IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dbos')
                        AND NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dbosdirac') THEN
                        ALTER SCHEMA "dbos" RENAME TO "dbosdirac";
                      END IF;
                    END $$;
                """)
            )

        # Create the schema and transaction_outputs table in the application database
        with self.engine.begin() as conn:
            # Check if schema exists first
            schema_exists = conn.execute(
                sa.text(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema_name"
                ),
                parameters={"schema_name": self.schema},
            ).scalar()

            if not schema_exists:
                schema_creation_query = sa.text(f'CREATE SCHEMA "{self.schema}"')
                conn.execute(schema_creation_query)

        # Convert transaction_outputs to partitioned table or create fresh
        with self.engine.begin() as conn:
            conn.execute(
                sa.text(f"""
                    DO $$ BEGIN
                      IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{self.schema}' AND tablename = 'transaction_outputs')
                        AND NOT EXISTS (SELECT 1 FROM pg_partitioned_table pt
                                        JOIN pg_class c ON pt.partrelid = c.oid
                                        JOIN pg_namespace n ON c.relnamespace = n.oid
                                        WHERE n.nspname = '{self.schema}' AND c.relname = 'transaction_outputs') THEN
                        -- Existing non-partitioned table: rename, create partitioned, migrate data, drop old
                        ALTER TABLE "{self.schema}".transaction_outputs RENAME TO transaction_outputs_old;

                        CREATE TABLE "{self.schema}".transaction_outputs (
                            workflow_uuid TEXT NOT NULL,
                            function_id INT4 NOT NULL,
                            output TEXT,
                            error TEXT,
                            txn_id TEXT,
                            txn_snapshot TEXT,
                            executor_id TEXT,
                            function_name TEXT NOT NULL DEFAULT '',
                            created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
                            PRIMARY KEY (workflow_uuid, function_id)
                        ) PARTITION BY RANGE (workflow_uuid);

                        CREATE TABLE "{self.schema}".transaction_outputs_default PARTITION OF "{self.schema}".transaction_outputs DEFAULT;

                        CREATE INDEX transaction_outputs_created_at_index ON "{self.schema}".transaction_outputs (created_at);

                        INSERT INTO "{self.schema}".transaction_outputs SELECT * FROM "{self.schema}".transaction_outputs_old;
                        DROP TABLE "{self.schema}".transaction_outputs_old CASCADE;

                      ELSIF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{self.schema}' AND tablename = 'transaction_outputs') THEN
                        -- Fresh install: create partitioned table directly
                        CREATE TABLE "{self.schema}".transaction_outputs (
                            workflow_uuid TEXT NOT NULL,
                            function_id INT4 NOT NULL,
                            output TEXT,
                            error TEXT,
                            txn_id TEXT,
                            txn_snapshot TEXT,
                            executor_id TEXT,
                            function_name TEXT NOT NULL DEFAULT '',
                            created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
                            PRIMARY KEY (workflow_uuid, function_id)
                        ) PARTITION BY RANGE (workflow_uuid);

                        CREATE TABLE "{self.schema}".transaction_outputs_default PARTITION OF "{self.schema}".transaction_outputs DEFAULT;

                        CREATE INDEX transaction_outputs_created_at_index ON "{self.schema}".transaction_outputs (created_at);
                      ELSE
                        -- Already partitioned, check for missing columns
                        IF NOT EXISTS (
                          SELECT 1 FROM information_schema.columns
                          WHERE table_schema = '{self.schema}' AND table_name = 'transaction_outputs' AND column_name = 'function_name'
                        ) THEN
                          ALTER TABLE "{self.schema}".transaction_outputs ADD COLUMN function_name TEXT NOT NULL DEFAULT '';
                        END IF;
                      END IF;
                    END $$;
                """)
            )

        # Create or replace maintain_partitions procedure for app DB.
        # Use a separate transaction so failure doesn't affect the main migration.
        try:
            with self.engine.begin() as proc_conn:
                proc_conn.execute(
                    sa.text(f"""
                        CREATE OR REPLACE PROCEDURE "{self.schema}".maintain_partitions()
                        LANGUAGE plpgsql AS $proc$
                        DECLARE
                            week_start TIMESTAMP WITH TIME ZONE;
                            week_end TIMESTAMP WITH TIME ZONE;
                            week_label TEXT;
                            uuid_lower TEXT;
                            uuid_upper TEXT;
                            sched_lower TEXT;
                            sched_upper TEXT;
                            epoch_ms_lower BIGINT;
                            epoch_ms_upper BIGINT;
                            hex_lower TEXT;
                            hex_upper TEXT;
                            part_name TEXT;
                            i INTEGER;
                        BEGIN
                            FOR i IN -1..4 LOOP
                                week_start := date_trunc('week', now()) + (i * INTERVAL '1 week');
                                week_end := week_start + INTERVAL '1 week';
                                week_label := to_char(week_start, 'IYYY') || 'w' || lpad(to_char(week_start, 'IW'), 2, '0');

                                epoch_ms_lower := (EXTRACT(epoch FROM week_start) * 1000)::BIGINT;
                                epoch_ms_upper := (EXTRACT(epoch FROM week_end) * 1000)::BIGINT;
                                hex_lower := lpad(to_hex(epoch_ms_lower), 12, '0');
                                hex_upper := lpad(to_hex(epoch_ms_upper), 12, '0');
                                uuid_lower := substring(hex_lower from 1 for 8) || '-' || substring(hex_lower from 9 for 4) || '-7';
                                uuid_upper := substring(hex_upper from 1 for 8) || '-' || substring(hex_upper from 9 for 4) || '-7';

                                sched_lower := 'sched-' || to_char(week_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS');
                                sched_upper := 'sched-' || to_char(week_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS');

                                -- UUIDv7 partition
                                part_name := 'transaction_outputs_' || week_label || '_uuid';
                                IF NOT EXISTS (
                                    SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                                    WHERE n.nspname = '{self.schema}' AND c.relname = part_name
                                ) THEN
                                    EXECUTE format(
                                        'CREATE TABLE "{self.schema}".%I PARTITION OF "{self.schema}".transaction_outputs FOR VALUES FROM (%L) TO (%L)',
                                        part_name, uuid_lower, uuid_upper
                                    );
                                END IF;

                                -- sched-* partition
                                part_name := 'transaction_outputs_' || week_label || '_sched';
                                IF NOT EXISTS (
                                    SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                                    WHERE n.nspname = '{self.schema}' AND c.relname = part_name
                                ) THEN
                                    EXECUTE format(
                                        'CREATE TABLE "{self.schema}".%I PARTITION OF "{self.schema}".transaction_outputs FOR VALUES FROM (%L) TO (%L)',
                                        part_name, sched_lower, sched_upper
                                    );
                                END IF;
                            END LOOP;
                        END;
                        $proc$;
                    """)
                )
        except Exception:
            # If the role doesn't have CREATE privilege, the procedure
            # should already exist from migration. Just log and continue.
            dbos_logger.debug(
                "Could not create maintain_partitions procedure (may already exist from migration)"
            )

        # Call maintain_partitions to create/extend partitions
        with self.engine.begin() as maint_conn:
            maint_conn.execute(sa.text(f'CALL "{self.schema}".maintain_partitions()'))

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in PostgreSQL."""
        return dbapi_error.orig.sqlstate == "23505"  # type: ignore

    def _is_serialization_error(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a serialization/concurrency error in PostgreSQL."""
        # 40001: serialization_failure (MVCC conflict)
        # 40P01: deadlock_detected
        driver_error = dbapi_error.orig
        return (
            driver_error is not None
            and isinstance(driver_error, psycopg.OperationalError)
            and driver_error.sqlstate in ("40001", "40P01")
        )


class SQLiteApplicationDatabase(ApplicationDatabase):
    """SQLite-specific implementation of ApplicationDatabase."""

    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a SQLite engine."""
        # TODO: Make the schema dynamic so this isn't needed
        ApplicationSchema.transaction_outputs.schema = None
        sqlite_kwargs = engine_kwargs.copy()
        connect_args = sqlite_kwargs.get("connect_args", {})
        if connect_args:
            filtered_keys = [
                k for k in connect_args if k in ("application_name", "connect_timeout")
            ]
            if filtered_keys:
                dbos_logger.debug(
                    f"Ignoring PostgreSQL-specific connect_args for SQLite: {filtered_keys}"
                )
            sqlite_connect_args = {
                k: v
                for k, v in connect_args.items()
                if k not in ("application_name", "connect_timeout")
            }
            sqlite_kwargs["connect_args"] = sqlite_connect_args
        return sa.create_engine(database_url, **sqlite_kwargs)

    def run_migrations(self) -> None:
        with self.engine.begin() as conn:
            # Check if table exists
            result = conn.execute(
                sa.text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='transaction_outputs'"
                )
            ).fetchone()

            if result is None:
                conn.execute(
                    sa.text(
                        f"""
                        CREATE TABLE transaction_outputs (
                            workflow_uuid TEXT NOT NULL,
                            function_id INTEGER NOT NULL,
                            output TEXT,
                            error TEXT,
                            txn_id TEXT,
                            txn_snapshot TEXT NOT NULL,
                            executor_id TEXT,
                            function_name TEXT NOT NULL DEFAULT '',
                            created_at BIGINT NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
                            PRIMARY KEY (workflow_uuid, function_id)
                        )
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        "CREATE INDEX transaction_outputs_created_at_index ON transaction_outputs (created_at)"
                    )
                )

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in SQLite."""
        return "UNIQUE constraint failed" in str(dbapi_error.orig)

    def _is_serialization_error(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a serialization/concurrency error in SQLite."""
        # SQLite database is locked or busy errors
        error_msg = str(dbapi_error.orig).lower()
        return (
            "database is locked" in error_msg or "database table is locked" in error_msg
        )
