from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


BATCH_SIZE = int(os.getenv("MRA_DB_BATCH_SIZE", "2000"))


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required MariaDB setting: {name}")
    return value


def _db_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(tzinfo=None)
    if isinstance(value, np.generic):
        return value.item()
    return value


def _json_value(value: Any) -> Any:
    value = _db_value(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _iter_rows(
    frame: pd.DataFrame,
    source_columns: list[str],
    loaded_at_utc: datetime,
) -> Iterable[tuple[Any, ...]]:
    for row in frame[source_columns].itertuples(index=False, name=None):
        yield tuple(_db_value(value) for value in row) + (loaded_at_utc,)


def _table_exists(cursor, database: str, table: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (database, table),
    )
    return bool(cursor.fetchone()[0])


def _load_snapshot(
    connection,
    database: str,
    table: str,
    create_sql: str,
    insert_sql: str,
    rows: Iterable[tuple[Any, ...]],
) -> int:
    staging = f"{table}_staging"
    previous = f"{table}_previous"
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS `{staging}`")
        cursor.execute(create_sql.format(table=staging))

        batch: list[tuple[Any, ...]] = []
        inserted = 0
        for row in rows:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(insert_sql.format(table=staging), batch)
                inserted += len(batch)
                batch.clear()
        if batch:
            cursor.executemany(insert_sql.format(table=staging), batch)
            inserted += len(batch)
        connection.commit()

        cursor.execute(f"SELECT COUNT(*) FROM `{staging}`")
        staged_count = int(cursor.fetchone()[0])
        if staged_count != inserted:
            raise RuntimeError(
                f"MariaDB row verification failed for {table}: "
                f"inserted={inserted}, staged={staged_count}"
            )

        cursor.execute(f"DROP TABLE IF EXISTS `{previous}`")
        if _table_exists(cursor, database, table):
            cursor.execute(
                f"RENAME TABLE `{table}` TO `{previous}`, "
                f"`{staging}` TO `{table}`"
            )
            cursor.execute(f"DROP TABLE `{previous}`")
        else:
            cursor.execute(f"RENAME TABLE `{staging}` TO `{table}`")
        connection.commit()
        return staged_count
    finally:
        cursor.close()


FACT_CREATE_SQL = """
CREATE TABLE `{table}` (
    `spro_id` VARCHAR(191) NOT NULL,
    `audit_group` VARCHAR(20) NULL,
    `q_dimension` VARCHAR(20) NULL,
    `case_num` BIGINT NOT NULL,
    `value_score` TINYINT NULL,
    `year_month_case_num` VARCHAR(191) NULL,
    `year_month` VARCHAR(20) NOT NULL,
    `role` VARCHAR(50) NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`spro_id`, `case_num`, `year_month`),
    INDEX `idx_fact_period_role` (`year_month`, `role`),
    INDEX `idx_fact_quality` (`audit_group`, `q_dimension`, `value_score`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

FACT_INSERT_SQL = """
INSERT INTO `{table}` (
    `spro_id`, `audit_group`, `q_dimension`, `case_num`, `value_score`,
    `year_month_case_num`, `year_month`, `role`, `loaded_at_utc`
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

FACT_SOURCE_COLUMNS = [
    "SProID",
    "Group",
    "QDimension",
    "CaseNum",
    "Value",
    "YearMonthCaseNum",
    "Year-Month",
    "Role",
]


ENRICHED_CREATE_SQL = """
CREATE TABLE `{table}` (
    `spro_id` VARCHAR(191) NOT NULL,
    `audit_group` VARCHAR(20) NULL,
    `q_dimension` VARCHAR(20) NULL,
    `case_num` BIGINT NOT NULL,
    `value_score` TINYINT NULL,
    `year_month_case_num` VARCHAR(191) NULL,
    `year_month` VARCHAR(20) NOT NULL,
    `role` VARCHAR(50) NULL,
    `cluster_id` VARCHAR(191) NULL,
    `hnan_year_month` VARCHAR(191) NULL,
    `hn` VARCHAR(64) NULL,
    `an` VARCHAR(64) NULL,
    `adm_datetime` DATETIME NULL,
    `discharge_datetime` DATETIME NULL,
    `doctor_code` VARCHAR(64) NULL,
    `main_icd` VARCHAR(64) NULL,
    `icd_cm_code1` VARCHAR(64) NULL,
    `patient_type` VARCHAR(64) NULL,
    `nationality` VARCHAR(128) NULL,
    `discharge_ward_name` VARCHAR(255) NULL,
    `trauma` VARCHAR(64) NULL,
    `los` DOUBLE NULL,
    `count_complete` BIGINT NULL,
    `auditor_error` BIGINT NULL,
    `question_doc_num` VARCHAR(128) NULL,
    `question_name` TEXT NULL,
    `question_group` VARCHAR(64) NULL,
    `question_details` TEXT NULL,
    `question_profession` VARCHAR(128) NULL,
    `question_role` VARCHAR(50) NULL,
    `mapped_cluster_name` VARCHAR(255) NULL,
    `mapped_cluster_group` VARCHAR(64) NULL,
    `mapped_ward` VARCHAR(255) NULL,
    `mapped_main_icd_name` TEXT NULL,
    `doctor_code_in_master` TINYINT NOT NULL DEFAULT 0,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`spro_id`, `case_num`, `year_month`),
    INDEX `idx_enriched_period_role` (`year_month`, `role`),
    INDEX `idx_enriched_patient` (`hn`, `an`),
    INDEX `idx_enriched_quality` (`q_dimension`, `value_score`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

ENRICHED_INSERT_SQL = """
INSERT INTO `{table}` (
    `spro_id`, `audit_group`, `q_dimension`, `case_num`, `value_score`,
    `year_month_case_num`, `year_month`, `role`, `cluster_id`,
    `hnan_year_month`, `hn`, `an`, `adm_datetime`, `discharge_datetime`,
    `doctor_code`, `main_icd`, `icd_cm_code1`, `patient_type`,
    `nationality`, `discharge_ward_name`, `trauma`, `los`,
    `count_complete`, `auditor_error`, `question_doc_num`, `question_name`,
    `question_group`, `question_details`, `question_profession`, `question_role`,
    `mapped_cluster_name`, `mapped_cluster_group`, `mapped_ward`,
    `mapped_main_icd_name`, `doctor_code_in_master`, `loaded_at_utc`
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
"""

ENRICHED_SOURCE_COLUMNS = [
    "SProID",
    "Group",
    "QDimension",
    "CaseNum",
    "Value",
    "YearMonthCaseNum",
    "Year-Month",
    "Role",
    "ClusterID",
    "HNANYearMonth",
    "HN",
    "AN",
    "AdmDateTime",
    "DischargeDateTime",
    "DoctorCode",
    "MainICD",
    "ICDCmCode1",
    "PatientType",
    "Nationality",
    "DischargeWardName",
    "Trauma",
    "LOS",
    "CountComplete",
    "AuditorError",
    "QuestionDocNum",
    "QuestionName",
    "QuestionGroup",
    "QuestionDetails",
    "QuestionProfession",
    "QuestionRole",
    "MappedClusterName",
    "MappedClusterGroup",
    "MappedWard",
    "MappedMainICDName",
    "DoctorCodeInMaster",
]


QUESTION_DIM_CREATE_SQL = """
CREATE TABLE `{table}` (
    `spro_id` VARCHAR(191) NOT NULL,
    `doc_num` VARCHAR(128) NULL,
    `question_name` TEXT NULL,
    `audit_group` VARCHAR(64) NULL,
    `question_details` TEXT NULL,
    `profession` VARCHAR(128) NULL,
    `role_name` VARCHAR(50) NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`spro_id`),
    INDEX `idx_question_role` (`role_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

QUESTION_DIM_INSERT_SQL = """
INSERT INTO `{table}` (
    `spro_id`, `doc_num`, `question_name`, `audit_group`,
    `question_details`, `profession`, `role_name`, `loaded_at_utc`
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

CLUSTER_DIM_CREATE_SQL = """
CREATE TABLE `{table}` (
    `cluster_id` VARCHAR(191) NOT NULL,
    `cluster_name` VARCHAR(255) NULL,
    `cluster_group` VARCHAR(64) NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

CLUSTER_DIM_INSERT_SQL = """
INSERT INTO `{table}` (`cluster_id`, `cluster_name`, `cluster_group`, `loaded_at_utc`)
VALUES (%s, %s, %s, %s)
"""

WARD_DIM_CREATE_SQL = """
CREATE TABLE `{table}` (
    `discharge_ward_name` VARCHAR(255) NOT NULL,
    `mapped_ward` VARCHAR(255) NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`discharge_ward_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

WARD_DIM_INSERT_SQL = """
INSERT INTO `{table}` (`discharge_ward_name`, `mapped_ward`, `loaded_at_utc`)
VALUES (%s, %s, %s)
"""

ICD_DIM_CREATE_SQL = """
CREATE TABLE `{table}` (
    `main_icd` VARCHAR(64) NOT NULL,
    `main_icd_name` TEXT NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`main_icd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

ICD_DIM_INSERT_SQL = """
INSERT INTO `{table}` (`main_icd`, `main_icd_name`, `loaded_at_utc`)
VALUES (%s, %s, %s)
"""

DOCTOR_DIM_CREATE_SQL = """
CREATE TABLE `{table}` (
    `doctor_code` VARCHAR(64) NOT NULL,
    `in_master` TINYINT NOT NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`doctor_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

DOCTOR_DIM_INSERT_SQL = """
INSERT INTO `{table}` (`doctor_code`, `in_master`, `loaded_at_utc`)
VALUES (%s, %s, %s)
"""

COMPLIANCE_CREATE_SQL = """
CREATE TABLE `{table}` (
    `role_name` VARCHAR(50) NOT NULL,
    `year_month` VARCHAR(20) NOT NULL,
    `total_score` DECIMAL(18,4) NULL,
    `total_audit_items` BIGINT NOT NULL,
    `scored_items` BIGINT NOT NULL,
    `unscored_items` BIGINT NOT NULL,
    `compliance_rate` DECIMAL(12,8) NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`role_name`, `year_month`),
    INDEX `idx_compliance_period` (`year_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

COMPLIANCE_INSERT_SQL = """
INSERT INTO `{table}` (
    `role_name`, `year_month`, `total_score`, `total_audit_items`,
    `scored_items`, `unscored_items`, `compliance_rate`, `loaded_at_utc`
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

CLUSTER_COMPLIANCE_CREATE_SQL = """
CREATE TABLE `{table}` (
    `cluster_id` VARCHAR(191) NOT NULL,
    `cluster_name` VARCHAR(255) NOT NULL,
    `year_month` VARCHAR(20) NOT NULL,
    `total_score` DECIMAL(18,4) NULL,
    `total_audit_items` BIGINT NOT NULL,
    `scored_items` BIGINT NOT NULL,
    `unscored_items` BIGINT NOT NULL,
    `compliance_rate` DECIMAL(12,8) NULL,
    `loaded_at_utc` DATETIME(6) NOT NULL,
    PRIMARY KEY (`cluster_id`, `year_month`),
    INDEX `idx_cluster_compliance_name` (`cluster_name`),
    INDEX `idx_cluster_compliance_period` (`year_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

CLUSTER_COMPLIANCE_INSERT_SQL = """
INSERT INTO `{table}` (
    `cluster_id`, `cluster_name`, `year_month`, `total_score`,
    `total_audit_items`, `scored_items`, `unscored_items`,
    `compliance_rate`, `loaded_at_utc`
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _load_run_metadata(connection, report: dict[str, Any], output_dir: Path) -> None:
    pipeline = report["pipeline"]
    dataset = report["dataset"]
    join = report["join"]
    readiness = report["agent_readiness"]
    run_id = pipeline["run_id"]
    generated_at = pd.to_datetime(
        pipeline["generated_at_utc"], utc=True
    ).to_pydatetime().replace(tzinfo=None)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS `mra_pipeline_runs` (
                `run_id` CHAR(36) NOT NULL,
                `generated_at_utc` DATETIME(6) NOT NULL,
                `pipeline_name` VARCHAR(128) NOT NULL,
                `pipeline_version` VARCHAR(32) NOT NULL,
                `readiness_status` VARCHAR(40) NOT NULL,
                `fact_rows` BIGINT NOT NULL,
                `enriched_rows` BIGINT NOT NULL,
                `join_coverage_rate` DECIMAL(9,6) NOT NULL,
                `failed_checks` INT NOT NULL,
                `warning_checks` INT NOT NULL,
                `quality_report_json` JSON NOT NULL,
                `loaded_at_utc` DATETIME(6) NOT NULL,
                PRIMARY KEY (`run_id`),
                INDEX `idx_pipeline_generated` (`generated_at_utc`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cursor.execute(
            """
            INSERT INTO `mra_pipeline_runs` (
                `run_id`, `generated_at_utc`, `pipeline_name`,
                `pipeline_version`, `readiness_status`, `fact_rows`,
                `enriched_rows`, `join_coverage_rate`, `failed_checks`,
                `warning_checks`, `quality_report_json`, `loaded_at_utc`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `readiness_status` = VALUES(`readiness_status`),
                `fact_rows` = VALUES(`fact_rows`),
                `enriched_rows` = VALUES(`enriched_rows`),
                `join_coverage_rate` = VALUES(`join_coverage_rate`),
                `failed_checks` = VALUES(`failed_checks`),
                `warning_checks` = VALUES(`warning_checks`),
                `quality_report_json` = VALUES(`quality_report_json`),
                `loaded_at_utc` = VALUES(`loaded_at_utc`)
            """,
            (
                run_id,
                generated_at,
                pipeline["name"],
                pipeline["version"],
                readiness["status"],
                int(dataset["fact_rows"]),
                int(dataset["enriched_rows"]),
                float(join["coverage_rate"]),
                int(readiness["failed_checks"]),
                int(readiness["warning_checks"]),
                json.dumps(report, ensure_ascii=False, default=str),
                loaded_at,
            ),
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS `mra_quality_checks` (
                `run_id` CHAR(36) NOT NULL,
                `check_id` VARCHAR(128) NOT NULL,
                `scope_name` VARCHAR(128) NULL,
                `dimension_name` VARCHAR(128) NULL,
                `check_status` VARCHAR(16) NOT NULL,
                `severity` VARCHAR(16) NOT NULL,
                `message_th` TEXT NULL,
                `evidence_json` JSON NULL,
                PRIMARY KEY (`run_id`, `check_id`),
                INDEX `idx_check_status` (`check_status`, `severity`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cursor.execute("DELETE FROM `mra_quality_checks` WHERE `run_id` = %s", (run_id,))
        cursor.executemany(
            """
            INSERT INTO `mra_quality_checks` (
                `run_id`, `check_id`, `scope_name`, `dimension_name`,
                `check_status`, `severity`, `message_th`, `evidence_json`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    run_id,
                    check["check_id"],
                    check.get("scope"),
                    check.get("dimension"),
                    check["status"],
                    check["severity"],
                    check.get("message_th"),
                    json.dumps(check.get("evidence"), ensure_ascii=False, default=str),
                )
                for check in report["checks"]
            ],
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS `mra_rejected_records` (
                `run_id` CHAR(36) NOT NULL,
                `row_number` BIGINT NOT NULL,
                `role_name` VARCHAR(50) NULL,
                `record_json` JSON NOT NULL,
                `loaded_at_utc` DATETIME(6) NOT NULL,
                PRIMARY KEY (`run_id`, `row_number`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cursor.execute("DELETE FROM `mra_rejected_records` WHERE `run_id` = %s", (run_id,))
        rejected_path = output_dir / "rejected_records.csv"
        if rejected_path.is_file():
            rejected = pd.read_csv(rejected_path)
            rejected_rows = []
            for row_number, record in enumerate(rejected.to_dict(orient="records"), start=1):
                clean_record = {key: _json_value(value) for key, value in record.items()}
                rejected_rows.append(
                    (
                        run_id,
                        row_number,
                        clean_record.get("Role"),
                        json.dumps(clean_record, ensure_ascii=False, default=str),
                        loaded_at,
                    )
                )
            if rejected_rows:
                cursor.executemany(
                    """
                    INSERT INTO `mra_rejected_records` (
                        `run_id`, `row_number`, `role_name`, `record_json`,
                        `loaded_at_utc`
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    rejected_rows,
                )
        connection.commit()
    finally:
        cursor.close()


def load_mra_to_mariadb(output_dir: str | Path) -> dict[str, Any]:
    import mysql.connector

    output_dir = Path(output_dir)
    fact_path = output_dir / "audit_fact.parquet"
    enriched_path = output_dir / "audit_enriched.parquet"
    compliance_path = output_dir / "compliance_by_profession.parquet"
    cluster_compliance_path = output_dir / "compliance_by_cluster.parquet"
    report_path = output_dir / "quality_report.json"
    dimension_paths = {
        "question": output_dir / "moi_question_dimension.parquet",
        "cluster": output_dir / "moi_cluster_dimension.parquet",
        "ward": output_dir / "moi_ward_dimension.parquet",
        "icd10": output_dir / "moi_icd10_dimension.parquet",
        "doctor": output_dir / "moi_doctor_code_dimension.parquet",
    }
    for required_path in (
        fact_path,
        enriched_path,
        compliance_path,
        cluster_compliance_path,
        report_path,
        *dimension_paths.values(),
    ):
        if not required_path.is_file():
            raise FileNotFoundError(f"Required MariaDB input not found: {required_path}")

    host = os.getenv("MRA_DB_HOST", "mariadb").strip()
    port = int(os.getenv("MRA_DB_PORT", "3306"))
    database = os.getenv("MRA_DB_NAME", "mra_analytics").strip()
    user = _required_env("MRA_DB_USER")
    password = _required_env("MRA_DB_PASSWORD")
    if not database.replace("_", "").isalnum():
        raise RuntimeError("MRA_DB_NAME may contain only letters, digits, and underscores")

    fact = pd.read_parquet(fact_path)
    enriched = pd.read_parquet(enriched_path)
    compliance = pd.read_parquet(compliance_path)
    cluster_compliance = pd.read_parquet(cluster_compliance_path)
    dimensions = {
        name: pd.read_parquet(path) for name, path in dimension_paths.items()
    }
    report = json.loads(report_path.read_text(encoding="utf-8"))
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)

    connection = mysql.connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        connection_timeout=20,
        autocommit=False,
    )
    try:
        fact_rows = _load_snapshot(
            connection,
            database,
            "mra_audit_fact",
            FACT_CREATE_SQL,
            FACT_INSERT_SQL,
            _iter_rows(fact, FACT_SOURCE_COLUMNS, loaded_at),
        )
        enriched_rows = _load_snapshot(
            connection,
            database,
            "mra_audit_enriched",
            ENRICHED_CREATE_SQL,
            ENRICHED_INSERT_SQL,
            _iter_rows(enriched, ENRICHED_SOURCE_COLUMNS, loaded_at),
        )
        question_rows = _load_snapshot(
            connection,
            database,
            "mra_question_dimension",
            QUESTION_DIM_CREATE_SQL,
            QUESTION_DIM_INSERT_SQL,
            _iter_rows(
                dimensions["question"],
                ["SProID", "DocNum", "DocName", "Group", "Details", "Profession", "Role"],
                loaded_at,
            ),
        )
        cluster_rows = _load_snapshot(
            connection,
            database,
            "mra_cluster_dimension",
            CLUSTER_DIM_CREATE_SQL,
            CLUSTER_DIM_INSERT_SQL,
            _iter_rows(
                dimensions["cluster"],
                ["ClusterID", "MappedClusterName", "MappedClusterGroup"],
                loaded_at,
            ),
        )
        ward_rows = _load_snapshot(
            connection,
            database,
            "mra_ward_dimension",
            WARD_DIM_CREATE_SQL,
            WARD_DIM_INSERT_SQL,
            _iter_rows(
                dimensions["ward"],
                ["DischargeWardName", "MappedWard"],
                loaded_at,
            ),
        )
        icd_rows = _load_snapshot(
            connection,
            database,
            "mra_icd10_dimension",
            ICD_DIM_CREATE_SQL,
            ICD_DIM_INSERT_SQL,
            _iter_rows(
                dimensions["icd10"],
                ["MainICD", "MappedMainICDName"],
                loaded_at,
            ),
        )
        doctor_rows = _load_snapshot(
            connection,
            database,
            "mra_doctor_code_dimension",
            DOCTOR_DIM_CREATE_SQL,
            DOCTOR_DIM_INSERT_SQL,
            _iter_rows(
                dimensions["doctor"],
                ["DoctorCode", "DoctorCodeInMaster"],
                loaded_at,
            ),
        )
        compliance_rows = _load_snapshot(
            connection,
            database,
            "mra_compliance_by_profession",
            COMPLIANCE_CREATE_SQL,
            COMPLIANCE_INSERT_SQL,
            _iter_rows(
                compliance,
                [
                    "Role",
                    "Year-Month",
                    "total_score",
                    "total_audit_items",
                    "scored_items",
                    "unscored_items",
                    "compliance_rate",
                ],
                loaded_at,
            ),
        )
        cluster_compliance_rows = _load_snapshot(
            connection,
            database,
            "mra_compliance_by_cluster",
            CLUSTER_COMPLIANCE_CREATE_SQL,
            CLUSTER_COMPLIANCE_INSERT_SQL,
            _iter_rows(
                cluster_compliance,
                [
                    "ClusterID",
                    "SCShortName",
                    "Year-Month",
                    "total_score",
                    "total_audit_items",
                    "scored_items",
                    "unscored_items",
                    "compliance_rate",
                ],
                loaded_at,
            ),
        )
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                CREATE OR REPLACE VIEW `mra_compliance_by_profession_overall` AS
                SELECT
                    `role_name`,
                    SUM(`total_score`) AS `total_score`,
                    SUM(`total_audit_items`) AS `total_audit_items`,
                    SUM(`scored_items`) AS `scored_items`,
                    SUM(`unscored_items`) AS `unscored_items`,
                    SUM(`total_score`) / NULLIF(SUM(`total_audit_items`), 0)
                        AS `compliance_rate`
                FROM `mra_compliance_by_profession`
                GROUP BY `role_name`
                """
            )
            cursor.execute(
                """
                CREATE OR REPLACE VIEW `mra_compliance_by_cluster_overall` AS
                SELECT
                    `cluster_id`,
                    MAX(`cluster_name`) AS `cluster_name`,
                    SUM(`total_score`) AS `total_score`,
                    SUM(`total_audit_items`) AS `total_audit_items`,
                    SUM(`scored_items`) AS `scored_items`,
                    SUM(`unscored_items`) AS `unscored_items`,
                    SUM(`total_score`) / NULLIF(SUM(`total_audit_items`), 0)
                        AS `compliance_rate`
                FROM `mra_compliance_by_cluster`
                GROUP BY `cluster_id`
                """
            )
            connection.commit()
        finally:
            cursor.close()
        _load_run_metadata(connection, report, output_dir)

        cursor = connection.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM `mra_audit_fact`")
            verified_fact_rows = int(cursor.fetchone()[0])
            cursor.execute("SELECT COUNT(*) FROM `mra_audit_enriched`")
            verified_enriched_rows = int(cursor.fetchone()[0])
        finally:
            cursor.close()

        if verified_fact_rows != len(fact) or verified_enriched_rows != len(enriched):
            raise RuntimeError("MariaDB final row-count verification failed")

        receipt = {
            "status": "LOADED",
            "database": database,
            "host": host,
            "port": port,
            "run_id": report["pipeline"]["run_id"],
            "tables": {
                "mra_audit_fact": fact_rows,
                "mra_audit_enriched": enriched_rows,
                "mra_question_dimension": question_rows,
                "mra_cluster_dimension": cluster_rows,
                "mra_ward_dimension": ward_rows,
                "mra_icd10_dimension": icd_rows,
                "mra_doctor_code_dimension": doctor_rows,
                "mra_compliance_by_profession": compliance_rows,
                "mra_compliance_by_cluster": cluster_compliance_rows,
                "mra_quality_checks": len(report["checks"]),
            },
            "loaded_at_utc": loaded_at.replace(tzinfo=timezone.utc).isoformat(),
        }
        (output_dir / "mariadb_load_receipt.json").write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(
            "MariaDB load complete: "
            f"fact={fact_rows:,}, enriched={enriched_rows:,}, "
            f"database={database}"
        )
        return receipt
    finally:
        connection.close()
