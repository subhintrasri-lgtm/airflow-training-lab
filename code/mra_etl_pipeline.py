from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PIPELINE_NAME = "agentic_mra_airflow_etl"
PIPELINE_VERSION = "1.3.0"

DATA_ROOT = Path(os.getenv("MRA_DATA_DIR", "/opt/airflow/data"))
INPUT_DIR = Path(os.getenv("MRA_INPUT_DIR", str(DATA_ROOT / "input")))
STAGING_DIR = Path(
    os.getenv("MRA_STAGING_DIR", str(DATA_ROOT / "staging" / "mra_agentic"))
)
OUTPUT_DIR = Path(
    os.getenv("MRA_OUTPUT_DIR", str(DATA_ROOT / "output" / "mra_agentic"))
)
AGENT_DIR = Path(os.getenv("MRA_AGENT_DIR", str(DATA_ROOT / "agent")))
ONEDRIVE_INPUT_VALUE = os.getenv("MRA_ONEDRIVE_INPUT_DIR", "").strip()
ONEDRIVE_INPUT_DIR = Path(ONEDRIVE_INPUT_VALUE) if ONEDRIVE_INPUT_VALUE else None
MOI_ONEDRIVE_INPUT_VALUE = os.getenv("MRA_MOI_ONEDRIVE_INPUT_DIR", "").strip()
MOI_ONEDRIVE_INPUT_DIR = (
    Path(MOI_ONEDRIVE_INPUT_VALUE) if MOI_ONEDRIVE_INPUT_VALUE else None
)
SUBCLUSTER_ONEDRIVE_INPUT_VALUE = os.getenv(
    "MRA_SUBCLUSTER_ONEDRIVE_INPUT_DIR", ""
).strip()
SUBCLUSTER_ONEDRIVE_INPUT_DIR = (
    Path(SUBCLUSTER_ONEDRIVE_INPUT_VALUE)
    if SUBCLUSTER_ONEDRIVE_INPUT_VALUE
    else None
)
MOI_LOCAL_INPUT_DIR = Path(os.getenv("MRA_MOI_INPUT_DIR", str(INPUT_DIR)))
REFERENCE_INPUT_DIR = Path(
    os.getenv("MRA_REFERENCE_INPUT_DIR", str(INPUT_DIR))
)
AUDIT_SNAPSHOT_DIR = STAGING_DIR / "source_snapshot"
MOI_SNAPSHOT_DIR = STAGING_DIR / "moi_reference_snapshot"
REFERENCE_FILE = "ResultMRauditJCIHA.xlsx"
MOI_REFERENCE_FILES = [
    "MRCode.xlsx",
    "HAReportJan2024-May2025.xlsm",
    "NewAuditForm2025.xlsx",
]
SUBCLUSTER_REFERENCE_FILE = "SubClusterID.xlsb"

ROLE_CONFIG = [
    {"key": "doctor", "source_file": "1.Doctor.xlsb", "prefix": "Doctor", "sheet": "Doctor"},
    {"key": "nurse", "source_file": "2.Nurse.xlsb", "prefix": "Nurse", "sheet": "Nurse"},
    {"key": "pharmacist", "source_file": "3.Pharmacist.xlsb", "prefix": "Pharmacist", "sheet": "Pharmacist"},
    {"key": "dietitian", "source_file": "4.Dietitian.xlsb", "prefix": "Dietitian", "sheet": "Dietitian"},
    {"key": "physiotherapist", "source_file": "5.Physiotherapist.xlsb", "prefix": "Physiotherapist", "sheet": "Physiotherapist"},
    {"key": "csr", "source_file": "6.CSR.xlsb", "prefix": "CSR", "sheet": "CSR"},
    {"key": "lab", "source_file": "7.Lab.xlsb", "prefix": "Lab", "sheet": "Lab"},
    {"key": "xray", "source_file": "8.X-Ray.xlsb", "prefix": "XRay", "sheet": "XRay"},
]

ROLE_BY_KEY = {item["key"]: item for item in ROLE_CONFIG}
APPROVED_GROUP_MAPPING = {"ipd": "IPD", "opd": "OPD", "both": "Both"}
APPROVED_DIMENSION_MAPPING = {"a": "A", "c": "C", "l": "L", "t": "T"}
MONTH_PATTERN = re.compile(
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(\d{2})$"
)
MONTH_NUMBER = {
    month: number
    for number, month in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        start=1,
    )
}
FACT_COLUMNS = [
    "SProID",
    "Group",
    "QDimension",
    "CaseNum",
    "Value",
    "YearMonthCaseNum",
    "Year-Month",
]
GRAIN_COLUMNS = ["SProID", "CaseNum", "Year-Month"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_directories() -> None:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    AGENT_DIR.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def audit_input_dir() -> Path:
    """Return the immutable audit snapshot for this run when OneDrive is enabled."""
    return AUDIT_SNAPSHOT_DIR if ONEDRIVE_INPUT_DIR is not None else INPUT_DIR


def moi_input_dir() -> Path:
    """Return the immutable MOI reference snapshot when OneDrive is enabled."""
    return (
        MOI_SNAPSHOT_DIR
        if MOI_ONEDRIVE_INPUT_DIR is not None
        else MOI_LOCAL_INPUT_DIR
    )


def sync_onedrive_inputs() -> dict[str, Any]:
    """Copy all eight OneDrive workbooks into one verified local snapshot."""
    ensure_directories()
    if ONEDRIVE_INPUT_DIR is None:
        return {
            "status": "LOCAL_INPUT",
            "files_synced": 0,
            "input_dir": str(INPUT_DIR),
        }

    if not ONEDRIVE_INPUT_DIR.is_dir():
        raise FileNotFoundError(
            f"ไม่พบ OneDrive input directory: {ONEDRIVE_INPUT_DIR}"
        )

    pending = STAGING_DIR / "source_snapshot_pending"
    previous = STAGING_DIR / "source_snapshot_previous"
    shutil.rmtree(pending, ignore_errors=True)
    shutil.rmtree(previous, ignore_errors=True)
    pending.mkdir(parents=True, exist_ok=False)

    manifest = []
    try:
        for config in ROLE_CONFIG:
            source = ONEDRIVE_INPUT_DIR / config["source_file"]
            if not source.is_file():
                raise FileNotFoundError(f"ไม่พบไฟล์ OneDrive: {source.name}")

            source_hash_before = sha256_file(source)
            destination = pending / source.name
            shutil.copy2(source, destination)
            source_hash_after = sha256_file(source)
            copied_hash = sha256_file(destination)
            if source_hash_before != source_hash_after or copied_hash != source_hash_after:
                raise RuntimeError(
                    f"OneDrive กำลังเปลี่ยนไฟล์ระหว่างคัดลอก: {source.name}"
                )
            manifest.append(
                {
                    "file": source.name,
                    "size_bytes": destination.stat().st_size,
                    "modified_at_utc": datetime.fromtimestamp(
                        destination.stat().st_mtime, tz=timezone.utc
                    ).isoformat(),
                    "sha256": copied_hash,
                }
            )

        if AUDIT_SNAPSHOT_DIR.exists():
            AUDIT_SNAPSHOT_DIR.replace(previous)
        try:
            pending.replace(AUDIT_SNAPSHOT_DIR)
        except Exception:
            if previous.exists() and not AUDIT_SNAPSHOT_DIR.exists():
                previous.replace(AUDIT_SNAPSHOT_DIR)
            raise
        shutil.rmtree(previous, ignore_errors=True)
    except Exception:
        shutil.rmtree(pending, ignore_errors=True)
        raise

    write_json(STAGING_DIR / "onedrive_snapshot_manifest.json", manifest)
    print(f"สร้าง OneDrive snapshot สำเร็จ {len(manifest)} ไฟล์")
    return {
        "status": "SNAPSHOT_READY",
        "files_synced": len(manifest),
        "snapshot_dir": str(AUDIT_SNAPSHOT_DIR),
    }


def sync_moi_reference_inputs() -> dict[str, Any]:
    """Copy approved MOI and Cluster workbooks into one verified snapshot."""
    ensure_directories()
    if MOI_ONEDRIVE_INPUT_DIR is None:
        return {
            "status": "LOCAL_INPUT",
            "files_synced": 0,
            "input_dir": str(MOI_LOCAL_INPUT_DIR),
        }
    if not MOI_ONEDRIVE_INPUT_DIR.is_dir():
        raise FileNotFoundError(
            f"ไม่พบ MOI OneDrive directory: {MOI_ONEDRIVE_INPUT_DIR}"
        )
    if (
        SUBCLUSTER_ONEDRIVE_INPUT_DIR is not None
        and not SUBCLUSTER_ONEDRIVE_INPUT_DIR.is_dir()
    ):
        raise FileNotFoundError(
            f"ไม่พบ SubCluster OneDrive directory: {SUBCLUSTER_ONEDRIVE_INPUT_DIR}"
        )

    pending = STAGING_DIR / "moi_reference_snapshot_pending"
    previous = STAGING_DIR / "moi_reference_snapshot_previous"
    shutil.rmtree(pending, ignore_errors=True)
    shutil.rmtree(previous, ignore_errors=True)
    pending.mkdir(parents=True, exist_ok=False)
    manifest = []
    try:
        source_specs = [
            (MOI_ONEDRIVE_INPUT_DIR, file_name, "moi_reference")
            for file_name in MOI_REFERENCE_FILES
        ]
        source_specs.append(
            (
                SUBCLUSTER_ONEDRIVE_INPUT_DIR or MOI_ONEDRIVE_INPUT_DIR,
                SUBCLUSTER_REFERENCE_FILE,
                "subcluster_reference",
            )
        )
        for source_dir, file_name, source_type in source_specs:
            source = source_dir / file_name
            if not source.is_file():
                raise FileNotFoundError(f"ไม่พบไฟล์ Mapping OneDrive: {file_name}")
            source_hash_before = sha256_file(source)
            destination = pending / file_name
            shutil.copy2(source, destination)
            source_hash_after = sha256_file(source)
            copied_hash = sha256_file(destination)
            if source_hash_before != source_hash_after or copied_hash != source_hash_after:
                raise RuntimeError(f"OneDrive กำลังเปลี่ยนไฟล์ระหว่างคัดลอก: {file_name}")
            manifest.append(
                {
                    "file": file_name,
                    "source_type": source_type,
                    "size_bytes": destination.stat().st_size,
                    "modified_at_utc": datetime.fromtimestamp(
                        destination.stat().st_mtime, tz=timezone.utc
                    ).isoformat(),
                    "sha256": copied_hash,
                }
            )

        if MOI_SNAPSHOT_DIR.exists():
            MOI_SNAPSHOT_DIR.replace(previous)
        try:
            pending.replace(MOI_SNAPSHOT_DIR)
        except Exception:
            if previous.exists() and not MOI_SNAPSHOT_DIR.exists():
                previous.replace(MOI_SNAPSHOT_DIR)
            raise
        shutil.rmtree(previous, ignore_errors=True)
    except Exception:
        shutil.rmtree(pending, ignore_errors=True)
        raise

    write_json(STAGING_DIR / "moi_snapshot_manifest.json", manifest)
    print(f"สร้าง MOI reference snapshot สำเร็จ {len(manifest)} ไฟล์")
    return {
        "status": "SNAPSHOT_READY",
        "files_synced": len(manifest),
        "snapshot_dir": str(MOI_SNAPSHOT_DIR),
    }


def clean_text(value: Any):
    if value is None or pd.isna(value):
        return pd.NA
    cleaned = str(value).replace("\u00a0", " ").strip()
    return cleaned if cleaned else pd.NA


def apply_approved_mapping(value: Any, mapping: dict[str, str]):
    cleaned = clean_text(value)
    if pd.isna(cleaned):
        return pd.NA, "blank"
    mapped = mapping.get(str(cleaned).casefold())
    return (mapped, "approved") if mapped is not None else (pd.NA, "unapproved")


def normalize_score(value: Any):
    if value is None or pd.isna(value):
        return pd.NA, "source_null"
    if isinstance(value, str):
        stripped = value.replace("\u00a0", " ").strip()
        if not stripped:
            return pd.NA, "approved_blank"
        try:
            number = float(stripped)
        except ValueError:
            return pd.NA, "invalid_score"
    elif isinstance(value, (int, float, np.integer, np.floating)):
        number = float(value)
    else:
        return pd.NA, "invalid_score"

    if number not in (0.0, 1.0):
        return pd.NA, "invalid_score"
    return int(number), "approved_score"


def sheet_period(sheet_name: str):
    matched = MONTH_PATTERN.fullmatch(sheet_name)
    if not matched:
        return None
    month, year_2digit = matched.groups()
    return 2000 + int(year_2digit), MONTH_NUMBER[month], month


def check_input_files() -> dict[str, Any]:
    ensure_directories()
    audit_dir = audit_input_dir()
    required = [
        (audit_dir, item["source_file"], "audit") for item in ROLE_CONFIG
    ] + [
        (REFERENCE_INPUT_DIR, REFERENCE_FILE, "reference")
    ] + [
        (moi_input_dir(), file_name, "moi_reference")
        for file_name in MOI_REFERENCE_FILES
    ] + [
        (moi_input_dir(), SUBCLUSTER_REFERENCE_FILE, "subcluster_reference")
    ]
    missing = [name for base, name, _ in required if not (base / name).is_file()]
    if missing:
        raise FileNotFoundError("ไม่พบไฟล์: " + ", ".join(missing))

    manifest = []
    for base, name, source_type in required:
        path = base / name
        manifest.append(
            {
                "file": name,
                "source_type": source_type,
                "size_bytes": path.stat().st_size,
                "modified_at_utc": datetime.fromtimestamp(
                    path.stat().st_mtime, tz=timezone.utc
                ).isoformat(),
                "sha256": sha256_file(path),
            }
        )
    write_json(STAGING_DIR / "source_manifest.json", manifest)
    print(f"ตรวจพบไฟล์ครบ {len(manifest)} ไฟล์")
    return {"status": "READY", "files_found": len(manifest)}


def extract_role(role_key: str) -> dict[str, Any]:
    ensure_directories()
    if role_key not in ROLE_BY_KEY:
        raise ValueError(f"ไม่รู้จัก role_key={role_key}")

    config = ROLE_BY_KEY[role_key]
    source_path = audit_input_dir() / config["source_file"]
    workbook = pd.ExcelFile(source_path, engine="pyxlsb")
    monthly_sheets = [
        sheet for sheet in workbook.sheet_names if sheet_period(sheet) is not None
    ]
    monthly_sheets.sort(key=lambda name: sheet_period(name)[:2])
    if not monthly_sheets:
        raise ValueError(f"ไม่พบชีตรายเดือนใน {source_path.name}")

    accepted_frames = []
    rejected_frames = []
    extraction_log = []

    for sheet_name in monthly_sheets:
        raw = pd.read_excel(source_path, sheet_name=sheet_name, engine="pyxlsb", header=None)
        if raw.empty:
            extraction_log.append(
                {"sheet": sheet_name, "status": "empty_sheet", "rows_output": 0}
            )
            continue

        first_column = raw.iloc[:, 0].map(clean_text)
        candidates = first_column.index[first_column.eq("SProID")].tolist()
        if not candidates:
            extraction_log.append(
                {"sheet": sheet_name, "status": "header_not_found", "rows_output": 0}
            )
            continue

        header_row = int(candidates[0])
        headers = raw.iloc[header_row]
        case_columns = []
        for column_index, value in headers.iloc[7:].items():
            if (
                isinstance(value, (int, float, np.integer, np.floating))
                and not pd.isna(value)
                and float(value).is_integer()
                and int(value) > 0
            ):
                case_columns.append((column_index, int(value)))

        questions = raw.iloc[header_row + 1 :].copy()
        questions = questions[questions.iloc[:, 0].notna()].copy()
        questions.iloc[:, 0] = questions.iloc[:, 0].map(clean_text)
        questions = questions[
            questions.iloc[:, 0]
            .astype("string")
            .str.match(rf"^{re.escape(config['prefix'])}\d+$", na=False)
        ].copy()

        metadata = questions.iloc[:, :7].copy()
        metadata.columns = [
            "SProID",
            "DocNum",
            "DocName",
            "GroupRaw",
            "Details",
            "Profession",
            "QDimensionRaw",
        ]
        score_matrix = questions.loc[:, [column for column, _ in case_columns]].copy()
        score_matrix.columns = [case_number for _, case_number in case_columns]
        wide = pd.concat([metadata, score_matrix], axis=1)
        long = wide.melt(
            id_vars=list(metadata.columns), var_name="CaseNum", value_name="RawValue"
        )
        long = long[long["RawValue"].notna()].copy()

        score_results = long["RawValue"].map(normalize_score)
        long["Value"] = score_results.map(lambda item: item[0])
        long["ScoreStatus"] = score_results.map(lambda item: item[1])
        group_results = long["GroupRaw"].map(
            lambda value: apply_approved_mapping(value, APPROVED_GROUP_MAPPING)
        )
        dimension_results = long["QDimensionRaw"].map(
            lambda value: apply_approved_mapping(value, APPROVED_DIMENSION_MAPPING)
        )
        long["Group"] = group_results.map(lambda item: item[0])
        long["GroupMappingStatus"] = group_results.map(lambda item: item[1])
        long["QDimension"] = dimension_results.map(lambda item: item[0])
        long["DimensionMappingStatus"] = dimension_results.map(lambda item: item[1])

        year, _, month = sheet_period(sheet_name)
        long["Year-Month"] = f"{year}-{month}"
        long["CaseNum"] = pd.to_numeric(long["CaseNum"], errors="raise").astype("int64")
        long["YearMonthCaseNum"] = long["Year-Month"] + "-" + long["CaseNum"].astype(str)
        long["Role"] = config["sheet"]
        long["SourceFile"] = config["source_file"]
        long["SourceSheet"] = sheet_name

        rejected = long[long["ScoreStatus"].eq("invalid_score")].copy()
        accepted = long[~long["ScoreStatus"].eq("invalid_score")].copy()
        if not rejected.empty:
            rejected_frames.append(rejected)
        if not accepted.empty:
            accepted_frames.append(accepted)

        extraction_log.append(
            {
                "source_file": config["source_file"],
                "sheet": sheet_name,
                "header_row_1_based": header_row + 1,
                "question_rows": int(len(questions)),
                "case_columns": int(len(case_columns)),
                "rows_output": int(len(accepted)),
                "rows_rejected": int(len(rejected)),
                "status": "ok",
            }
        )

    if not accepted_frames:
        raise ValueError(f"ไม่พบข้อมูลที่ใช้ได้ใน {config['source_file']}")

    accepted_all = pd.concat(accepted_frames, ignore_index=True)
    accepted_all["SProID"] = accepted_all["SProID"].map(clean_text)
    accepted_all["Value"] = pd.array(accepted_all["Value"], dtype="Int64")
    fact = accepted_all[FACT_COLUMNS + ["Role"]].copy()
    detail_columns = FACT_COLUMNS + [
        "Role",
        "GroupMappingStatus",
        "DimensionMappingStatus",
        "ScoreStatus",
        "SourceFile",
        "SourceSheet",
        "RawValue",
    ]
    details = accepted_all[detail_columns].copy()
    # Raw evidence can contain both numbers and whitespace markers. Store it as
    # text so Parquet has one deterministic type across every source sheet.
    details["RawValue"] = details["RawValue"].astype("string")

    fact.to_parquet(STAGING_DIR / f"{role_key}_fact.parquet", index=False)
    details.to_parquet(STAGING_DIR / f"{role_key}_details.parquet", index=False)
    write_json(STAGING_DIR / f"{role_key}_extraction_log.json", extraction_log)

    rejected_rows = 0
    if rejected_frames:
        rejected_all = pd.concat(rejected_frames, ignore_index=True)
        rejected_columns = [
            "SProID",
            "CaseNum",
            "RawValue",
            "ScoreStatus",
            "SourceFile",
            "SourceSheet",
        ]
        rejected_all[rejected_columns].to_csv(
            STAGING_DIR / f"{role_key}_rejected.csv",
            index=False,
            encoding="utf-8-sig",
        )
        rejected_rows = int(len(rejected_all))
    else:
        (STAGING_DIR / f"{role_key}_rejected.csv").unlink(missing_ok=True)

    result = {
        "role": config["sheet"],
        "role_key": role_key,
        "rows": int(len(fact)),
        "months": int(fact["Year-Month"].nunique()),
        "rejected_rows": rejected_rows,
        "fact_path": str(STAGING_DIR / f"{role_key}_fact.parquet"),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def combine_fact() -> dict[str, Any]:
    ensure_directories()
    frames = []
    for config in ROLE_CONFIG:
        path = STAGING_DIR / f"{config['key']}_fact.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"ไม่พบผล Extract: {path}")
        frames.append(pd.read_parquet(path))

    combined = pd.concat(frames, ignore_index=True)
    combined["_period"] = pd.PeriodIndex(
        pd.to_datetime(combined["Year-Month"], format="%Y-%b"), freq="M"
    )
    combined = (
        combined.sort_values(["_period", "Role", "SProID", "CaseNum"], kind="stable")
        .drop(columns="_period")
        .reset_index(drop=True)
    )
    path = STAGING_DIR / "combined_fact.parquet"
    combined.to_parquet(path, index=False)
    print(f"รวมข้อมูลสำเร็จ {len(combined):,} แถว")
    return {"rows": int(len(combined)), "path": str(path)}


def _check(check_id, scope, dimension, status, severity, message_th, evidence):
    return {
        "check_id": check_id,
        "scope": scope,
        "dimension": dimension,
        "status": status,
        "severity": severity,
        "message_th": message_th,
        "evidence": evidence,
    }


def validate_data_quality() -> dict[str, Any]:
    ensure_directories()
    combined = pd.read_parquet(STAGING_DIR / "combined_fact.parquet")
    checks = []

    extraction_logs = []
    for config in ROLE_CONFIG:
        extraction_logs.extend(read_json(STAGING_DIR / f"{config['key']}_extraction_log.json"))
    failed_headers = [item for item in extraction_logs if item["status"] != "ok"]
    checks.append(
        _check(
            "DQ001_HEADERS",
            "all_sources",
            "schema",
            "PASS" if not failed_headers else "FAIL",
            "CRITICAL",
            "พบ header SProID ในทุกชีตรายเดือน" if not failed_headers else "บางชีตไม่พบ header SProID",
            {"failed_sheets": failed_headers, "sheets_checked": len(extraction_logs)},
        )
    )

    duplicate_rows = int(combined.duplicated(GRAIN_COLUMNS, keep=False).sum())
    checks.append(
        _check(
            "DQ002_GRAIN_UNIQUE",
            "combined_fact",
            "uniqueness",
            "PASS" if duplicate_rows == 0 else "FAIL",
            "CRITICAL",
            "คีย์หลักไม่ซ้ำ" if duplicate_rows == 0 else "พบคีย์ซ้ำที่ grain หลัก",
            {"duplicate_rows": duplicate_rows, "grain": GRAIN_COLUMNS},
        )
    )

    rejected_count = 0
    invalid_mapping_count = 0
    row_summary = []
    for config in ROLE_CONFIG:
        rejected_path = STAGING_DIR / f"{config['key']}_rejected.csv"
        if rejected_path.is_file():
            rejected_count += len(pd.read_csv(rejected_path))
        details = pd.read_parquet(STAGING_DIR / f"{config['key']}_details.parquet")
        invalid_mapping_count += int(details["GroupMappingStatus"].eq("unapproved").sum())
        invalid_mapping_count += int(details["DimensionMappingStatus"].eq("unapproved").sum())
        role_fact = combined[combined["Role"].eq(config["sheet"])]
        row_summary.append(
            {
                "Role": config["sheet"],
                "Rows": int(len(role_fact)),
                "Months": int(role_fact["Year-Month"].nunique()),
                "Scored": int(role_fact["Value"].notna().sum()),
                "Rejected": int(len(pd.read_csv(rejected_path))) if rejected_path.is_file() else 0,
            }
        )

    checks.append(
        _check(
            "DQ003_SCORE_DOMAIN",
            "combined_fact",
            "validity",
            "PASS" if rejected_count == 0 else "WARN",
            "HIGH",
            "คะแนนอยู่ใน approved domain" if rejected_count == 0 else "แยกคะแนนนอก approved domain แล้ว",
            {"rejected_rows": rejected_count, "allowed_values": [0, 1, None]},
        )
    )
    checks.append(
        _check(
            "DQ004_APPROVED_MAPPING",
            "combined_fact",
            "consistency",
            "PASS" if invalid_mapping_count == 0 else "FAIL",
            "HIGH",
            "Group และ QDimension ผ่าน approved mapping" if invalid_mapping_count == 0 else "พบ mapping ที่ไม่ได้รับอนุมัติ",
            {"unapproved_rows": invalid_mapping_count},
        )
    )

    invalid_id_count = int(
        (~combined["SProID"].astype("string").str.match(
            r"^(Doctor|Nurse|Pharmacist|Dietitian|Physiotherapist|CSR|Lab|XRay)\d+$",
            na=False,
        )).sum()
    )
    checks.append(
        _check(
            "DQ005_SPROID_FORMAT",
            "combined_fact",
            "validity",
            "PASS" if invalid_id_count == 0 else "FAIL",
            "HIGH",
            "SProID มีรูปแบบถูกต้อง" if invalid_id_count == 0 else "พบ SProID รูปแบบผิด",
            {"invalid_rows": invalid_id_count},
        )
    )

    for item in row_summary:
        checks.append(
            _check(
                f"DQ006_NONEMPTY_{item['Role'].upper()}",
                item["Role"],
                "completeness",
                "PASS" if item["Rows"] > 0 else "FAIL",
                "CRITICAL",
                f"{item['Role']} มีข้อมูล {item['Rows']:,} แถว",
                {"rows": item["Rows"], "months": item["Months"]},
            )
        )

    write_json(STAGING_DIR / "validation_checks.json", checks)
    write_json(STAGING_DIR / "row_summary.json", row_summary)
    failures = sum(item["status"] == "FAIL" for item in checks)
    warnings = sum(item["status"] == "WARN" for item in checks)
    print(f"Validate สำเร็จ: FAIL={failures}, WARN={warnings}")
    return {"checks": len(checks), "failures": failures, "warnings": warnings}


def join_mraudit_lists() -> dict[str, Any]:
    ensure_directories()
    fact = pd.read_parquet(STAGING_DIR / "combined_fact.parquet")
    reference_path = REFERENCE_INPUT_DIR / REFERENCE_FILE
    dimension = pd.read_excel(reference_path, sheet_name="MRAuditLists", engine="openpyxl")
    if "YearMonthCaseNum" not in dimension.columns:
        raise ValueError("MRAuditLists ไม่มีคอลัมน์ YearMonthCaseNum")
    dimension["YearMonthCaseNum"] = dimension["YearMonthCaseNum"].map(clean_text)
    dimension = dimension[dimension["YearMonthCaseNum"].notna()].copy()
    duplicate_keys = int(dimension.duplicated("YearMonthCaseNum", keep=False).sum())
    if duplicate_keys:
        raise ValueError(f"MRAuditLists มี YearMonthCaseNum ซ้ำ {duplicate_keys} แถว")

    dimension_columns = [
        "YearMonthCaseNum",
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
    ]
    missing_columns = [name for name in dimension_columns if name not in dimension.columns]
    if missing_columns:
        raise ValueError("MRAuditLists ขาดคอลัมน์: " + ", ".join(missing_columns))

    enriched = fact.merge(
        dimension[dimension_columns],
        on="YearMonthCaseNum",
        how="left",
        validate="many_to_one",
        indicator="_join_status",
    )
    unmatched_rows = int(enriched["_join_status"].eq("left_only").sum())
    coverage = 1 - (unmatched_rows / len(enriched))
    enriched["CountComplete"] = enriched["Value"].notna().astype("int64")
    enriched["AuditorError"] = 0
    enriched.to_parquet(STAGING_DIR / "enriched.parquet", index=False)

    checks = [
        _check(
            "DQ007_DIMENSION_KEY_UNIQUE",
            "MRAuditLists",
            "integrity",
            "PASS",
            "CRITICAL",
            "YearMonthCaseNum ฝั่ง dimension ไม่ซ้ำ",
            {"duplicate_key_rows": duplicate_keys},
        ),
        _check(
            "DQ008_JOIN_COVERAGE",
            "enriched",
            "integrity",
            "PASS" if coverage >= 0.99 else "WARN",
            "HIGH",
            "Join coverage ผ่านเกณฑ์ 99%" if coverage >= 0.99 else "บางรายการจับคู่ MRAuditLists ไม่ได้",
            {
                "matched_rows": int(len(enriched) - unmatched_rows),
                "unmatched_rows": unmatched_rows,
                "coverage_rate": round(coverage, 6),
                "threshold": 0.99,
            },
        ),
    ]
    metrics = {
        "fact_rows": int(len(fact)),
        "enriched_rows": int(len(enriched)),
        "matched_rows": int(len(enriched) - unmatched_rows),
        "unmatched_rows": unmatched_rows,
        "coverage_rate": round(coverage, 6),
    }
    write_json(STAGING_DIR / "join_checks.json", checks)
    write_json(STAGING_DIR / "join_metrics.json", metrics)
    print(f"Join coverage: {coverage:.2%}")
    return metrics


def build_moi_reference_dimensions() -> dict[str, Any]:
    """Build deterministic, deduplicated dimensions from the MOI workbooks."""
    from mra_moi_mapper import build_moi_reference_dimensions as build_dimensions

    result = build_dimensions(moi_input_dir(), STAGING_DIR)
    write_json(STAGING_DIR / "moi_dimension_metrics.json", result["metrics"])
    write_json(STAGING_DIR / "moi_dimension_checks.json", result["checks"])
    rows = {
        name: int(metrics["rows"])
        for name, metrics in result["metrics"].items()
    }
    print("สร้าง MOI dimensions สำเร็จ: " + json.dumps(rows, ensure_ascii=False))
    return {"dimensions": rows, "checks": len(result["checks"])}


def map_moi_references() -> dict[str, Any]:
    """Enrich the private analytical rows with approved MOI reference fields."""
    from mra_moi_mapper import map_moi_references as apply_mappings

    result = apply_mappings(STAGING_DIR)
    write_json(STAGING_DIR / "moi_mapping_metrics.json", result["mappings"])
    write_json(STAGING_DIR / "moi_mapping_checks.json", result["checks"])
    print(f"MOI mapping สำเร็จ {result['rows']:,} แถว")
    return {"rows": result["rows"], "mappings": result["mappings"]}


def reconcile_ha_report() -> dict[str, Any]:
    """Compare encounter coverage with HA Report without persisting identifiers."""
    from mra_moi_mapper import reconcile_ha_report as reconcile

    result = reconcile(moi_input_dir(), STAGING_DIR)
    write_json(STAGING_DIR / "ha_reconciliation_metrics.json", result)
    write_json(STAGING_DIR / "ha_reconciliation_checks.json", result["checks"])
    print("ตรวจเทียบ HA Report สำเร็จโดยบันทึกเฉพาะจำนวนและอัตราครอบคลุม")
    return {"period": result["period"], "metrics": result["metrics"]}


def _compare_to_reference(actual: pd.DataFrame, reference: pd.DataFrame, scope: str):
    actual_cmp = actual[FACT_COLUMNS].copy()
    reference_cmp = reference[FACT_COLUMNS].copy()
    for frame in (actual_cmp, reference_cmp):
        frame["CaseNum"] = pd.to_numeric(frame["CaseNum"], errors="coerce").astype("Int64")
        frame["Value"] = pd.to_numeric(frame["Value"], errors="coerce").astype("Int64")
    compared = reference_cmp.merge(
        actual_cmp,
        on=GRAIN_COLUMNS,
        how="outer",
        suffixes=("_reference", "_actual"),
        indicator=True,
    )
    both = compared[compared["_merge"].eq("both")].copy()
    equal_values = (
        both["Value_reference"].astype("Int64").fillna(-99)
        == both["Value_actual"].astype("Int64").fillna(-99)
    )
    return {
        "scope": scope,
        "actual_rows": int(len(actual_cmp)),
        "reference_rows": int(len(reference_cmp)),
        "matched_keys": int(compared["_merge"].eq("both").sum()),
        "actual_only": int(compared["_merge"].eq("right_only").sum()),
        "reference_only": int(compared["_merge"].eq("left_only").sum()),
        "value_mismatches_on_matched_keys": int((~equal_values).sum()),
        "exact_match": bool(compared["_merge"].eq("both").all() and equal_values.all()),
    }


def compare_reference() -> dict[str, Any]:
    ensure_directories()
    reference_path = REFERENCE_INPUT_DIR / REFERENCE_FILE
    comparisons = []
    checks = []
    for config in ROLE_CONFIG:
        actual = pd.read_parquet(STAGING_DIR / f"{config['key']}_fact.parquet")
        reference = pd.read_excel(reference_path, sheet_name=config["sheet"], engine="openpyxl")
        comparison = _compare_to_reference(actual, reference, config["sheet"])
        comparison["source_newer_than_reference"] = bool(
            (audit_input_dir() / config["source_file"]).stat().st_mtime
            > reference_path.stat().st_mtime
        )
        comparisons.append(comparison)
        checks.append(
            _check(
                f"DQ009_REFERENCE_{config['sheet'].upper()}",
                config["sheet"],
                "reference_drift",
                "PASS" if comparison["exact_match"] else "WARN",
                "MEDIUM",
                "ผลลัพธ์ตรงกับชีตอ้างอิง" if comparison["exact_match"] else "ผลลัพธ์ต่างจากชีตอ้างอิง",
                comparison,
            )
        )
    write_json(STAGING_DIR / "reference_comparisons.json", comparisons)
    write_json(STAGING_DIR / "reference_checks.json", checks)
    warnings = sum(item["status"] == "WARN" for item in checks)
    print(f"Reference comparison สำเร็จ: WARN={warnings}")
    return {"comparisons": len(comparisons), "warnings": warnings}


def build_quality_report() -> dict[str, Any]:
    ensure_directories()
    fact = pd.read_parquet(STAGING_DIR / "combined_fact.parquet")
    join_metrics = read_json(STAGING_DIR / "join_metrics.json")
    row_summary = read_json(STAGING_DIR / "row_summary.json")
    comparisons = read_json(STAGING_DIR / "reference_comparisons.json")
    moi_dimension_metrics = read_json(STAGING_DIR / "moi_dimension_metrics.json")
    moi_mapping_metrics = read_json(STAGING_DIR / "moi_mapping_metrics.json")
    ha_reconciliation = read_json(STAGING_DIR / "ha_reconciliation_metrics.json")
    checks = (
        read_json(STAGING_DIR / "validation_checks.json")
        + read_json(STAGING_DIR / "join_checks.json")
        + read_json(STAGING_DIR / "moi_dimension_checks.json")
        + read_json(STAGING_DIR / "moi_mapping_checks.json")
        + read_json(STAGING_DIR / "ha_reconciliation_checks.json")
        + read_json(STAGING_DIR / "reference_checks.json")
    )
    failures = [item for item in checks if item["status"] == "FAIL"]
    warnings = [item for item in checks if item["status"] == "WARN"]
    status = "BLOCKED" if failures else "READY_WITH_WARNINGS" if warnings else "READY"
    periods = pd.PeriodIndex(
        pd.to_datetime(fact["Year-Month"], format="%Y-%b"), freq="M"
    )
    report = {
        "schema_version": "1.0",
        "pipeline": {
            "name": PIPELINE_NAME,
            "version": PIPELINE_VERSION,
            "run_id": str(uuid.uuid4()),
            "generated_at_utc": utc_now_iso(),
            "deterministic": True,
        },
        "dataset": {
            "grain": GRAIN_COLUMNS,
            "fact_rows": int(len(fact)),
            "enriched_rows": join_metrics["enriched_rows"],
            "roles": row_summary,
            "date_period_min": str(periods.min()),
            "date_period_max": str(periods.max()),
        },
        "join": join_metrics,
        "moi_mapping": {
            "dimensions": moi_dimension_metrics,
            "coverage": moi_mapping_metrics,
        },
        "ha_reconciliation": ha_reconciliation,
        "checks": checks,
        "reference_comparison": comparisons,
        "agent_readiness": {
            "status": status,
            "ready_for_aggregate_analysis": not failures,
            "ready_for_patient_level_ai": False,
            "failed_checks": len(failures),
            "warning_checks": len(warnings),
            "restrictions_th": [
                "ส่งเข้า AI เฉพาะ agent_payload.json",
                "ห้ามส่ง HN, AN หรือข้อมูลระดับบุคคลเข้า LLM",
                "ต้องเปิดเผย join coverage, MOI mapping coverage และ reference drift",
                "HA Report ใช้ตรวจเทียบแบบ aggregate เท่านั้น ไม่ส่ง encounter key เข้า LLM",
            ],
        },
        "provenance": read_json(STAGING_DIR / "source_manifest.json"),
    }
    write_json(OUTPUT_DIR / "quality_report.json", report)
    print(f"Quality status: {status}; FAIL={len(failures)}; WARN={len(warnings)}")
    return {"status": status, "failures": len(failures), "warnings": len(warnings)}


def quality_gate() -> dict[str, Any]:
    report = read_json(OUTPUT_DIR / "quality_report.json")
    readiness = report["agent_readiness"]
    if readiness["status"] == "BLOCKED":
        failed_ids = [
            item["check_id"] for item in report["checks"] if item["status"] == "FAIL"
        ]
        raise RuntimeError("Data Quality BLOCKED: " + ", ".join(failed_ids))
    print(f"Quality gate ผ่านด้วยสถานะ {readiness['status']}")
    return readiness


def load_outputs() -> dict[str, Any]:
    ensure_directories()
    fact = pd.read_parquet(STAGING_DIR / "combined_fact.parquet")
    enriched = pd.read_parquet(STAGING_DIR / "moi_enriched.parquet")
    role_frames = {
        config["sheet"]: pd.read_parquet(STAGING_DIR / f"{config['key']}_fact.parquet")[FACT_COLUMNS]
        for config in ROLE_CONFIG
    }
    row_summary = pd.DataFrame(read_json(STAGING_DIR / "row_summary.json"))
    mapping_summary = pd.DataFrame(read_json(STAGING_DIR / "moi_mapping_metrics.json"))
    ha_summary = pd.DataFrame(
        read_json(STAGING_DIR / "ha_reconciliation_metrics.json")["metrics"]
    )
    compliance_by_profession_period = _compliance_summary(
        fact, ["Role", "Year-Month"]
    )
    cluster_rows = enriched[
        enriched["ClusterID"].notna() & enriched["SCShortName"].notna()
    ].copy()
    compliance_by_cluster_period = _compliance_summary(
        cluster_rows, ["ClusterID", "SCShortName", "Year-Month"]
    )

    fact_path = OUTPUT_DIR / "audit_fact.parquet"
    enriched_path = OUTPUT_DIR / "audit_enriched.parquet"
    compliance_path = OUTPUT_DIR / "compliance_by_profession.parquet"
    cluster_compliance_path = OUTPUT_DIR / "compliance_by_cluster.parquet"
    workbook_path = OUTPUT_DIR / "agentic_mra_airflow_output.xlsx"
    fact.to_parquet(fact_path, index=False)
    enriched.drop(columns="_join_status").to_parquet(enriched_path, index=False)
    compliance_by_profession_period.to_parquet(compliance_path, index=False)
    compliance_by_cluster_period.to_parquet(cluster_compliance_path, index=False)
    dimension_files = [
        "moi_question_dimension.parquet",
        "moi_cluster_dimension.parquet",
        "moi_ward_dimension.parquet",
        "moi_icd10_dimension.parquet",
        "moi_doctor_code_dimension.parquet",
    ]
    for file_name in dimension_files:
        shutil.copyfile(STAGING_DIR / file_name, OUTPUT_DIR / file_name)
    for file_name in [
        "moi_dimension_metrics.json",
        "moi_mapping_metrics.json",
        "ha_reconciliation_metrics.json",
    ]:
        shutil.copyfile(STAGING_DIR / file_name, OUTPUT_DIR / file_name)

    rejected_frames = []
    for config in ROLE_CONFIG:
        path = STAGING_DIR / f"{config['key']}_rejected.csv"
        if path.is_file():
            frame = pd.read_csv(path)
            frame["Role"] = config["sheet"]
            rejected_frames.append(frame)
    if rejected_frames:
        pd.concat(rejected_frames, ignore_index=True).to_csv(
            OUTPUT_DIR / "rejected_records.csv", index=False, encoding="utf-8-sig"
        )

    with pd.ExcelWriter(workbook_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as writer:
        for role, frame in role_frames.items():
            frame.to_excel(writer, sheet_name=role, index=False)
        fact.to_excel(writer, sheet_name="CombinedAudit", index=False)
        enriched.drop(columns="_join_status").to_excel(writer, sheet_name="EnrichedData", index=False)
        row_summary.to_excel(writer, sheet_name="QualitySummary", index=False)
        mapping_summary.to_excel(writer, sheet_name="MappingCoverage", index=False)
        ha_summary.to_excel(writer, sheet_name="HAReconciliation", index=False)
        compliance_by_profession_period.to_excel(
            writer, sheet_name="ComplianceByProfession", index=False
        )
        compliance_by_cluster_period.to_excel(
            writer, sheet_name="ComplianceByCluster", index=False
        )
        header_format = writer.book.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#1F4E78",
                "border": 1,
                "align": "center",
                "valign": "vcenter",
            }
        )
        for worksheet in writer.sheets.values():
            worksheet.freeze_panes(1, 0)
            worksheet.set_row(0, 24, header_format)
            worksheet.set_column(0, 30, 18)

    mapping = {
        "role_config": ROLE_CONFIG,
        "group_mapping": APPROVED_GROUP_MAPPING,
        "dimension_mapping": APPROVED_DIMENSION_MAPPING,
        "score_domain": [0, 1, None],
        "grain": GRAIN_COLUMNS,
        "moi_reference_files": MOI_REFERENCE_FILES,
        "subcluster_reference_file": SUBCLUSTER_REFERENCE_FILE,
        "moi_join_keys": {
            "NewAuditForm2025.xlsx": "SProID",
            "MRCode.xlsx": ["ClusterID", "DischargeWardName", "MainICD", "DoctorCode"],
            "SubClusterID.xlsb": {"join_key": "ClusterID", "cluster_name": "SCShortName"},
            "HAReportJan2024-May2025.xlsm": ["HN+AN (IPD)", "HN+VisitDate (OPD), aggregate reconciliation only"],
        },
    }
    write_json(OUTPUT_DIR / "approved_mapping.json", mapping)
    print(f"Load สำเร็จ: {OUTPUT_DIR}")
    return {"fact_rows": int(len(fact)), "output_dir": str(OUTPUT_DIR)}


def load_to_mariadb() -> dict[str, Any]:
    """Load the validated analytical snapshot into the project MariaDB."""
    from mra_mariadb_loader import load_mra_to_mariadb

    return load_mra_to_mariadb(OUTPUT_DIR)


AGENT_PROMPT_TH = """
คุณคือ AI Agent ด้าน Data Quality สำหรับข้อมูลตรวจเวชระเบียน

1. อ่าน quality_summary, checks และ aggregates จาก JSON เท่านั้น
2. หาก readiness_status = BLOCKED ให้หยุดและรายงาน blocking checks
3. ห้ามสร้างตัวเลขหรือสาเหตุที่ไม่มีในข้อมูล
4. completion_rate = complete / scored เท่านั้น
5. เมื่อถาม Compliance rate ให้ใช้สูตรเดียวกับ Power BI: SUM(Value) / COUNTROWS เท่านั้น
6. ห้ามเฉลี่ย compliance_rate ระหว่างกลุ่ม ให้รวม total_score และ total_audit_items ก่อนหาร
7. เมื่อถาม Cluster ให้ใช้ compliance_by_cluster และชื่อจาก SCShortName ที่ Mapping ด้วย ClusterID
8. ประเด็นที่ควรปรับปรุงภายใน Cluster ต้องอ้าง cluster_improvement_areas พร้อมตัวตั้ง/ตัวหาร และถือเป็นจุดคะแนนต่ำที่ต้อง Human Review ไม่ใช่สาเหตุที่ยืนยันแล้ว
9. แจ้ง reference drift, MOI mapping coverage และ HA reconciliation ทุกครั้งที่มีผลต่อข้อสรุป
10. ห้ามขอ แสดง หรืออนุมาน HN, AN ชื่อผู้ป่วย ชื่อแพทย์ หรือข้อมูลส่วนบุคคล

ตอบภาษาไทยโดยแบ่งเป็น: สถานะข้อมูล, ข้อค้นพบ, จุดผิดปกติ,
ผลกระทบ, งานที่มนุษย์ต้องตรวจ และข้อจำกัด
""".strip()


def _json_safe_records(frame: pd.DataFrame):
    cleaned = frame.astype(object).where(pd.notna(frame), None)
    records = cleaned.to_dict(orient="records")
    for record in records:
        for key, value in list(record.items()):
            if isinstance(value, np.integer):
                record[key] = int(value)
            elif isinstance(value, np.floating):
                record[key] = float(value)
    return records


def _compliance_summary(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Calculate the Power BI-compatible Compliance measure at an additive grain."""
    work = frame.copy()
    work["Value"] = pd.to_numeric(work["Value"], errors="coerce")
    summary = (
        work.groupby(group_columns, dropna=False)
        .agg(
            total_score=(
                "Value",
                lambda values: (
                    np.nan
                    if values.notna().sum() == 0
                    else float(values.sum(min_count=1))
                ),
            ),
            total_audit_items=("Value", "size"),
            scored_items=("Value", "count"),
        )
        .reset_index()
    )
    summary["unscored_items"] = (
        summary["total_audit_items"] - summary["scored_items"]
    )
    numerator = pd.to_numeric(summary["total_score"], errors="coerce").astype("float64")
    denominator = pd.to_numeric(
        summary["total_audit_items"], errors="coerce"
    ).astype("float64")
    summary["compliance_rate"] = np.where(
        denominator > 0,
        numerator / denominator,
        np.nan,
    )
    summary["compliance_rate"] = pd.to_numeric(
        summary["compliance_rate"], errors="coerce"
    ).round(6)
    return summary


def _compliance_chart_svg(
    compliance: pd.DataFrame,
    period_min: str,
    period_max: str,
    *,
    entity_column: str = "Role",
    detail_column: str | None = None,
    title: str = "MRA Compliance Rate by Profession",
) -> str:
    """Build a privacy-safe SVG chart from aggregate compliance results only."""
    chart = compliance.copy()
    chart["compliance_rate"] = pd.to_numeric(
        chart["compliance_rate"], errors="coerce"
    )
    chart = chart.dropna(subset=["compliance_rate"]).sort_values(
        "compliance_rate", ascending=True
    )

    width = 1200
    height = max(620, 220 + len(chart) * 58)
    plot_left = 275
    plot_right = 1080
    plot_width = plot_right - plot_left
    plot_top = 158
    row_height = 58
    bar_height = 30

    role_th = {
        "Doctor": "แพทย์",
        "Nurse": "พยาบาล",
        "Pharmacist": "เภสัชกร",
        "Dietitian": "นักกำหนดอาหาร",
        "Physiotherapist": "นักกายภาพบำบัด",
        "CSR": "เวชระเบียน / CSR",
        "Lab": "ห้องปฏิบัติการ",
        "XRay": "รังสีวิทยา",
    }

    def metric_color(rate: float) -> str:
        if rate >= 0.90:
            return "#148F77"
        if rate > 0.70:
            return "#D4A017"
        return "#C84E51"

    description = "; ".join(
        f"{row.get(entity_column)} {float(row['compliance_rate']) * 100:.2f}%"
        for row in chart.to_dict(orient="records")
    )
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
            f'height="{height}" viewBox="0 0 {width} {height}" role="img" '
            'aria-labelledby="chart-title chart-desc">'
        ),
        f'<title id="chart-title">{escape(title)}</title>',
        f'<desc id="chart-desc">{escape(description)}</desc>',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        '<style>text{font-family:Tahoma,Arial,sans-serif;fill:#1F2933}'
        '.title{font-size:30px;font-weight:700}.subtitle{font-size:16px;fill:#52606D}'
        '.label{font-size:17px;font-weight:600}.value{font-size:16px;font-weight:700}'
        '.detail{font-size:13px;fill:#616E7C}.tick{font-size:13px;fill:#7B8794}'
        '.note{font-size:14px;fill:#52606D}</style>',
        f'<text id="chart-title-text" class="title" x="48" y="52">{escape(title)}</text>',
        (
            f'<text class="subtitle" x="48" y="84">ช่วงข้อมูล {escape(str(period_min))} ถึง '
            f'{escape(str(period_max))} · สูตร Power BI: SUM(Value) / COUNTROWS(MRA Data)</text>'
        ),
        '<text class="subtitle" x="48" y="111">เรียงจากต่ำไปสูง · แสดงคะแนนรวม / จำนวนรายการตรวจ</text>',
    ]

    for tick in (0, 25, 50, 75, 100):
        x = plot_left + (tick / 100) * plot_width
        parts.append(
            f'<line x1="{x:.1f}" y1="{plot_top - 12}" x2="{x:.1f}" '
            f'y2="{plot_top + len(chart) * row_height}" stroke="#E4E7EB" stroke-width="1"/>'
        )
        parts.append(
            f'<text class="tick" x="{x:.1f}" y="{plot_top - 22}" text-anchor="middle">{tick}%</text>'
        )

    for index, row in enumerate(chart.to_dict(orient="records")):
        entity = str(row.get(entity_column, ""))
        label = role_th.get(entity, entity)
        detail = str(row.get(detail_column, entity)) if detail_column else entity
        rate = max(0.0, min(1.0, float(row["compliance_rate"])))
        score = float(row["total_score"])
        total = int(row["total_audit_items"])
        y = plot_top + index * row_height
        bar_width = rate * plot_width
        parts.extend(
            [
                f'<text class="label" x="{plot_left - 18}" y="{y + 15}" text-anchor="end">{escape(label)}</text>',
                f'<text class="detail" x="{plot_left - 18}" y="{y + 35}" text-anchor="end">{escape(detail)}</text>',
                f'<rect x="{plot_left}" y="{y}" width="{plot_width}" height="{bar_height}" rx="5" fill="#F0F4F8"/>',
                (
                    f'<rect x="{plot_left}" y="{y}" width="{bar_width:.1f}" height="{bar_height}" '
                    f'rx="5" fill="{metric_color(rate)}" stroke="#34495E" stroke-width="0.8"/>'
                ),
                (
                    f'<text class="value" x="{min(plot_left + bar_width + 10, 1105):.1f}" '
                    f'y="{y + 21}">{rate * 100:.2f}%</text>'
                ),
                (
                    f'<text class="detail" x="{plot_left}" y="{y + 47}">'
                    f'{score:,.0f} / {total:,} รายการ</text>'
                ),
            ]
        )

    note_y = plot_top + len(chart) * row_height + 34
    parts.extend(
        [
            f'<text class="note" x="48" y="{note_y}">สีช่วยบอกระดับเท่านั้น โปรดใช้ค่าร้อยละและตัวตั้ง/ตัวหารในการตีความ</text>',
            f'<text class="note" x="48" y="{note_y + 24}">แหล่งข้อมูล: MRA aggregate payload · ไม่มีข้อมูลระบุตัวผู้ป่วย</text>',
            '</svg>',
        ]
    )
    return "\n".join(parts)


def build_agent_payload() -> dict[str, Any]:
    ensure_directories()
    fact = pd.read_parquet(OUTPUT_DIR / "audit_fact.parquet")
    enriched = pd.read_parquet(OUTPUT_DIR / "audit_enriched.parquet")
    report = read_json(OUTPUT_DIR / "quality_report.json")
    aggregate = (
        fact.groupby(["Role", "Year-Month", "QDimension"], dropna=False)
        .agg(
            records=("Value", "size"),
            scored=("Value", "count"),
            complete=("Value", lambda values: int(values.eq(1).sum())),
            incomplete=("Value", lambda values: int(values.eq(0).sum())),
        )
        .reset_index()
    )
    aggregate["completion_rate"] = np.where(
        aggregate["scored"].gt(0),
        aggregate["complete"] / aggregate["scored"],
        np.nan,
    )
    compliance_by_profession = _compliance_summary(fact, ["Role"])
    compliance_by_profession_period = _compliance_summary(
        fact, ["Role", "Year-Month"]
    )
    cluster_rows = enriched[
        enriched["ClusterID"].notna() & enriched["SCShortName"].notna()
    ].copy()
    compliance_by_cluster = _compliance_summary(
        cluster_rows, ["ClusterID", "SCShortName"]
    )
    compliance_by_cluster_period = _compliance_summary(
        cluster_rows, ["ClusterID", "SCShortName", "Year-Month"]
    )
    cluster_improvement_areas = _compliance_summary(
        cluster_rows,
        [
            "ClusterID",
            "SCShortName",
            "Role",
            "QDimension",
            "SProID",
            "QuestionName",
            "QuestionDetails",
        ],
    )
    cluster_improvement_areas = (
        cluster_improvement_areas.sort_values(
            ["ClusterID", "compliance_rate", "total_audit_items"],
            ascending=[True, True, False],
            kind="stable",
        )
        .groupby("ClusterID", dropna=False, sort=False)
        .head(10)
        .reset_index(drop=True)
    )
    chart_name = "compliance_by_profession.svg"
    chart_svg = _compliance_chart_svg(
        compliance_by_profession,
        report["dataset"]["date_period_min"],
        report["dataset"]["date_period_max"],
    )
    (OUTPUT_DIR / chart_name).write_text(chart_svg, encoding="utf-8")
    (AGENT_DIR / chart_name).write_text(chart_svg, encoding="utf-8")
    cluster_chart_name = "compliance_by_cluster.svg"
    cluster_chart_svg = _compliance_chart_svg(
        compliance_by_cluster,
        report["dataset"]["date_period_min"],
        report["dataset"]["date_period_max"],
        entity_column="SCShortName",
        detail_column="ClusterID",
        title="MRA Compliance Rate by Cluster",
    )
    (OUTPUT_DIR / cluster_chart_name).write_text(cluster_chart_svg, encoding="utf-8")
    (AGENT_DIR / cluster_chart_name).write_text(cluster_chart_svg, encoding="utf-8")
    payload = {
        "schema_version": "1.0",
        "run_id": report["pipeline"]["run_id"],
        "quality_summary": {
            "readiness_status": report["agent_readiness"]["status"],
            "fact_rows": report["dataset"]["fact_rows"],
            "join_coverage_rate": report["join"]["coverage_rate"],
            "failed_checks": report["agent_readiness"]["failed_checks"],
            "warning_checks": report["agent_readiness"]["warning_checks"],
            "date_period_min": report["dataset"]["date_period_min"],
            "date_period_max": report["dataset"]["date_period_max"],
        },
        "moi_mapping_summary": report["moi_mapping"]["coverage"],
        "ha_reconciliation_summary": {
            "period": report["ha_reconciliation"]["period"],
            "metrics": report["ha_reconciliation"]["metrics"],
        },
        "metric_definition": {
            "complete": "Value = 1",
            "incomplete": "Value = 0",
            "scored": "Value is not null",
            "completion_rate": "complete / scored",
            "power_bi_measure": "% Compliance",
            "total_score": "SUM(Value)",
            "total_audit_items": "COUNTROWS(MRA Data)",
            "compliance_rate": "total_score / total_audit_items",
            "warning": "Do not average compliance_rate; aggregate total_score and total_audit_items first",
        },
        "compliance_by_profession": _json_safe_records(compliance_by_profession),
        "compliance_by_profession_period": _json_safe_records(
            compliance_by_profession_period
        ),
        "compliance_by_cluster": _json_safe_records(compliance_by_cluster),
        "compliance_by_cluster_period": _json_safe_records(
            compliance_by_cluster_period
        ),
        "cluster_improvement_areas": _json_safe_records(
            cluster_improvement_areas
        ),
        "cluster_mapping_definition": {
            "source_file": SUBCLUSTER_REFERENCE_FILE,
            "join_key": "ClusterID",
            "cluster_name_column": "SCShortName",
            "improvement_area_limit_per_cluster": 10,
        },
        "presentation": {
            "compliance_chart_file": chart_name,
            "compliance_chart_path": "/data/agent/compliance_by_profession.svg",
            "cluster_compliance_chart_file": cluster_chart_name,
            "cluster_compliance_chart_path": "/data/agent/compliance_by_cluster.svg",
            "contains_patient_identifiers": False,
        },
        "aggregates": _json_safe_records(aggregate),
        "checks": report["checks"],
        "privacy": {
            "contains_patient_identifiers": False,
            "allowed_use": "aggregate_quality_analysis_only",
        },
    }
    write_json(OUTPUT_DIR / "agent_payload.json", payload)
    # Publish only the aggregate, privacy-reviewed payload to the directory
    # mounted read-only by n8n. Detailed patient-level output is never mounted.
    write_json(AGENT_DIR / "agent_payload.json", payload)
    (OUTPUT_DIR / "agent_prompt_th.txt").write_text(AGENT_PROMPT_TH, encoding="utf-8")

    manifest = {"created_at_utc": utc_now_iso(), "outputs": []}
    for path in sorted(OUTPUT_DIR.iterdir()):
        if path.is_file() and path.name not in {"run_manifest.json", "agentic_mra_airflow_output.zip"}:
            manifest["outputs"].append(
                {"file": path.name, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
            )
    write_json(OUTPUT_DIR / "run_manifest.json", manifest)
    # The archive must live outside OUTPUT_DIR; placing it inside the source
    # folder would make the ZIP include itself recursively.
    archive = shutil.make_archive(
        str(OUTPUT_DIR.parent / "agentic_mra_airflow_output"),
        "zip",
        root_dir=OUTPUT_DIR,
    )
    print(f"สร้าง Agent payload และ ZIP สำเร็จ: {archive}")
    return {"status": payload["quality_summary"]["readiness_status"], "archive": archive}


def notify_n8n() -> dict[str, Any]:
    ensure_directories()
    webhook_url = os.getenv("N8N_WEBHOOK_URL", "").strip()
    if not webhook_url:
        result = {
            "status": "SKIPPED",
            "reason": "ยังไม่ได้กำหนด N8N_WEBHOOK_URL",
            "created_at_utc": utc_now_iso(),
        }
        write_json(OUTPUT_DIR / "n8n_delivery_receipt.json", result)
        print(result["reason"])
        return result

    import requests

    payload = read_json(OUTPUT_DIR / "agent_payload.json")
    prompt = (OUTPUT_DIR / "agent_prompt_th.txt").read_text(encoding="utf-8")
    ai_enabled = os.getenv("N8N_AI_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    response = requests.post(
        webhook_url,
        json={
            "event_type": "mra_quality_report_ready",
            "source": "airflow",
            "request_ai_analysis": ai_enabled,
            "payload": payload,
            "prompt_th": prompt,
        },
        timeout=60,
    )
    response.raise_for_status()

    try:
        n8n_response: Any = response.json()
    except ValueError:
        n8n_response = {"text": response.text[:1000]}

    result = {
        "status": "SENT",
        "http_status": response.status_code,
        "webhook_url": webhook_url,
        "ai_requested": ai_enabled,
        "n8n_response": n8n_response,
        "created_at_utc": utc_now_iso(),
    }
    write_json(OUTPUT_DIR / "n8n_delivery_receipt.json", result)
    print(f"ส่งข้อมูลสรุปไป n8n สำเร็จ: HTTP {response.status_code}")
    return result


def human_review_ready() -> dict[str, Any]:
    report = read_json(OUTPUT_DIR / "quality_report.json")
    receipt_path = OUTPUT_DIR / "n8n_delivery_receipt.json"
    receipt = read_json(receipt_path) if receipt_path.is_file() else {"status": "NOT_RUN"}
    result = {
        "quality_status": report["agent_readiness"]["status"],
        "n8n_status": receipt["status"],
        "action": "โปรดให้มนุษย์ตรวจ Quality Report และอนุมัติผลการวิเคราะห์",
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result
