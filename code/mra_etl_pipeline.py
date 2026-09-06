from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PIPELINE_NAME = "agentic_mra_airflow_etl"
PIPELINE_VERSION = "1.0.0"

DATA_ROOT = Path(os.getenv("MRA_DATA_DIR", "/opt/airflow/data"))
INPUT_DIR = Path(os.getenv("MRA_INPUT_DIR", str(DATA_ROOT / "input")))
STAGING_DIR = Path(
    os.getenv("MRA_STAGING_DIR", str(DATA_ROOT / "staging" / "mra_agentic"))
)
OUTPUT_DIR = Path(
    os.getenv("MRA_OUTPUT_DIR", str(DATA_ROOT / "output" / "mra_agentic"))
)
AGENT_DIR = Path(os.getenv("MRA_AGENT_DIR", str(DATA_ROOT / "agent")))
REFERENCE_FILE = "ResultMRauditJCIHA.xlsx"

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
    required = [item["source_file"] for item in ROLE_CONFIG] + [REFERENCE_FILE]
    missing = [name for name in required if not (INPUT_DIR / name).is_file()]
    if missing:
        raise FileNotFoundError("ไม่พบไฟล์: " + ", ".join(missing))

    manifest = []
    for name in required:
        path = INPUT_DIR / name
        manifest.append(
            {
                "file": name,
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
    source_path = INPUT_DIR / config["source_file"]
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
    reference_path = INPUT_DIR / REFERENCE_FILE
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
    reference_path = INPUT_DIR / REFERENCE_FILE
    comparisons = []
    checks = []
    for config in ROLE_CONFIG:
        actual = pd.read_parquet(STAGING_DIR / f"{config['key']}_fact.parquet")
        reference = pd.read_excel(reference_path, sheet_name=config["sheet"], engine="openpyxl")
        comparison = _compare_to_reference(actual, reference, config["sheet"])
        comparison["source_newer_than_reference"] = bool(
            (INPUT_DIR / config["source_file"]).stat().st_mtime > reference_path.stat().st_mtime
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
    checks = (
        read_json(STAGING_DIR / "validation_checks.json")
        + read_json(STAGING_DIR / "join_checks.json")
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
                "ต้องเปิดเผย join coverage และ reference drift",
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
    enriched = pd.read_parquet(STAGING_DIR / "enriched.parquet")
    role_frames = {
        config["sheet"]: pd.read_parquet(STAGING_DIR / f"{config['key']}_fact.parquet")[FACT_COLUMNS]
        for config in ROLE_CONFIG
    }
    row_summary = pd.DataFrame(read_json(STAGING_DIR / "row_summary.json"))

    fact_path = OUTPUT_DIR / "audit_fact.parquet"
    enriched_path = OUTPUT_DIR / "audit_enriched.parquet"
    workbook_path = OUTPUT_DIR / "agentic_mra_airflow_output.xlsx"
    fact.to_parquet(fact_path, index=False)
    enriched.drop(columns="_join_status").to_parquet(enriched_path, index=False)

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
5. แจ้ง reference drift และ join coverage ทุกครั้งที่มีผลต่อข้อสรุป
6. ห้ามขอ แสดง หรืออนุมาน HN, AN ชื่อผู้ป่วย หรือข้อมูลส่วนบุคคล

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


def build_agent_payload() -> dict[str, Any]:
    ensure_directories()
    fact = pd.read_parquet(OUTPUT_DIR / "audit_fact.parquet")
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
    payload = {
        "schema_version": "1.0",
        "run_id": report["pipeline"]["run_id"],
        "quality_summary": {
            "readiness_status": report["agent_readiness"]["status"],
            "fact_rows": report["dataset"]["fact_rows"],
            "join_coverage_rate": report["join"]["coverage_rate"],
            "failed_checks": report["agent_readiness"]["failed_checks"],
            "warning_checks": report["agent_readiness"]["warning_checks"],
        },
        "metric_definition": {
            "complete": "Value = 1",
            "incomplete": "Value = 0",
            "scored": "Value is not null",
            "completion_rate": "complete / scored",
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
