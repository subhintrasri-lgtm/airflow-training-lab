from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


NEW_AUDIT_FORM_FILE = "NewAuditForm2025.xlsx"
MR_CODE_FILE = "MRCode.xlsx"
HA_REPORT_FILE = "HAReportJan2024-May2025.xlsm"
SUBCLUSTER_FILE = "SubClusterID.xlsb"
MOI_REFERENCE_FILES = [MR_CODE_FILE, HA_REPORT_FILE, NEW_AUDIT_FORM_FILE]

FORM_SHEETS = {
    "1.Doctor": ("Doctor", r"^Doctor\d+$"),
    "2.Nurse": ("Nurse", r"^Nurse\d+$"),
    "3.Pharmacist": ("Pharmacist", r"^Pharmacist\d+$"),
    "4.Dietitian": ("Dietitian", r"^Dietitian\d+$"),
    "5.Physiotherapist": ("Physiotherapist", r"^Physiotherapist\d+$"),
    "6.CSR": ("CSR", r"^CSR\d+$"),
    "7.Lab": ("Lab", r"^Lab\d+$"),
    "8.X-Ray": ("XRay", r"^XRay\d+$"),
}


def _clean_text(value: Any):
    if value is None or pd.isna(value):
        return pd.NA
    if isinstance(value, (float, np.floating)) and float(value).is_integer():
        return str(int(value))
    value = str(value).strip()
    return value if value else pd.NA


def _clean_key(series: pd.Series) -> pd.Series:
    return series.map(_clean_text).astype("string")


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


def _find_header_row(path: Path, sheet_name: str, expected: str) -> int:
    preview = pd.read_excel(
        path,
        sheet_name=sheet_name,
        engine="openpyxl",
        header=None,
        nrows=30,
        usecols=range(0, 9),
    )
    first_column = preview.iloc[:, 0].map(_clean_text)
    matches = first_column.index[first_column.eq(expected)].tolist()
    if not matches:
        raise ValueError(f"ไม่พบ header {expected} ในชีต {sheet_name}")
    return int(matches[0])


def _metadata_conflict_count(frame: pd.DataFrame, key: str) -> int:
    metadata = [column for column in frame.columns if column != key]
    if not metadata:
        return 0
    signatures = (
        frame[metadata]
        .fillna("")
        .astype("string")
        .agg("\x1f".join, axis=1)
    )
    work = pd.DataFrame({key: frame[key], "_signature": signatures})
    return int((work.groupby(key, dropna=False)["_signature"].nunique() > 1).sum())


def _read_question_dimension(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    frames = []
    sheet_metrics = []
    for sheet_name, (role, pattern) in FORM_SHEETS.items():
        header_row = _find_header_row(path, sheet_name, "SProID")
        frame = pd.read_excel(
            path,
            sheet_name=sheet_name,
            engine="openpyxl",
            header=header_row,
            usecols=range(0, 6),
            dtype=object,
        )
        required = ["SProID", "DocNum", "DocName", "Group", "Details", "Profession"]
        missing = [column for column in required if column not in frame.columns]
        if missing:
            raise ValueError(f"{sheet_name} ขาดคอลัมน์: {', '.join(missing)}")
        frame = frame[required].copy()
        for column in required:
            frame[column] = frame[column].map(_clean_text)
        frame = frame[
            frame["SProID"].astype("string").str.match(pattern, na=False)
        ].copy()
        frame["Role"] = role
        frames.append(frame)
        sheet_metrics.append(
            {
                "sheet": sheet_name,
                "role": role,
                "header_row_1_based": header_row + 1,
                "rows": int(len(frame)),
            }
        )

    combined = pd.concat(frames, ignore_index=True)
    duplicate_rows = int(combined.duplicated("SProID", keep=False).sum())
    conflict_keys = _metadata_conflict_count(combined, "SProID")
    combined = (
        combined.sort_values(["Role", "SProID"], kind="stable")
        .drop_duplicates("SProID", keep="first")
        .reset_index(drop=True)
    )
    metrics = {
        "rows": int(len(combined)),
        "duplicate_rows_before_deduplication": duplicate_rows,
        "conflicting_sproid_keys": conflict_keys,
        "sheets": sheet_metrics,
    }
    return combined, metrics


def _read_dimension(
    path: Path,
    sheet_name: str,
    columns: list[str],
    key: str,
    rename: dict[str, str] | None = None,
    engine: str = "openpyxl",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = pd.read_excel(path, sheet_name=sheet_name, engine=engine, dtype=object)
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{sheet_name} ขาดคอลัมน์: {', '.join(missing)}")
    frame = frame[columns].copy()
    for column in columns:
        frame[column] = frame[column].map(_clean_text)
    frame = frame[frame[key].notna()].copy()
    conflict_keys = _metadata_conflict_count(frame, key)
    duplicate_rows = int(frame.duplicated(key, keep=False).sum())
    frame = frame.drop_duplicates(key, keep="first").reset_index(drop=True)
    if rename:
        frame = frame.rename(columns=rename)
    return frame, {
        "sheet": sheet_name,
        "rows": int(len(frame)),
        "duplicate_rows_before_deduplication": duplicate_rows,
        "conflicting_keys": conflict_keys,
    }


def build_moi_reference_dimensions(
    moi_input_dir: str | Path,
    staging_dir: str | Path,
) -> dict[str, Any]:
    moi_input_dir = Path(moi_input_dir)
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    question, question_metrics = _read_question_dimension(
        moi_input_dir / NEW_AUDIT_FORM_FILE
    )
    mr_code_path = moi_input_dir / MR_CODE_FILE
    mr_cluster, mr_cluster_metrics = _read_dimension(
        mr_code_path,
        "Cluster",
        ["ClusterName", "ClusterID", "Group"],
        "ClusterID",
        {"ClusterName": "MRClusterName", "Group": "MappedClusterGroup"},
    )
    subcluster, subcluster_metrics = _read_dimension(
        moi_input_dir / SUBCLUSTER_FILE,
        "SubClusterID",
        ["ClusterID", "SCShortName"],
        "ClusterID",
        engine="pyxlsb",
    )
    cluster = subcluster.merge(
        mr_cluster,
        on="ClusterID",
        how="left",
        validate="one_to_one",
    )
    cluster["MappedClusterName"] = cluster["SCShortName"]
    cluster = cluster[
        [
            "ClusterID",
            "SCShortName",
            "MappedClusterName",
            "MappedClusterGroup",
            "MRClusterName",
        ]
    ]
    cluster_metrics = {
        **subcluster_metrics,
        "rows": int(len(cluster)),
        "mr_cluster_rows": int(mr_cluster_metrics["rows"]),
        "mr_cluster_ids_matched": int(cluster["MRClusterName"].notna().sum()),
        "cluster_name_source": "SCShortName",
    }
    ward, ward_metrics = _read_dimension(
        mr_code_path,
        "Ward",
        ["DischargeWardName", "D/C Ward"],
        "DischargeWardName",
        {"D/C Ward": "MappedWard"},
    )
    icd10, icd_metrics = _read_dimension(
        mr_code_path,
        "ICD10",
        ["MainICD", "MainICDName"],
        "MainICD",
        {"MainICDName": "MappedMainICDName"},
    )
    doctor, doctor_metrics = _read_dimension(
        mr_code_path,
        "MasterDoctor",
        ["DoctorCode"],
        "DoctorCode",
    )
    doctor["DoctorCodeInMaster"] = 1

    question.to_parquet(staging_dir / "moi_question_dimension.parquet", index=False)
    cluster.to_parquet(staging_dir / "moi_cluster_dimension.parquet", index=False)
    ward.to_parquet(staging_dir / "moi_ward_dimension.parquet", index=False)
    icd10.to_parquet(staging_dir / "moi_icd10_dimension.parquet", index=False)
    doctor.to_parquet(staging_dir / "moi_doctor_code_dimension.parquet", index=False)

    dimension_metrics = {
        "question": question_metrics,
        "cluster": cluster_metrics,
        "subcluster": subcluster_metrics,
        "ward": ward_metrics,
        "icd10": icd_metrics,
        "doctor_code": doctor_metrics,
    }
    checks = []
    for name, metrics in dimension_metrics.items():
        conflicts = int(
            metrics.get("conflicting_sproid_keys", metrics.get("conflicting_keys", 0))
        )
        conflict_status = "WARN" if name == "question" else "FAIL"
        conflict_severity = "HIGH" if name == "question" else "CRITICAL"
        checks.append(
            _check(
                f"DQ010_MOI_{name.upper()}_KEYS",
                name,
                "reference_uniqueness",
                "PASS" if conflicts == 0 else conflict_status,
                conflict_severity,
                "คีย์ Mapping ไม่ขัดแย้งกัน"
                if conflicts == 0
                else "พบคีย์ Mapping เดียวกันแต่รายละเอียดขัดแย้งกัน",
                {
                    "rows": int(metrics["rows"]),
                    "conflicting_keys": conflicts,
                    "duplicate_rows_before_deduplication": int(
                        metrics.get("duplicate_rows_before_deduplication", 0)
                    ),
                },
            )
        )

    return {"metrics": dimension_metrics, "checks": checks}


def _coverage_metric(
    frame: pd.DataFrame,
    source_key: str,
    mapped_column: str,
    mapping_name: str,
    threshold: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    eligible = frame[source_key].notna()
    matched = eligible & frame[mapped_column].notna()
    eligible_rows = int(eligible.sum())
    matched_rows = int(matched.sum())
    coverage = matched_rows / eligible_rows if eligible_rows else None
    passed = coverage is not None and coverage >= threshold
    metrics = {
        "mapping": mapping_name,
        "eligible_rows": eligible_rows,
        "matched_rows": matched_rows,
        "unmatched_rows": eligible_rows - matched_rows,
        "coverage_rate": round(coverage, 6) if coverage is not None else None,
        "threshold": threshold,
    }
    check = _check(
        f"DQ011_MOI_{mapping_name.upper()}_COVERAGE",
        mapping_name,
        "mapping_coverage",
        "PASS" if passed else "WARN",
        "HIGH",
        "Mapping coverage ผ่านเกณฑ์" if passed else "Mapping coverage ต่ำกว่าเกณฑ์",
        metrics,
    )
    return metrics, check


def map_moi_references(staging_dir: str | Path) -> dict[str, Any]:
    staging_dir = Path(staging_dir)
    enriched = pd.read_parquet(staging_dir / "enriched.parquet")
    question = pd.read_parquet(staging_dir / "moi_question_dimension.parquet").rename(
        columns={
            "DocNum": "QuestionDocNum",
            "DocName": "QuestionName",
            "Group": "QuestionGroup",
            "Details": "QuestionDetails",
            "Profession": "QuestionProfession",
            "Role": "QuestionRole",
        }
    )
    cluster = pd.read_parquet(staging_dir / "moi_cluster_dimension.parquet")
    ward = pd.read_parquet(staging_dir / "moi_ward_dimension.parquet")
    icd10 = pd.read_parquet(staging_dir / "moi_icd10_dimension.parquet")
    doctor = pd.read_parquet(staging_dir / "moi_doctor_code_dimension.parquet")

    for column in ["SProID", "ClusterID", "DischargeWardName", "MainICD", "DoctorCode"]:
        enriched[column] = _clean_key(enriched[column])
    for frame, key in [
        (question, "SProID"),
        (cluster, "ClusterID"),
        (ward, "DischargeWardName"),
        (icd10, "MainICD"),
        (doctor, "DoctorCode"),
    ]:
        frame[key] = _clean_key(frame[key])

    enriched = enriched.merge(question, on="SProID", how="left", validate="many_to_one")
    enriched = enriched.merge(cluster, on="ClusterID", how="left", validate="many_to_one")
    enriched = enriched.merge(ward, on="DischargeWardName", how="left", validate="many_to_one")
    enriched = enriched.merge(icd10, on="MainICD", how="left", validate="many_to_one")
    enriched = enriched.merge(doctor, on="DoctorCode", how="left", validate="many_to_one")
    enriched["DoctorCodeInMaster"] = (
        pd.to_numeric(enriched["DoctorCodeInMaster"], errors="coerce")
        .fillna(0)
        .astype("int8")
    )

    specs = [
        ("SProID", "QuestionName", "question", 0.95),
        ("ClusterID", "MappedClusterName", "cluster", 0.90),
        ("DischargeWardName", "MappedWard", "ward", 0.90),
        ("MainICD", "MappedMainICDName", "icd10", 0.90),
        ("DoctorCode", "DoctorCodeInMaster", "doctor_code", 0.90),
    ]
    metrics = []
    checks = []
    for source_key, mapped_column, name, threshold in specs:
        if name == "doctor_code":
            eligible = enriched[source_key].notna()
            matched = eligible & enriched[mapped_column].eq(1)
            eligible_rows = int(eligible.sum())
            matched_rows = int(matched.sum())
            coverage = matched_rows / eligible_rows if eligible_rows else None
            metric = {
                "mapping": name,
                "eligible_rows": eligible_rows,
                "matched_rows": matched_rows,
                "unmatched_rows": eligible_rows - matched_rows,
                "coverage_rate": round(coverage, 6) if coverage is not None else None,
                "threshold": threshold,
            }
            passed = coverage is not None and coverage >= threshold
            check = _check(
                "DQ011_MOI_DOCTOR_CODE_COVERAGE",
                name,
                "mapping_coverage",
                "PASS" if passed else "WARN",
                "HIGH",
                "Mapping coverage ผ่านเกณฑ์" if passed else "Mapping coverage ต่ำกว่าเกณฑ์",
                metric,
            )
        else:
            metric, check = _coverage_metric(
                enriched, source_key, mapped_column, name, threshold
            )
        metrics.append(metric)
        checks.append(check)

    enriched.to_parquet(staging_dir / "moi_enriched.parquet", index=False)
    return {"rows": int(len(enriched)), "mappings": metrics, "checks": checks}


def _normalise_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")


def _key_set(frame: pd.DataFrame, columns: list[str]) -> set[tuple[str, ...]]:
    work = frame[columns].copy()
    for column in columns:
        work[column] = _clean_key(work[column])
    work = work.dropna(subset=columns)
    return set(map(tuple, work[columns].astype(str).itertuples(index=False, name=None)))


def _reconciliation_metric(name: str, audit_keys, reference_keys, threshold=0.80):
    matched = len(audit_keys & reference_keys)
    eligible = len(audit_keys)
    coverage = matched / eligible if eligible else None
    metric = {
        "scope": name,
        "audit_distinct_encounters": eligible,
        "ha_report_distinct_encounters": len(reference_keys),
        "matched_encounters": matched,
        "audit_only_encounters": len(audit_keys - reference_keys),
        "ha_report_only_encounters": len(reference_keys - audit_keys),
        "coverage_rate": round(coverage, 6) if coverage is not None else None,
        "threshold": threshold,
    }
    passed = coverage is not None and coverage >= threshold
    if coverage is None:
        message = "ไม่มีข้อมูล Audit ในช่วงเวลาของ HA Report จึงบันทึกเป็นรายการให้ตรวจสอบ"
    elif passed:
        message = "การตรวจเทียบกับ HA Report ผ่านเกณฑ์"
    else:
        message = "การตรวจเทียบกับ HA Report ต่ำกว่าเกณฑ์"
    return metric, _check(
        f"DQ012_HA_{name.upper()}_RECONCILIATION",
        name,
        "reference_reconciliation",
        "PASS" if passed else "WARN",
        "MEDIUM",
        message,
        metric,
    )


def reconcile_ha_report(
    moi_input_dir: str | Path,
    staging_dir: str | Path,
) -> dict[str, Any]:
    moi_input_dir = Path(moi_input_dir)
    staging_dir = Path(staging_dir)
    enriched = pd.read_parquet(staging_dir / "moi_enriched.parquet")
    period = pd.to_datetime(enriched["Year-Month"], format="%Y-%b", errors="coerce")
    in_scope = enriched[
        period.between(pd.Timestamp("2024-01-01"), pd.Timestamp("2025-05-31"))
    ].copy()

    ha_path = moi_input_dir / HA_REPORT_FILE
    ha_ipd = pd.read_excel(
        ha_path,
        sheet_name="Analysis IPD",
        engine="openpyxl",
        usecols=lambda column: column in {"HN", "AN"},
        dtype=object,
    )
    ha_opd = pd.read_excel(
        ha_path,
        sheet_name="Analysis OPD",
        engine="openpyxl",
        usecols=lambda column: column in {"HN", "Visit Date"},
        dtype=object,
    )

    audit_ipd = in_scope[in_scope["Group"].isin(["IPD", "Both"])][["HN", "AN"]]
    audit_ipd_keys = _key_set(audit_ipd, ["HN", "AN"])
    ha_ipd_keys = _key_set(ha_ipd, ["HN", "AN"])

    audit_opd = in_scope[in_scope["Group"].isin(["OPD", "Both"])][
        ["HN", "AdmDateTime"]
    ].copy()
    audit_opd["VisitDate"] = _normalise_date(audit_opd["AdmDateTime"])
    ha_opd["VisitDate"] = _normalise_date(ha_opd["Visit Date"])
    audit_opd_keys = _key_set(audit_opd, ["HN", "VisitDate"])
    ha_opd_keys = _key_set(ha_opd, ["HN", "VisitDate"])

    metrics = []
    checks = []
    for name, audit_keys, reference_keys in [
        ("IPD", audit_ipd_keys, ha_ipd_keys),
        ("OPD", audit_opd_keys, ha_opd_keys),
    ]:
        metric, check = _reconciliation_metric(name, audit_keys, reference_keys)
        metrics.append(metric)
        checks.append(check)
    return {"period": "2024-01 through 2025-05", "metrics": metrics, "checks": checks}
