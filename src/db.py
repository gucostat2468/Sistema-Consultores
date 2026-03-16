from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json
import os
import re
import sqlite3
import unicodedata

from src.auth import hash_password, slugify_username, verify_password
from src.credit_excel_parser import CreditLimitRecord
from src.pdf_parser import ReceivableRecord


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "consultores.db"

DEFAULT_FINANCIAL_READ_USERNAMES = {"vitor_financeiro"}
EXTRA_FINANCIAL_READ_USERNAMES = {
    str(item or "").strip().lower()
    for item in os.getenv("FINANCIAL_USERNAMES", "").split(",")
    if str(item or "").strip()
}
FINANCIAL_READ_USERNAMES = DEFAULT_FINANCIAL_READ_USERNAMES | EXTRA_FINANCIAL_READ_USERNAMES


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS consultants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    email TEXT,
    is_admin INTEGER NOT NULL DEFAULT 0 CHECK (is_admin IN (0, 1)),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    customer_code TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, customer_code)
);

CREATE TABLE IF NOT EXISTS consultant_customers (
    consultant_id INTEGER NOT NULL REFERENCES consultants(id) ON DELETE CASCADE,
    customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consultant_id, customer_id)
);

CREATE TABLE IF NOT EXISTS receivables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    consultant_id INTEGER NOT NULL REFERENCES consultants(id) ON DELETE CASCADE,
    customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE RESTRICT,
    source_file TEXT NOT NULL,
    source_page INTEGER NOT NULL,
    report_generated_at TEXT,
    document_id TEXT NOT NULL,
    document_ref TEXT NOT NULL,
    note_number TEXT NOT NULL,
    installment TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_date TEXT NOT NULL,
    due_date TEXT NOT NULL,
    balance_cents INTEGER NOT NULL,
    installment_value_cents INTEGER NOT NULL,
    raw_line TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (
        consultant_id,
        document_id,
        document_ref,
        installment,
        due_date,
        note_number,
        balance_cents,
        installment_value_cents
    )
);

CREATE INDEX IF NOT EXISTS idx_receivables_consultant_due_date
    ON receivables (consultant_id, due_date);
CREATE INDEX IF NOT EXISTS idx_receivables_customer
    ON receivables (customer_id);

CREATE TABLE IF NOT EXISTS credit_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    consultant_id INTEGER NOT NULL REFERENCES consultants(id) ON DELETE CASCADE,
    customer_name TEXT NOT NULL,
    cnpj TEXT,
    cnpj_key TEXT NOT NULL DEFAULT '',
    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    source_section TEXT NOT NULL,
    credit_limit_cents INTEGER,
    credit_used_cents INTEGER,
    credit_available_cents INTEGER,
    limit_policy TEXT,
    note TEXT,
    updated_at TEXT,
    raw_limit_value TEXT,
    raw_used_value TEXT,
    raw_available_value TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (consultant_id, customer_name, cnpj_key)
);

CREATE INDEX IF NOT EXISTS idx_credit_limits_consultant
    ON credit_limits (consultant_id);
CREATE INDEX IF NOT EXISTS idx_credit_limits_customer_name
    ON credit_limits (customer_name);

CREATE TABLE IF NOT EXISTS import_format_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    sheet_name TEXT,
    header_signature TEXT,
    detected_columns_json TEXT,
    records_detected INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_import_format_profiles_kind_created
    ON import_format_profiles (source_kind, created_at DESC);

CREATE TABLE IF NOT EXISTS ingestion_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL UNIQUE,
    mode TEXT NOT NULL,
    actor_username TEXT NOT NULL,
    strict_vendors INTEGER NOT NULL DEFAULT 0 CHECK (strict_vendors IN (0, 1)),
    append_mode INTEGER NOT NULL DEFAULT 0 CHECK (append_mode IN (0, 1)),
    status TEXT NOT NULL DEFAULT 'started',
    message TEXT,
    audit_json TEXT,
    warnings_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_batches_created
    ON ingestion_batches (created_at DESC);

CREATE TABLE IF NOT EXISTS ingestion_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    file_type TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    meta_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingestion_files_batch
    ON ingestion_files (batch_id, created_at ASC);

CREATE TABLE IF NOT EXISTS staging_receivables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    file_id INTEGER REFERENCES ingestion_files(id) ON DELETE SET NULL,
    source_kind TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_page INTEGER NOT NULL DEFAULT 1,
    report_generated_at TEXT,
    vendor_name TEXT NOT NULL,
    vendor_key TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_key TEXT NOT NULL,
    customer_code TEXT,
    status TEXT NOT NULL,
    document_id TEXT NOT NULL,
    document_ref TEXT NOT NULL,
    note_number TEXT NOT NULL,
    installment TEXT NOT NULL,
    issue_date TEXT NOT NULL,
    due_date TEXT NOT NULL,
    balance_cents INTEGER NOT NULL,
    installment_value_cents INTEGER NOT NULL,
    raw_line TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_receivables_batch
    ON staging_receivables (batch_id, customer_key, due_date);

CREATE TABLE IF NOT EXISTS staging_credit_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES ingestion_batches(id) ON DELETE CASCADE,
    file_id INTEGER REFERENCES ingestion_files(id) ON DELETE SET NULL,
    source_kind TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row INTEGER NOT NULL DEFAULT 0,
    source_section TEXT NOT NULL,
    consultant_name TEXT NOT NULL,
    consultant_key TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_key TEXT NOT NULL,
    cnpj TEXT,
    cnpj_key TEXT NOT NULL DEFAULT '',
    credit_limit_cents INTEGER,
    credit_used_cents INTEGER,
    credit_available_cents INTEGER,
    limit_policy TEXT,
    note TEXT,
    updated_at TEXT,
    raw_limit_value TEXT,
    raw_used_value TEXT,
    raw_available_value TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_credit_limits_batch
    ON staging_credit_limits (batch_id, customer_key);

CREATE TABLE IF NOT EXISTS order_email_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    isabel_emails TEXT NOT NULL DEFAULT '',
    vitor_emails TEXT NOT NULL DEFAULT '',
    marcos_emails TEXT NOT NULL DEFAULT '',
    updated_by_user_id INTEGER REFERENCES consultants(id) ON DELETE SET NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    order_number TEXT NOT NULL,
    consultant_id INTEGER NOT NULL REFERENCES consultants(id) ON DELETE RESTRICT,
    requested_by_user_id INTEGER NOT NULL REFERENCES consultants(id) ON DELETE RESTRICT,
    customer_code TEXT,
    customer_name TEXT NOT NULL,
    customer_id_doc TEXT,
    order_value_cents INTEGER NOT NULL,
    open_balance_cents INTEGER NOT NULL DEFAULT 0,
    credit_limit_cents INTEGER NOT NULL DEFAULT 0,
    over_limit_cents INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    status_reason TEXT,
    extracted_json TEXT,
    original_pdf_path TEXT NOT NULL,
    signed_pdf_path TEXT,
    analysis_pdf_path TEXT,
    package_pdf_path TEXT,
    signature_mode TEXT,
    signature_hash TEXT,
    signature_canvas_path TEXT,
    signed_by_user_id INTEGER REFERENCES consultants(id) ON DELETE SET NULL,
    signed_at TEXT,
    returned_reason TEXT,
    distribution_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_order_requests_status_created
    ON order_requests (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_requests_consultant_created
    ON order_requests (consultant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_requests_requested_by
    ON order_requests (requested_by_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS order_request_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_request_id INTEGER NOT NULL REFERENCES order_requests(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    actor_user_id INTEGER REFERENCES consultants(id) ON DELETE SET NULL,
    actor_name TEXT,
    from_status TEXT,
    to_status TEXT,
    message TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_order_request_events_order_created
    ON order_request_events (order_request_id, created_at DESC);

CREATE TABLE IF NOT EXISTS order_email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_request_id INTEGER NOT NULL REFERENCES order_requests(id) ON DELETE CASCADE,
    email_kind TEXT NOT NULL,
    recipient TEXT NOT NULL,
    subject TEXT NOT NULL,
    success INTEGER NOT NULL DEFAULT 0 CHECK (success IN (0, 1)),
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_order_email_logs_order
    ON order_email_logs (order_request_id, created_at DESC);
"""


@dataclass(frozen=True)
class AuthenticatedUser:
    id: int
    name: str
    username: str
    is_admin: bool
    email: str | None = None


@dataclass
class ImportSummary:
    total_records_seen: int = 0
    imported_rows: int = 0
    updated_rows: int = 0
    duplicated_rows: int = 0
    deduplicated_rows: int = 0
    skipped_rows: int = 0
    new_customers: int = 0
    new_consultants: int = 0
    created_consultants: dict[str, str] = field(default_factory=dict)


@dataclass
class CreditImportSummary:
    total_records_seen: int = 0
    imported_rows: int = 0
    updated_rows: int = 0
    skipped_rows: int = 0
    unresolved_consultants: dict[str, int] = field(default_factory=dict)


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        conn.executescript(SCHEMA_SQL)
        ensure_schema_upgrades(conn)


def ensure_schema_upgrades(conn: sqlite3.Connection) -> None:
    consultant_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(consultants)").fetchall()
    }
    if "email" not in consultant_columns:
        conn.execute("ALTER TABLE consultants ADD COLUMN email TEXT")

    order_request_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(order_requests)").fetchall()
    }
    if "analysis_pdf_path" not in order_request_columns:
        conn.execute("ALTER TABLE order_requests ADD COLUMN analysis_pdf_path TEXT")
    if "package_pdf_path" not in order_request_columns:
        conn.execute("ALTER TABLE order_requests ADD COLUMN package_pdf_path TEXT")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def authenticate_user(username: str, password: str) -> AuthenticatedUser | None:
    username_clean = str(username).strip()
    if not username_clean:
        return None

    username_key = username_clean.casefold()
    candidates = [username_key]
    if username_key == "adm":
        candidates.append("admin")
    elif username_key == "admin":
        candidates.append("adm")

    row = None
    with get_connection() as conn:
        for candidate in candidates:
            row = conn.execute(
                """
                SELECT id, name, username, password_hash, is_admin, email
                FROM consultants
                WHERE lower(username) = ? AND is_active = 1
                """,
                (candidate,),
            ).fetchone()
            if row:
                break

    if not row:
        return None
    if not verify_password(password, row["password_hash"]):
        return None

    return AuthenticatedUser(
        id=row["id"],
        name=row["name"],
        username=row["username"],
        is_admin=bool(row["is_admin"]),
        email=row["email"],
    )


def ensure_admin_user(password: str, username: str = "admin") -> None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM consultants WHERE username = ?",
            (username,),
        ).fetchone()
        if row:
            return
        conn.execute(
            """
            INSERT INTO consultants (name, username, password_hash, is_admin)
            VALUES (?, ?, ?, 1)
            """,
            ("Administrador", username, hash_password(password)),
        )


def list_consultants() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, username, email, is_admin
            FROM consultants
            WHERE is_active = 1
            ORDER BY is_admin DESC, name ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def get_consultant_by_id(consultant_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, name, username, email, is_admin, is_active
            FROM consultants
            WHERE id = ?
            """,
            (int(consultant_id),),
        ).fetchone()
    return dict(row) if row else None


def list_active_admin_users() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, username, email
            FROM consultants
            WHERE is_active = 1 AND is_admin = 1
            ORDER BY name ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def update_consultant_email(*, consultant_id: int, email: str | None) -> None:
    value = normalize_spaces(email) if email else None
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE consultants
            SET email = ?
            WHERE id = ?
            """,
            (value, int(consultant_id)),
        )


def import_receivables(
    records: list[ReceivableRecord],
    *,
    source_file: str,
    default_password: str,
    wipe_existing: bool = True,
) -> ImportSummary:
    summary = ImportSummary(total_records_seen=len(records))
    if not records:
        return summary

    with get_connection() as conn:
        if wipe_existing:
            conn.execute("DELETE FROM receivables")
            conn.execute("DELETE FROM consultant_customers")
            conn.execute("DELETE FROM customers")

        existing_usernames = {
            row["username"] for row in conn.execute("SELECT username FROM consultants")
        }
        consultant_cache: dict[str, int] = {}
        customer_cache: dict[tuple[int, str], int] = {}

        for record in records:
            consultant_name = normalize_spaces(record.vendor_name)
            customer_name = normalize_spaces(record.customer_name)
            customer_code = normalize_customer_code(record.customer_code)

            consultant_id = consultant_cache.get(consultant_name)
            if consultant_id is None:
                consultant_id, created_username = get_or_create_consultant(
                    conn=conn,
                    consultant_name=consultant_name,
                    default_password=default_password,
                    existing_usernames=existing_usernames,
                )
                consultant_cache[consultant_name] = consultant_id
                if created_username:
                    summary.created_consultants[consultant_name] = created_username
                    summary.new_consultants += 1

            customer_key = (consultant_id, normalize_name_key(customer_name))
            customer_id = customer_cache.get(customer_key)
            if customer_id is None:
                customer_id, merged_duplicates, customer_created = get_or_create_customer(
                    conn=conn,
                    consultant_id=consultant_id,
                    customer_name=customer_name,
                    customer_code=customer_code,
                )
                summary.deduplicated_rows += merged_duplicates
                if customer_created:
                    summary.new_customers += 1
                customer_cache[customer_key] = customer_id

            conn.execute(
                """
                INSERT OR IGNORE INTO consultant_customers (consultant_id, customer_id)
                VALUES (?, ?)
                """,
                (consultant_id, customer_id),
            )
            existing_rows = conn.execute(
                """
                SELECT
                    id,
                    customer_id,
                    source_file,
                    source_page,
                    report_generated_at,
                    status,
                    issue_date,
                    balance_cents,
                    installment_value_cents,
                    raw_line
                FROM receivables
                WHERE consultant_id = ?
                  AND document_id = ?
                  AND document_ref = ?
                  AND installment = ?
                  AND due_date = ?
                  AND note_number = ?
                ORDER BY id ASC
                """,
                (
                    consultant_id,
                    record.document_id,
                    record.document_ref,
                    record.installment,
                    record.due_date,
                    record.note_number,
                ),
            ).fetchall()

            if not existing_rows:
                conn.execute(
                    """
                    INSERT INTO receivables (
                        consultant_id,
                        customer_id,
                        source_file,
                        source_page,
                        report_generated_at,
                        document_id,
                        document_ref,
                        note_number,
                        installment,
                        status,
                        issue_date,
                        due_date,
                        balance_cents,
                        installment_value_cents,
                        raw_line
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        consultant_id,
                        customer_id,
                        source_file,
                        record.source_page,
                        record.report_generated_at,
                        record.document_id,
                        record.document_ref,
                        record.note_number,
                        record.installment,
                        record.status,
                        record.issue_date,
                        record.due_date,
                        record.balance_cents,
                        record.installment_value_cents,
                        record.raw_line,
                    ),
                )
                summary.imported_rows += 1
                continue

            canonical = max(
                existing_rows,
                key=lambda row: (
                    parse_optional_iso_datetime(row["report_generated_at"]) or datetime.min,
                    int(row["id"]),
                ),
            )
            canonical_id = int(canonical["id"])
            duplicate_ids = [int(row["id"]) for row in existing_rows if int(row["id"]) != canonical_id]
            if duplicate_ids:
                placeholders = ",".join("?" for _ in duplicate_ids)
                conn.execute(
                    f"DELETE FROM receivables WHERE id IN ({placeholders})",
                    tuple(duplicate_ids),
                )
                summary.deduplicated_rows += len(duplicate_ids)

            changed = any(
                (
                    canonical["customer_id"] != customer_id,
                    canonical["source_file"] != source_file,
                    int(canonical["source_page"]) != int(record.source_page),
                    canonical["report_generated_at"] != record.report_generated_at,
                    canonical["status"] != record.status,
                    canonical["issue_date"] != record.issue_date,
                    int(canonical["balance_cents"]) != int(record.balance_cents),
                    int(canonical["installment_value_cents"]) != int(record.installment_value_cents),
                    canonical["raw_line"] != record.raw_line,
                )
            )
            if changed:
                if not should_apply_snapshot_update(
                    canonical["report_generated_at"],
                    record.report_generated_at,
                ):
                    summary.skipped_rows += 1
                    continue
                conn.execute(
                    """
                    UPDATE receivables
                    SET
                        customer_id = ?,
                        source_file = ?,
                        source_page = ?,
                        report_generated_at = ?,
                        status = ?,
                        issue_date = ?,
                        balance_cents = ?,
                        installment_value_cents = ?,
                        raw_line = ?
                    WHERE id = ?
                    """,
                    (
                        customer_id,
                        source_file,
                        record.source_page,
                        record.report_generated_at,
                        record.status,
                        record.issue_date,
                        record.balance_cents,
                        record.installment_value_cents,
                        record.raw_line,
                        canonical_id,
                    ),
                )
                summary.updated_rows += 1
            else:
                summary.duplicated_rows += 1

    return summary


def import_credit_limits(
    records: list[CreditLimitRecord],
    *,
    source_file: str,
    wipe_existing: bool = True,
) -> CreditImportSummary:
    summary = CreditImportSummary(total_records_seen=len(records))
    if not records:
        return summary

    with get_connection() as conn:
        if wipe_existing:
            conn.execute("DELETE FROM credit_limits")

        consultant_rows = conn.execute(
            "SELECT id, name FROM consultants WHERE is_active = 1"
        ).fetchall()
        consultant_by_name = {normalize_name_key(row["name"]): row["id"] for row in consultant_rows}

        for record in records:
            consultant_id = resolve_consultant_id(record.consultant_name, consultant_by_name)
            if consultant_id is None:
                key = " ".join(record.consultant_name.split()) if record.consultant_name else "<vazio>"
                summary.unresolved_consultants[key] = summary.unresolved_consultants.get(key, 0) + 1
                summary.skipped_rows += 1
                continue

            customer_name = " ".join(record.customer_name.split())
            cnpj_value = normalize_cnpj(record.cnpj)
            cnpj_key = cnpj_value or ""

            exists = conn.execute(
                """
                SELECT id, updated_at
                FROM credit_limits
                WHERE consultant_id = ?
                  AND customer_name = ?
                  AND cnpj_key = ?
                """,
                (consultant_id, customer_name, cnpj_key),
            ).fetchone()

            if exists and not should_apply_snapshot_update(exists["updated_at"], record.updated_at):
                summary.skipped_rows += 1
                continue

            conn.execute(
                """
                INSERT INTO credit_limits (
                    consultant_id,
                    customer_name,
                    cnpj,
                    cnpj_key,
                    source_file,
                    source_sheet,
                    source_row,
                    source_section,
                    credit_limit_cents,
                    credit_used_cents,
                    credit_available_cents,
                    limit_policy,
                    note,
                    updated_at,
                    raw_limit_value,
                    raw_used_value,
                    raw_available_value
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (consultant_id, customer_name, cnpj_key)
                DO UPDATE SET
                    source_file = excluded.source_file,
                    source_sheet = excluded.source_sheet,
                    source_row = excluded.source_row,
                    source_section = excluded.source_section,
                    credit_limit_cents = excluded.credit_limit_cents,
                    credit_used_cents = excluded.credit_used_cents,
                    credit_available_cents = excluded.credit_available_cents,
                    limit_policy = excluded.limit_policy,
                    note = excluded.note,
                    updated_at = excluded.updated_at,
                    raw_limit_value = excluded.raw_limit_value,
                    raw_used_value = excluded.raw_used_value,
                    raw_available_value = excluded.raw_available_value,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    consultant_id,
                    customer_name,
                    cnpj_value,
                    cnpj_key,
                    source_file,
                    record.source_sheet,
                    record.source_row,
                    record.source_section,
                    record.credit_limit_cents,
                    record.credit_used_cents,
                    record.credit_available_cents,
                    record.limit_policy,
                    record.note,
                    record.updated_at,
                    record.raw_limit_value,
                    record.raw_used_value,
                    record.raw_available_value,
                ),
            )

            if exists:
                summary.updated_rows += 1
            else:
                summary.imported_rows += 1

    return summary


def fetch_receivables_for_user(
    user: AuthenticatedUser,
    selected_consultant_id: int | None = None,
) -> list[dict]:
    where_sql = "WHERE r.consultant_id = ?"
    params: list[int] = [user.id]

    username_key = str(user.username or "").strip().lower()
    has_full_read_scope = user.is_admin or username_key in FINANCIAL_READ_USERNAMES

    if has_full_read_scope:
        if selected_consultant_id is None:
            where_sql = ""
            params = []
        else:
            where_sql = "WHERE r.consultant_id = ?"
            params = [selected_consultant_id]

    query = f"""
        SELECT
            r.id,
            c.id AS consultant_id,
            c.name AS consultant_name,
            c.username AS consultant_username,
            cust.name AS customer_name,
            cust.customer_code,
            r.status,
            r.document_id,
            r.document_ref,
            r.note_number,
            r.installment,
            r.issue_date,
            r.due_date,
            r.balance_cents,
            r.installment_value_cents,
            r.source_file,
            r.source_page,
            r.report_generated_at
        FROM receivables r
        JOIN consultants c ON c.id = r.consultant_id
        JOIN customers cust ON cust.id = r.customer_id
        {where_sql}
        ORDER BY r.due_date ASC, cust.name ASC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def fetch_credit_limits_for_user(
    user: AuthenticatedUser,
    selected_consultant_id: int | None = None,
) -> list[dict]:
    where_sql = "WHERE cl.consultant_id = ?"
    params: list[int] = [user.id]

    username_key = str(user.username or "").strip().lower()
    has_full_read_scope = user.is_admin or username_key in FINANCIAL_READ_USERNAMES

    if has_full_read_scope:
        if selected_consultant_id is None:
            where_sql = ""
            params = []
        else:
            where_sql = "WHERE cl.consultant_id = ?"
            params = [selected_consultant_id]

    query = f"""
        SELECT
            cl.id,
            cl.consultant_id,
            c.name AS consultant_name,
            c.username AS consultant_username,
            cl.customer_name,
            cl.cnpj,
            cl.credit_limit_cents,
            cl.credit_used_cents,
            cl.credit_available_cents,
            cl.limit_policy,
            cl.note,
            cl.updated_at,
            cl.source_file,
            cl.source_sheet,
            cl.source_row,
            cl.source_section
        FROM credit_limits cl
        JOIN consultants c ON c.id = cl.consultant_id
        {where_sql}
        ORDER BY cl.customer_name ASC
    """
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def fetch_customer_import_hints() -> dict[str, dict[str, str | None]]:
    receivable_query = """
        SELECT
            cust.name AS customer_name,
            COALESCE(cust.customer_code, '') AS customer_code,
            c.name AS consultant_name,
            COUNT(r.id) AS weight
        FROM receivables r
        JOIN customers cust ON cust.id = r.customer_id
        JOIN consultants c ON c.id = r.consultant_id
        WHERE c.is_active = 1
        GROUP BY cust.name, cust.customer_code, c.name
    """
    credit_query = """
        SELECT
            cl.customer_name AS customer_name,
            '' AS customer_code,
            c.name AS consultant_name,
            COUNT(*) AS weight
        FROM credit_limits cl
        JOIN consultants c ON c.id = cl.consultant_id
        WHERE c.is_active = 1
        GROUP BY cl.customer_name, c.name
    """
    with get_connection() as conn:
        rows = conn.execute(receivable_query).fetchall()
        rows += conn.execute(credit_query).fetchall()

    grouped: dict[str, dict[str, object]] = {}
    for row in rows:
        consultant_name = normalize_spaces(row["consultant_name"])
        customer_code = normalize_customer_code(row["customer_code"])
        weight = int(row["weight"] or 0)
        for key in build_customer_hint_keys(row["customer_name"]):
            bucket = grouped.setdefault(
                key,
                {
                    "consultant_weights": {},
                    "codes": set(),
                },
            )
            if consultant_name:
                consultant_weights: dict[str, int] = bucket["consultant_weights"]  # type: ignore[assignment]
                consultant_weights[consultant_name] = consultant_weights.get(consultant_name, 0) + max(weight, 1)
            if customer_code:
                codes: set[str] = bucket["codes"]  # type: ignore[assignment]
                codes.add(customer_code)

    hints: dict[str, dict[str, str | None]] = {}
    for key, bucket in grouped.items():
        consultant_weights: dict[str, int] = bucket["consultant_weights"]  # type: ignore[assignment]
        codes_set: set[str] = bucket["codes"]  # type: ignore[assignment]
        codes = sorted(codes_set)
        consultant_name: str | None = None
        if consultant_weights:
            consultant_name = sorted(
                consultant_weights.items(),
                key=lambda item: (item[1], item[0]),
                reverse=True,
            )[0][0]
        customer_code = codes[0] if len(codes) == 1 else None
        if consultant_name or customer_code:
            hints[key] = {
                "consultantName": consultant_name,
                "customerCode": customer_code,
            }
    return hints


def fetch_document_consultant_hints() -> dict[str, str]:
    query = """
        SELECT
            r.document_id,
            c.name AS consultant_name,
            COUNT(DISTINCT r.consultant_id) AS consultants_count
        FROM receivables r
        JOIN consultants c ON c.id = r.consultant_id
        GROUP BY r.document_id, c.name
    """
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()

    by_document: dict[str, set[str]] = {}
    for row in rows:
        document_id = normalize_spaces(str(row["document_id"] or ""))
        consultant_name = normalize_spaces(str(row["consultant_name"] or ""))
        if not document_id or not consultant_name:
            continue
        bucket = by_document.setdefault(document_id, set())
        bucket.add(consultant_name)

    hints: dict[str, str] = {}
    for document_id, consultants in by_document.items():
        if len(consultants) == 1:
            hints[document_id] = next(iter(consultants))
    return hints


def save_import_format_profile(
    *,
    source_file: str,
    source_kind: str,
    sheet_name: str | None,
    header_signature: str | None,
    detected_columns: list[str] | None,
    records_detected: int,
) -> None:
    detected_columns_json = json.dumps(detected_columns or [], ensure_ascii=False)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO import_format_profiles (
                source_file,
                source_kind,
                sheet_name,
                header_signature,
                detected_columns_json,
                records_detected
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                source_file,
                source_kind,
                sheet_name,
                header_signature,
                detected_columns_json,
                int(records_detected),
            ),
        )


def start_ingestion_batch(
    *,
    operation_id: str,
    mode: str,
    actor_username: str,
    strict_vendors: bool,
    append_mode: bool,
) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO ingestion_batches (
                operation_id,
                mode,
                actor_username,
                strict_vendors,
                append_mode
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(operation_id).strip(),
                str(mode).strip().lower(),
                normalize_spaces(actor_username),
                1 if strict_vendors else 0,
                1 if append_mode else 0,
            ),
        )
        return int(cursor.lastrowid)


def register_ingestion_file(
    *,
    batch_id: int,
    file_name: str,
    file_type: str,
    source_kind: str,
    record_count: int,
    meta: dict | None = None,
) -> int:
    meta_json = json.dumps(meta or {}, ensure_ascii=False)
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO ingestion_files (
                batch_id,
                file_name,
                file_type,
                source_kind,
                record_count,
                meta_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                int(batch_id),
                normalize_spaces(file_name),
                normalize_spaces(file_type),
                normalize_spaces(source_kind),
                int(record_count),
                meta_json,
            ),
        )
        return int(cursor.lastrowid)


def stage_receivables_records(
    *,
    batch_id: int,
    source_kind: str,
    source_file: str,
    records: list[ReceivableRecord],
    file_id: int | None = None,
) -> int:
    if not records:
        return 0

    rows: list[tuple] = []
    for record in records:
        vendor_name = normalize_spaces(record.vendor_name)
        customer_name = normalize_spaces(record.customer_name)
        rows.append(
            (
                int(batch_id),
                int(file_id) if file_id is not None else None,
                normalize_spaces(source_kind),
                normalize_spaces(source_file),
                int(record.source_page),
                record.report_generated_at,
                vendor_name,
                normalize_name_key(vendor_name),
                customer_name,
                normalize_name_key(customer_name),
                normalize_customer_code(record.customer_code),
                normalize_spaces(record.status),
                normalize_spaces(record.document_id),
                normalize_spaces(record.document_ref),
                normalize_spaces(record.note_number),
                normalize_spaces(record.installment),
                normalize_spaces(record.issue_date),
                normalize_spaces(record.due_date),
                int(record.balance_cents),
                int(record.installment_value_cents),
                normalize_spaces(record.raw_line),
            )
        )

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO staging_receivables (
                batch_id,
                file_id,
                source_kind,
                source_file,
                source_page,
                report_generated_at,
                vendor_name,
                vendor_key,
                customer_name,
                customer_key,
                customer_code,
                status,
                document_id,
                document_ref,
                note_number,
                installment,
                issue_date,
                due_date,
                balance_cents,
                installment_value_cents,
                raw_line
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def stage_credit_limit_records(
    *,
    batch_id: int,
    source_kind: str,
    source_file: str,
    records: list[CreditLimitRecord],
    file_id: int | None = None,
) -> int:
    if not records:
        return 0

    rows: list[tuple] = []
    for record in records:
        consultant_name = normalize_spaces(record.consultant_name)
        customer_name = normalize_spaces(record.customer_name)
        cnpj_value = normalize_cnpj(record.cnpj) or ""
        rows.append(
            (
                int(batch_id),
                int(file_id) if file_id is not None else None,
                normalize_spaces(source_kind),
                normalize_spaces(source_file),
                normalize_spaces(record.source_sheet),
                int(record.source_row),
                normalize_spaces(record.source_section),
                consultant_name,
                normalize_name_key(consultant_name),
                customer_name,
                normalize_name_key(customer_name),
                cnpj_value,
                cnpj_value,
                record.credit_limit_cents,
                record.credit_used_cents,
                record.credit_available_cents,
                record.limit_policy,
                record.note,
                record.updated_at,
                record.raw_limit_value,
                record.raw_used_value,
                record.raw_available_value,
            )
        )

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO staging_credit_limits (
                batch_id,
                file_id,
                source_kind,
                source_file,
                source_sheet,
                source_row,
                source_section,
                consultant_name,
                consultant_key,
                customer_name,
                customer_key,
                cnpj,
                cnpj_key,
                credit_limit_cents,
                credit_used_cents,
                credit_available_cents,
                limit_policy,
                note,
                updated_at,
                raw_limit_value,
                raw_used_value,
                raw_available_value
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def finalize_ingestion_batch(
    *,
    batch_id: int,
    status: str,
    message: str,
    audit: dict,
    warnings: list[str],
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET
                status = ?,
                message = ?,
                audit_json = ?,
                warnings_json = ?,
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                normalize_spaces(status),
                message,
                json.dumps(audit or {}, ensure_ascii=False),
                json.dumps(warnings or [], ensure_ascii=False),
                int(batch_id),
            ),
        )


def list_ingestion_batches(limit: int = 20) -> list[dict]:
    capped = max(1, min(int(limit), 200))
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                operation_id,
                mode,
                actor_username,
                strict_vendors,
                append_mode,
                status,
                message,
                audit_json,
                warnings_json,
                created_at,
                completed_at
            FROM ingestion_batches
            ORDER BY id DESC
            LIMIT ?
            """,
            (capped,),
        ).fetchall()
        batch_ids = [int(row["id"]) for row in rows]
        files_by_batch: dict[int, list[dict]] = {}
        if batch_ids:
            placeholders = ", ".join("?" for _ in batch_ids)
            file_rows = conn.execute(
                f"""
                SELECT
                    id,
                    batch_id,
                    file_name,
                    file_type,
                    source_kind,
                    record_count,
                    meta_json,
                    created_at
                FROM ingestion_files
                WHERE batch_id IN ({placeholders})
                ORDER BY batch_id DESC, id ASC
                """,
                batch_ids,
            ).fetchall()
            for file_row in file_rows:
                batch_id = int(file_row["batch_id"])
                file_meta_raw = file_row["meta_json"] or "{}"
                try:
                    file_meta = json.loads(file_meta_raw)
                except json.JSONDecodeError:
                    file_meta = {}
                files_by_batch.setdefault(batch_id, []).append(
                    {
                        "id": int(file_row["id"]),
                        "fileName": file_row["file_name"],
                        "fileType": file_row["file_type"],
                        "sourceKind": file_row["source_kind"],
                        "recordCount": int(file_row["record_count"] or 0),
                        "meta": file_meta,
                        "createdAt": file_row["created_at"],
                    }
                )

    payload: list[dict] = []
    for row in rows:
        row_id = int(row["id"])
        audit_raw = row["audit_json"] or "{}"
        warnings_raw = row["warnings_json"] or "[]"
        try:
            audit = json.loads(audit_raw)
        except json.JSONDecodeError:
            audit = {}
        try:
            warnings = json.loads(warnings_raw)
        except json.JSONDecodeError:
            warnings = []

        payload.append(
            {
                "id": row_id,
                "operationId": row["operation_id"],
                "mode": row["mode"],
                "actorUsername": row["actor_username"],
                "strictVendors": bool(row["strict_vendors"]),
                "appendMode": bool(row["append_mode"]),
                "status": row["status"],
                "message": row["message"],
                "audit": audit,
                "warnings": warnings,
                "files": files_by_batch.get(row_id, []),
                "createdAt": row["created_at"],
                "completedAt": row["completed_at"],
            }
        )
    return payload


def reset_operational_data(*, remove_non_admin_consultants: bool = True) -> dict:
    with get_connection() as conn:
        receivables_count = int(conn.execute("SELECT COUNT(*) FROM receivables").fetchone()[0])
        credit_limits_count = int(conn.execute("SELECT COUNT(*) FROM credit_limits").fetchone()[0])
        customers_count = int(conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0])
        consultant_links_count = int(conn.execute("SELECT COUNT(*) FROM consultant_customers").fetchone()[0])
        ingestion_batches_count = int(conn.execute("SELECT COUNT(*) FROM ingestion_batches").fetchone()[0])
        import_profiles_count = int(conn.execute("SELECT COUNT(*) FROM import_format_profiles").fetchone()[0])
        non_admin_consultants_count = int(
            conn.execute("SELECT COUNT(*) FROM consultants WHERE is_admin = 0").fetchone()[0]
        )

        conn.execute("DELETE FROM receivables")
        conn.execute("DELETE FROM consultant_customers")
        conn.execute("DELETE FROM customers")
        conn.execute("DELETE FROM credit_limits")
        conn.execute("DELETE FROM import_format_profiles")
        conn.execute("DELETE FROM ingestion_batches")

        consultants_removed = 0
        if remove_non_admin_consultants:
            conn.execute("DELETE FROM consultants WHERE is_admin = 0")
            consultants_removed = non_admin_consultants_count

    return {
        "receivables": receivables_count,
        "creditLimits": credit_limits_count,
        "customers": customers_count,
        "consultantLinks": consultant_links_count,
        "ingestionBatches": ingestion_batches_count,
        "importProfiles": import_profiles_count,
        "consultantsRemoved": consultants_removed,
    }


def get_or_create_consultant(
    *,
    conn: sqlite3.Connection,
    consultant_name: str,
    default_password: str,
    existing_usernames: set[str],
) -> tuple[int, str | None]:
    cleaned_name = normalize_spaces(consultant_name)

    row = conn.execute(
        """
        SELECT id, username
        FROM consultants
        WHERE name = ?
        """,
        (cleaned_name,),
    ).fetchone()
    if row:
        return row["id"], None

    normalized_target = normalize_name_key(cleaned_name)
    similar_rows = conn.execute(
        """
        SELECT id, username, name
        FROM consultants
        """
    ).fetchall()
    for candidate in similar_rows:
        if normalize_name_key(candidate["name"]) == normalized_target:
            if candidate["name"] != cleaned_name:
                conn.execute(
                    "UPDATE consultants SET name = ? WHERE id = ?",
                    (cleaned_name, int(candidate["id"])),
                )
            return int(candidate["id"]), None

    username = generate_unique_username(cleaned_name, existing_usernames)
    cursor = conn.execute(
        """
        INSERT INTO consultants (name, username, password_hash, is_admin)
        VALUES (?, ?, ?, 0)
        """,
        (cleaned_name, username, hash_password(default_password)),
    )
    return int(cursor.lastrowid), username


def get_or_create_customer(
    *,
    conn: sqlite3.Connection,
    consultant_id: int,
    customer_name: str,
    customer_code: str,
) -> tuple[int, int, bool]:
    cleaned_name = normalize_spaces(customer_name)
    cleaned_code = normalize_customer_code(customer_code)
    normalized_target_name = normalize_name_key(cleaned_name)
    merged_duplicates = 0

    consultant_candidates = find_consultant_customer_candidates(
        conn=conn,
        consultant_id=consultant_id,
        normalized_customer_name=normalized_target_name,
    )
    if consultant_candidates:
        canonical = choose_canonical_customer(
            consultant_candidates,
            preferred_code=cleaned_code,
        )
        canonical_id = int(canonical["id"])
        duplicate_ids = [
            int(candidate["id"])
            for candidate in consultant_candidates
            if int(candidate["id"]) != canonical_id
        ]
        if duplicate_ids:
            merged_duplicates += merge_duplicate_customers_for_consultant(
                conn=conn,
                consultant_id=consultant_id,
                canonical_customer_id=canonical_id,
                duplicate_customer_ids=duplicate_ids,
            )

        canonical_name = normalize_spaces(canonical["name"])
        canonical_code = normalize_customer_code(canonical["customer_code"])
        if canonical_name != cleaned_name:
            conn.execute(
                "UPDATE customers SET name = ? WHERE id = ?",
                (cleaned_name, canonical_id),
            )
        if cleaned_code and canonical_code != cleaned_code:
            set_customer_code_if_possible(
                conn=conn,
                customer_id=canonical_id,
                customer_name=cleaned_name,
                customer_code=cleaned_code,
            )
        return canonical_id, merged_duplicates, False

    row = conn.execute(
        """
        SELECT id
        FROM customers
        WHERE name = ? AND COALESCE(customer_code, '') = ?
        """,
        (cleaned_name, cleaned_code),
    ).fetchone()
    if row:
        return int(row["id"]), merged_duplicates, False

    existing_rows = conn.execute(
        """
        SELECT id, name, customer_code
        FROM customers
        """
    ).fetchall()

    same_name_rows = [
        candidate
        for candidate in existing_rows
        if normalize_name_key(candidate["name"]) == normalized_target_name
    ]
    for candidate in same_name_rows:
        if normalize_customer_code(candidate["customer_code"]) == cleaned_code:
            if candidate["name"] != cleaned_name:
                conn.execute(
                    "UPDATE customers SET name = ? WHERE id = ?",
                    (cleaned_name, int(candidate["id"])),
                )
            return int(candidate["id"]), merged_duplicates, False

    if cleaned_code:
        rows_without_code = [
            candidate
            for candidate in same_name_rows
            if normalize_customer_code(candidate["customer_code"]) == ""
        ]
        if len(rows_without_code) == 1:
            row_to_update = rows_without_code[0]
            conn.execute(
                """
                UPDATE customers
                SET name = ?, customer_code = ?
                WHERE id = ?
                """,
                (cleaned_name, cleaned_code, int(row_to_update["id"])),
            )
            return int(row_to_update["id"]), merged_duplicates, False

    if not cleaned_code and len(same_name_rows) == 1:
        candidate = same_name_rows[0]
        if candidate["name"] != cleaned_name:
            conn.execute(
                "UPDATE customers SET name = ? WHERE id = ?",
                (cleaned_name, int(candidate["id"])),
            )
        return int(candidate["id"]), merged_duplicates, False

    cursor = conn.execute(
        """
        INSERT INTO customers (name, customer_code)
        VALUES (?, ?)
        """,
        (cleaned_name, cleaned_code),
    )
    return int(cursor.lastrowid), merged_duplicates, True


def find_consultant_customer_candidates(
    *,
    conn: sqlite3.Connection,
    consultant_id: int,
    normalized_customer_name: str,
) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT
            cust.id,
            cust.name,
            COALESCE(cust.customer_code, '') AS customer_code,
            COUNT(r.id) AS titles_count
        FROM consultant_customers cc
        JOIN customers cust ON cust.id = cc.customer_id
        LEFT JOIN receivables r
            ON r.customer_id = cust.id
           AND r.consultant_id = cc.consultant_id
        WHERE cc.consultant_id = ?
        GROUP BY cust.id, cust.name, cust.customer_code
        """,
        (consultant_id,),
    ).fetchall()
    return [
        row
        for row in rows
        if normalize_name_key(row["name"]) == normalized_customer_name
    ]


def choose_canonical_customer(
    rows: list[sqlite3.Row],
    *,
    preferred_code: str,
) -> sqlite3.Row:
    preferred = normalize_customer_code(preferred_code)

    def score(row: sqlite3.Row) -> tuple[int, int, int, int]:
        code = normalize_customer_code(row["customer_code"])
        return (
            1 if preferred and code == preferred else 0,
            int(row["titles_count"]),
            1 if code else 0,
            -int(row["id"]),
        )

    return max(rows, key=score)


def merge_duplicate_customers_for_consultant(
    *,
    conn: sqlite3.Connection,
    consultant_id: int,
    canonical_customer_id: int,
    duplicate_customer_ids: list[int],
) -> int:
    deduplicated_titles = 0
    if not duplicate_customer_ids:
        return deduplicated_titles

    for duplicate_customer_id in duplicate_customer_ids:
        rows = conn.execute(
            """
            SELECT
                id,
                source_file,
                source_page,
                report_generated_at,
                document_id,
                document_ref,
                note_number,
                installment,
                status,
                issue_date,
                due_date,
                balance_cents,
                installment_value_cents,
                raw_line
            FROM receivables
            WHERE consultant_id = ? AND customer_id = ?
            ORDER BY id ASC
            """,
            (consultant_id, duplicate_customer_id),
        ).fetchall()

        for row in rows:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO receivables (
                    consultant_id,
                    customer_id,
                    source_file,
                    source_page,
                    report_generated_at,
                    document_id,
                    document_ref,
                    note_number,
                    installment,
                    status,
                    issue_date,
                    due_date,
                    balance_cents,
                    installment_value_cents,
                    raw_line
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    consultant_id,
                    canonical_customer_id,
                    row["source_file"],
                    int(row["source_page"]),
                    row["report_generated_at"],
                    row["document_id"],
                    row["document_ref"],
                    row["note_number"],
                    row["installment"],
                    row["status"],
                    row["issue_date"],
                    row["due_date"],
                    int(row["balance_cents"]),
                    int(row["installment_value_cents"]),
                    row["raw_line"],
                ),
            )
            if cursor.rowcount == 0:
                deduplicated_titles += 1
            conn.execute(
                "DELETE FROM receivables WHERE id = ?",
                (int(row["id"]),),
            )

        conn.execute(
            """
            DELETE FROM consultant_customers
            WHERE consultant_id = ? AND customer_id = ?
            """,
            (consultant_id, duplicate_customer_id),
        )
        maybe_delete_orphan_customer(conn=conn, customer_id=duplicate_customer_id)

    conn.execute(
        """
        INSERT OR IGNORE INTO consultant_customers (consultant_id, customer_id)
        VALUES (?, ?)
        """,
        (consultant_id, canonical_customer_id),
    )
    return deduplicated_titles


def maybe_delete_orphan_customer(*, conn: sqlite3.Connection, customer_id: int) -> None:
    has_links = conn.execute(
        "SELECT 1 FROM consultant_customers WHERE customer_id = ? LIMIT 1",
        (customer_id,),
    ).fetchone()
    if has_links:
        return

    has_receivables = conn.execute(
        "SELECT 1 FROM receivables WHERE customer_id = ? LIMIT 1",
        (customer_id,),
    ).fetchone()
    if has_receivables:
        return

    conn.execute("DELETE FROM customers WHERE id = ?", (customer_id,))


def set_customer_code_if_possible(
    *,
    conn: sqlite3.Connection,
    customer_id: int,
    customer_name: str,
    customer_code: str,
) -> None:
    existing = conn.execute(
        """
        SELECT id
        FROM customers
        WHERE name = ? AND COALESCE(customer_code, '') = ?
        LIMIT 1
        """,
        (customer_name, customer_code),
    ).fetchone()
    if existing and int(existing["id"]) != customer_id:
        return

    conn.execute(
        "UPDATE customers SET customer_code = ? WHERE id = ?",
        (customer_code, customer_id),
    )


def generate_unique_username(name: str, existing_usernames: set[str]) -> str:
    base = slugify_username(name)
    candidate = base
    index = 2
    while candidate in existing_usernames:
        candidate = f"{base}{index}"
        index += 1
    existing_usernames.add(candidate)
    return candidate


CONSULTANT_ALIAS_MAP = {
    "LUIZ CARLOS": "LUIZ CARLOS ABRANCHES",
    "ALISSON ROCHA": "ALISSON ROCHA ALENCAR",
    "DUSTIN TOCANTINS": "DUSTIN AKIHIRO MAENO",
    "GABRIEL MOURA": "GABRIEL MOURA DE ALMEIDA",
    "KALLEBE GOMES": "KALLEBE GOMES DA SILVA",
    "GUILHERME": "GUILHERME APARECIDO VANZELLA",
}


def resolve_consultant_id(raw_name: str, consultant_by_name: dict[str, int]) -> int | None:
    key = normalize_name_key(raw_name)
    canonical = CONSULTANT_ALIAS_MAP.get(key, key)
    if canonical in consultant_by_name:
        return consultant_by_name[canonical]

    for existing_name, consultant_id in consultant_by_name.items():
        if canonical in existing_name or existing_name in canonical:
            return consultant_id
    return None


def normalize_name_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_name.upper().replace("_", " ").split())


def normalize_spaces(value: str) -> str:
    return " ".join(str(value).split())


def normalize_customer_code(value: str | None) -> str:
    if value is None:
        return ""
    return normalize_spaces(value)


def build_customer_hint_keys(value: str) -> set[str]:
    keys: set[str] = set()
    raw = str(value or "")
    primary = normalize_name_key(raw)
    if primary:
        keys.add(primary)

    simplified = re.sub(r"\(.*?\)", " ", raw)
    simplified = re.sub(r"[-_/]", " ", simplified)
    simplified = re.sub(r"\b(MATRIZ|FILIAL|UNIDADE)\b", " ", simplified, flags=re.IGNORECASE)
    simplified = normalize_name_key(simplified)
    if simplified:
        keys.add(simplified)
    return keys


def normalize_cnpj(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).split())
    return cleaned or None


def parse_optional_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def should_apply_snapshot_update(existing_snapshot: str | None, incoming_snapshot: str | None) -> bool:
    existing_dt = parse_optional_iso_datetime(existing_snapshot)
    incoming_dt = parse_optional_iso_datetime(incoming_snapshot)

    if existing_dt and incoming_dt:
        return incoming_dt >= existing_dt
    if existing_dt and not incoming_dt:
        return False
    return True
