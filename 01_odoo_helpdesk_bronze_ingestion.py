# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01_odoo_helpdesk_bronze_ingestion
# MAGIC Ingesta robusta Odoo Producción -> Unity Catalog Bronze.
# MAGIC
# MAGIC Modelos:
# MAGIC - helpdesk.ticket
# MAGIC - helpdesk.stage
# MAGIC - helpdesk.team
# MAGIC - helpdesk.ticket.type
# MAGIC - res.partner
# MAGIC - res.users
# MAGIC - hr.employee
# MAGIC
# MAGIC Características:
# MAGIC - JSON-RPC con reintentos
# MAGIC - paginación completa
# MAGIC - validación dinámica de campos
# MAGIC - carga FULL inicial e INCREMENTAL por write_date
# MAGIC - ventana de relectura para evitar pérdidas
# MAGIC - MERGE idempotente por source_model + record_id
# MAGIC - auditoría por ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "AUTO", ["AUTO", "FULL", "INCREMENTAL"])
dbutils.widgets.text("batch_size", "500")
dbutils.widgets.text("overlap_hours", "24")
dbutils.widgets.text("full_start_date", "2024-01-01 00:00:00")

LOAD_MODE = dbutils.widgets.get("load_mode").upper()
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
OVERLAP_HOURS = int(dbutils.widgets.get("overlap_hours"))
FULL_START_DATE = dbutils.widgets.get("full_start_date")

CATALOG = "corp_dailytech"
BRONZE_SCHEMA = "bronze"
AUDIT_SCHEMA = "audit"
SECRET_SCOPE = "odoo-production"

assert LOAD_MODE in {"AUTO", "FULL", "INCREMENTAL"}
assert BATCH_SIZE > 0
assert OVERLAP_HOURS >= 0

print(f"LOAD_MODE={LOAD_MODE}")
print(f"BATCH_SIZE={BATCH_SIZE}")
print(f"OVERLAP_HOURS={OVERLAP_HOURS}")
print(f"FULL_START_DATE={FULL_START_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Secretos y conexión

# COMMAND ----------

import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BASE_URL = dbutils.secrets.get(SECRET_SCOPE, "base_url").rstrip("/")
DB = dbutils.secrets.get(SECRET_SCOPE, "db")
UID = int(dbutils.secrets.get(SECRET_SCOPE, "uid"))
API_KEY = dbutils.secrets.get(SECRET_SCOPE, "api_key")

RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.now(timezone.utc)

session = requests.Session()
retry = Retry(
    total=5,
    connect=5,
    read=5,
    status=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["POST"]),
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retry))

print("Conexión configurada.")
print(f"RUN_ID={RUN_ID}")
print(f"BASE_URL={BASE_URL}")
print(f"DB={DB}")
print(f"UID={UID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cliente JSON-RPC reutilizable

# COMMAND ----------

def odoo_execute_kw(model, method, args=None, kwargs=None, timeout=180):
    args = args or []
    kwargs = kwargs or {}

    payload = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "call",
        "params": {
            "service": "object",
            "method": "execute_kw",
            "args": [DB, UID, API_KEY, model, method, args, kwargs],
        },
    }

    response = session.post(
        BASE_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    response.raise_for_status()

    body = response.json()
    if "error" in body:
        error = body["error"]
        data = error.get("data", {})
        message = data.get("message") or error.get("message") or str(error)
        debug = data.get("debug", "")
        raise RuntimeError(
            f"Odoo JSON-RPC error | model={model} | method={method} | "
            f"message={message}\n{debug[:2500]}"
        )

    return body.get("result")


def validate_connection():
    result = odoo_execute_kw(
        "res.users",
        "search_read",
        args=[[['id', '=', UID]]],
        kwargs={"fields": ["id", "name", "login"], "limit": 1},
    )
    if not result:
        raise RuntimeError(f"No se pudo validar el UID {UID} en Odoo.")
    safe_user = result[0]
    print(f"Conexión Odoo OK: {safe_user.get('name')} ({safe_user.get('login')})")


validate_connection()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Metadata y paginación

# COMMAND ----------

def get_available_fields(model):
    metadata = odoo_execute_kw(
        model,
        "fields_get",
        args=[],
        kwargs={"attributes": ["string", "type", "relation", "readonly", "store"]},
    )
    return metadata or {}


def filter_existing_fields(model, requested_fields):
    available = get_available_fields(model)
    existing = [field for field in requested_fields if field in available]
    missing = sorted(set(requested_fields) - set(existing))
    print(f"{model}: campos disponibles solicitados={len(existing)}, no encontrados={len(missing)}")
    if missing:
        print(f"{model}: omitidos={missing}")
    return existing


def odoo_search_read_all(model, fields, domain=None, batch_size=500, order="id"):
    domain = domain or []
    records = []
    offset = 0

    while True:
        batch = odoo_execute_kw(
            model,
            "search_read",
            args=[domain],
            kwargs={
                "fields": fields,
                "limit": batch_size,
                "offset": offset,
                "order": order,
            },
        ) or []

        if not batch:
            break

        records.extend(batch)
        print(f"{model}: offset={offset}, lote={len(batch)}, acumulado={len(records)}")

        if len(batch) < batch_size:
            break

        offset += batch_size

    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configuración de modelos y campos

# COMMAND ----------

HELPDESK_TICKET_FIELDS = [
    "id", "ticket_ref", "name", "active", "description",
    "priority", "ticket_origin", "ticket_type_id", "line_type_id",
    "tag_ids", "kanban_state", "user_id", "team_id", "stage_id",
    "company_id", "partner_id", "commercial_partner_id", "partner_name",
    "partner_email", "partner_phone", "create_date", "write_date",
    "assign_date", "close_date", "date_last_stage_update", "assign_hours",
    "close_hours", "open_hours", "first_response_hours", "avg_response_hours",
    "total_response_hours", "total_hours_spent", "count_first_response_timer",
    "count_resolution_timer", "x_studio_criticidad", "x_studio_sla",
    "tiempo_primera_respuesta", "tiempo_primera_respuesta_esperado",
    "tiempo_resolucion_esperado", "tiempo_primera_respuesta_formatted",
    "total_hours_spent_formatted", "use_sla", "sla_ids", "sla_status_ids",
    "sla_deadline", "sla_deadline_hours", "sla_reached", "sla_reached_late",
    "sla_fail", "sla_success", "project_id", "project_sale_order_id",
    "sale_order_id", "sale_line_id", "analytic_account_id", "timesheet_ids",
    "message_ids", "activity_ids", "rating_last_value", "rating_last_feedback",
]

MODEL_CONFIG = {
    "helpdesk.ticket": {
        "table": "raw_helpdesk_tickets",
        "fields": HELPDESK_TICKET_FIELDS,
        "incremental": True,
    },
    "helpdesk.stage": {
        "table": "raw_helpdesk_stages",
        "fields": ["id", "name", "sequence", "fold", "team_ids", "create_date", "write_date"],
        "incremental": True,
    },
    "helpdesk.team": {
        "table": "raw_helpdesk_teams",
        "fields": ["id", "name", "active", "company_id", "member_ids", "use_sla", "create_date", "write_date"],
        "incremental": True,
    },
    "helpdesk.ticket.type": {
        "table": "raw_helpdesk_ticket_types",
        "fields": ["id", "name", "sequence", "create_date", "write_date"],
        "incremental": True,
    },
    "res.partner": {
        "table": "raw_partners",
        "fields": [
            "id", "name", "display_name", "active", "is_company", "parent_id",
            "commercial_partner_id", "email", "phone", "mobile", "website",
            "street", "street2", "city", "state_id", "country_id", "zip",
            "company_type", "vat", "create_date", "write_date",
        ],
        "incremental": True,
    },
    "res.users": {
        "table": "raw_users",
        "fields": [
            "id", "name", "login", "email", "active", "share", "partner_id",
            "company_id", "company_ids", "create_date", "write_date",
        ],
        "incremental": True,
    },
    "hr.employee": {
        "table": "raw_employees",
        "fields": [
            "id", "name", "active", "user_id", "work_email", "work_phone",
            "mobile_phone", "job_id", "department_id", "parent_id", "coach_id",
            "company_id", "create_date", "write_date",
        ],
        "incremental": True,
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tablas técnicas y auditoría

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{AUDIT_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{AUDIT_SCHEMA}.ingestion_runs (
    run_id STRING,
    source_system STRING,
    source_model STRING,
    target_table STRING,
    load_mode STRING,
    watermark STRING,
    extracted_count BIGINT,
    source_distinct_count BIGINT,
    target_count_after BIGINT,
    duplicate_source_ids BIGINT,
    status STRING,
    error_message STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
)
USING DELTA
""")

bronze_schema = T.StructType([
    T.StructField("source_model", T.StringType(), False),
    T.StructField("record_id", T.LongType(), False),
    T.StructField("payload", T.StringType(), False),
    T.StructField("source_write_date", T.StringType(), True),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("run_id", T.StringType(), False),
])


def ensure_bronze_table(table_name):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{table_name} (
        source_model STRING,
        record_id BIGINT,
        payload STRING,
        source_write_date STRING,
        ingested_at TIMESTAMP,
        run_id STRING
    )
    USING DELTA
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Watermark, normalización Bronze y MERGE

# COMMAND ----------

def target_is_empty(target_table):
    if not spark.catalog.tableExists(target_table):
        return True
    return spark.table(target_table).limit(1).count() == 0


def get_watermark(target_table, full_start_date, overlap_hours):
    if target_is_empty(target_table):
        return full_start_date

    max_value = (
        spark.table(target_table)
        .select(F.max(F.to_timestamp("source_write_date")).alias("max_write_date"))
        .collect()[0]["max_write_date"]
    )

    if max_value is None:
        return full_start_date

    return (max_value - timedelta(hours=overlap_hours)).strftime("%Y-%m-%d %H:%M:%S")


def records_to_bronze_df(model, records, run_id, ingested_at):
    rows = []
    for record in records:
        rows.append((
            model,
            int(record["id"]),
            json.dumps(record, ensure_ascii=False, default=str, sort_keys=True),
            None if record.get("write_date") is False else record.get("write_date"),
            ingested_at,
            run_id,
        ))
    return spark.createDataFrame(rows, bronze_schema)


def merge_bronze(source_df, target_table):
    source_dedup = (
        source_df
        .withColumn("_write_ts", F.to_timestamp("source_write_date"))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window
                .partitionBy("source_model", "record_id")
                .orderBy(F.col("_write_ts").desc_nulls_last(), F.col("ingested_at").desc())
            )
        )
        .filter(F.col("_rn") == 1)
        .drop("_write_ts", "_rn")
    )

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(
            source_dedup.alias("s"),
            "t.source_model = s.source_model AND t.record_id = s.record_id",
        )
        .whenMatchedUpdateAll(
            condition="""
                t.source_write_date IS NULL
                OR s.source_write_date IS NULL
                OR to_timestamp(s.source_write_date) >= to_timestamp(t.source_write_date)
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Ejecución de ingesta

# COMMAND ----------

def append_audit(record):
    audit_schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("source_system", T.StringType(), False),
        T.StructField("source_model", T.StringType(), False),
        T.StructField("target_table", T.StringType(), False),
        T.StructField("load_mode", T.StringType(), False),
        T.StructField("watermark", T.StringType(), True),
        T.StructField("extracted_count", T.LongType(), True),
        T.StructField("source_distinct_count", T.LongType(), True),
        T.StructField("target_count_after", T.LongType(), True),
        T.StructField("duplicate_source_ids", T.LongType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("finished_at", T.TimestampType(), False),
    ])
    spark.createDataFrame([record], audit_schema).write.mode("append").saveAsTable(
        f"{CATALOG}.{AUDIT_SCHEMA}.ingestion_runs"
    )


summary = []

for model, cfg in MODEL_CONFIG.items():
    model_started_at = datetime.now(timezone.utc)
    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{cfg['table']}"
    ensure_bronze_table(cfg["table"])

    effective_mode = LOAD_MODE
    if LOAD_MODE == "AUTO":
        effective_mode = "FULL" if target_is_empty(target_table) else "INCREMENTAL"

    watermark = None
    domain = []

    if effective_mode == "INCREMENTAL" and cfg.get("incremental", True):
        watermark = get_watermark(target_table, FULL_START_DATE, OVERLAP_HOURS)
        domain = [["write_date", ">=", watermark]]
    elif effective_mode == "FULL" and model == "helpdesk.ticket" and FULL_START_DATE:
        domain = [["create_date", ">=", FULL_START_DATE]]

    try:
        fields = filter_existing_fields(model, cfg["fields"])
        if "id" not in fields:
            raise RuntimeError(f"El modelo {model} no expone el campo id.")
        if "write_date" not in fields:
            print(f"ADVERTENCIA: {model} no expone write_date; se cargará como referencia completa.")
            domain = []
            watermark = None

        print("=" * 100)
        print(f"Modelo: {model}")
        print(f"Destino: {target_table}")
        print(f"Modo: {effective_mode}")
        print(f"Watermark: {watermark}")
        print(f"Domain: {domain}")

        records = odoo_search_read_all(
            model=model,
            fields=fields,
            domain=domain,
            batch_size=BATCH_SIZE,
            order="write_date,id" if "write_date" in fields else "id",
        )

        ids = [int(r["id"]) for r in records]
        extracted_count = len(ids)
        distinct_count = len(set(ids))
        duplicate_count = extracted_count - distinct_count

        if records:
            source_df = records_to_bronze_df(
                model=model,
                records=records,
                run_id=RUN_ID,
                ingested_at=datetime.now(timezone.utc),
            )
            merge_bronze(source_df, target_table)
        else:
            print(f"{model}: sin registros para el domain actual.")

        target_count = spark.table(target_table).count()
        status = "SUCCESS"
        error_message = None

        print(
            f"OK {model}: extraídos={extracted_count}, distintos={distinct_count}, "
            f"duplicados_fuente={duplicate_count}, destino={target_count}"
        )

    except Exception as exc:
        extracted_count = 0
        distinct_count = 0
        duplicate_count = 0
        target_count = spark.table(target_table).count() if spark.catalog.tableExists(target_table) else 0
        status = "FAILED"
        error_message = str(exc)[:8000]
        print(f"ERROR {model}: {error_message}")

    model_finished_at = datetime.now(timezone.utc)
    audit_record = (
        RUN_ID,
        "ODOO_PRODUCTION_JSONRPC",
        model,
        target_table,
        effective_mode,
        watermark,
        int(extracted_count),
        int(distinct_count),
        int(target_count),
        int(duplicate_count),
        status,
        error_message,
        model_started_at,
        model_finished_at,
    )
    append_audit(audit_record)

    summary.append({
        "model": model,
        "target_table": target_table,
        "mode": effective_mode,
        "watermark": watermark,
        "extracted": extracted_count,
        "distinct_ids": distinct_count,
        "duplicates": duplicate_count,
        "target_count": target_count,
        "status": status,
        "error": error_message or "",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumen y controles de calidad

# COMMAND ----------

# DBTITLE 1,Cell 19
summary_schema = T.StructType([
    T.StructField("model", T.StringType()),
    T.StructField("target_table", T.StringType()),
    T.StructField("mode", T.StringType()),
    T.StructField("watermark", T.StringType(), True),
    T.StructField("extracted", T.LongType()),
    T.StructField("distinct_ids", T.LongType()),
    T.StructField("duplicates", T.LongType()),
    T.StructField("target_count", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("error", T.StringType()),
])
summary_df = spark.createDataFrame(summary, schema=summary_schema)
display(summary_df.orderBy("model"))

failed = [row for row in summary if row["status"] == "FAILED"]
if failed:
    raise RuntimeError(
        "La ingesta terminó con errores: "
        + "; ".join(f"{x['model']}: {x['error']}" for x in failed)
    )

print("Todos los modelos finalizaron correctamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_model,
# MAGIC   COUNT(*) AS total_registros,
# MAGIC   COUNT(DISTINCT record_id) AS ids_unicos,
# MAGIC   COUNT(*) - COUNT(DISTINCT record_id) AS duplicados
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets
# MAGIC GROUP BY source_model;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM corp_dailytech.audit.ingestion_runs
# MAGIC ORDER BY finished_at DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     get_json_object(payload,'$.stage_id[1]') AS stage_name,
# MAGIC     COUNT(*) AS total
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets
# MAGIC GROUP BY stage_name
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     get_json_object(payload,'$.team_id[1]') AS team_name,
# MAGIC     COUNT(*) AS total
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets
# MAGIC GROUP BY team_name
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN get_json_object(payload,'$.sla_success')='true' THEN 'SI SLA'
# MAGIC         WHEN get_json_object(payload,'$.sla_fail')='true' THEN 'NO SLA'
# MAGIC         ELSE 'SIN EVALUAR'
# MAGIC     END AS estado_sla,
# MAGIC     COUNT(*) AS total
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets
# MAGIC GROUP BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     get_json_object(payload,'$.sla_success') AS sla_success,
# MAGIC     get_json_object(payload,'$.sla_fail')    AS sla_fail,
# MAGIC     COUNT(*) AS total
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets
# MAGIC GROUP BY
# MAGIC     get_json_object(payload,'$.sla_success'),
# MAGIC     get_json_object(payload,'$.sla_fail')
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total,
# MAGIC     AVG(
# MAGIC         CAST(
# MAGIC             get_json_object(payload,'$.first_response_hours')
# MAGIC             AS DOUBLE
# MAGIC         )
# MAGIC     ) AS avg_first_response
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     get_json_object(payload,'$.x_studio_criticidad') AS criticidad,
# MAGIC     COUNT(*) AS total
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets
# MAGIC GROUP BY 1 ORDER BY total DESC;