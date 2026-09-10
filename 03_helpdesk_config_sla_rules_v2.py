# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03_helpdesk_config_sla_rules V2
# MAGIC Configuración gobernada, homologación y evaluación del SLA contractual.
# MAGIC
# MAGIC Correcciones V2:
# MAGIC - `Issue` se homologa como `INCIDENTE`.
# MAGIC - `Question` se homologa como `CONSULTA`.
# MAGIC - `Requerimiento` y `REQUERIMIENTO` se homologan como `REQUERIMIENTO`.
# MAGIC - El dominio de correo tiene prioridad para identificar la entidad.
# MAGIC - Los tickets cancelados quedan `EXCLUIDO`.
# MAGIC - Las entidades con SLA propio quedan `SLA PROPIO` sin inventar horas.
# MAGIC - Se conserva una fila única por ticket y se audita la ejecución.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import uuid

CATALOG = "corp_dailytech"
CONFIG = f"{CATALOG}.config"
SILVER = f"{CATALOG}.silver"
AUDIT = f"{CATALOG}.audit"
BUILD_NAME = "03_helpdesk_config_sla_rules_v2"
RUN_ID = str(uuid.uuid4())

TICKET_SOURCE = f"{SILVER}.helpdesk_ticket"
ENTITY_TABLE = f"{CONFIG}.entity_mapping"
SLA_TABLE = f"{CONFIG}.sla_rules"
OUTPUT_TABLE = f"{SILVER}.helpdesk_ticket_contractual"
AUDIT_TABLE = f"{AUDIT}.sla_build_runs"

for schema_name in [CONFIG, SILVER, AUDIT]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

if not spark.catalog.tableExists(TICKET_SOURCE):
    raise RuntimeError(f"No existe la fuente: {TICKET_SOURCE}")

source_count = spark.table(TICKET_SOURCE).count()
if source_count == 0:
    raise RuntimeError(f"La fuente está vacía: {TICKET_SOURCE}")

print(f"RUN_ID={RUN_ID}")
print(f"Fuente={TICKET_SOURCE}")
print(f"Tickets fuente={source_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Publicar catálogo de entidades

# COMMAND ----------

entity_schema = T.StructType([
    T.StructField("entity_code", T.StringType(), False),
    T.StructField("entity_name", T.StringType(), False),
    T.StructField("email_domain", T.StringType(), True),
    T.StructField("customer_name_pattern", T.StringType(), True),
    T.StructField("match_priority", T.IntegerType(), False),
    T.StructField("rule_type", T.StringType(), False),
    T.StructField("is_active", T.BooleanType(), False),
    T.StructField("source_reference", T.StringType(), True),
])

ENTITY_ROWS = [
    ("ONPE", "ONPE", "onpe.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("INGEMMET", "INGEMMET", "ingemmet.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("SMV", "SMV", "smv.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("INACAL", "INACAL", "inacal.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("PCM", "PCM", "pcm.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("ATU", "ATU", "atu.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("PEIP-EB", "PEIP-EB", "peip-eb.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("PRONIS", "PRONIS", "pronis.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("MINCETUR", "MINCETUR", "mincetur.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("DEVIDA", "DEVIDA", "devida.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("INVERMET", "INVERMET", "invermet.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("CONTIGO", "CONTIGO", "contigo.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("PAIS", "PAIS", "pais.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("JNE", "JNE", "jne.gob.pe", None, 10, "DOMAIN", True, "Matriz SLA Helpdesk"),
    ("SMV", "SMV", None, "SUPERINTENDENCIA DEL MERCADO DE VALORES", 50, "NAME", True, "Alias Odoo"),
    ("ONPE", "ONPE", None, "ONPE", 50, "NAME", True, "Alias Odoo"),
    ("INGEMMET", "INGEMMET", None, "INGEMMET", 50, "NAME", True, "Alias Odoo"),
    ("INACAL", "INACAL", None, "INACAL", 50, "NAME", True, "Alias Odoo"),
    ("PEIP-EB", "PEIP-EB", None, "PEIP-EB", 50, "NAME", True, "Alias Odoo"),
]

entity_seed = (
    spark.createDataFrame(ENTITY_ROWS, entity_schema)
    .withColumn("updated_at", F.current_timestamp())
)

(entity_seed.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(ENTITY_TABLE))
print(f"Entidades publicadas={entity_seed.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Publicar matriz SLA contractual

# COMMAND ----------

sla_input_schema = "entity_code string, request_type string, contractual_criticality string, target_response_hours double, sla_mode string"

SLA_ROWS = [
    ("ONPE", "INCIDENTE", "ALTA", 4.0, "NUMERIC"),
    ("ONPE", "CONSULTA", "MEDIA", 7.0, "NUMERIC"),
    ("ONPE", "REQUERIMIENTO", "BAJA", 24.0, "NUMERIC"),
    ("INGEMMET", "INCIDENTE", "ALTA", 2.0, "NUMERIC"),
    ("INGEMMET", "CONSULTA", "MEDIA", 3.0, "NUMERIC"),
    ("INGEMMET", "REQUERIMIENTO", "BAJA", 4.0, "NUMERIC"),
    ("SMV", "INCIDENTE", "ALTA", 1.0, "NUMERIC"),
    ("SMV", "CONSULTA", "MEDIA", 2.0, "NUMERIC"),
    ("SMV", "REQUERIMIENTO", "BAJA", 8.0, "NUMERIC"),
    ("INACAL", "INCIDENTE", "ALTA", 2.0, "NUMERIC"),
    ("INACAL", "CONSULTA", "MEDIA", 4.0, "NUMERIC"),
    ("INACAL", "REQUERIMIENTO", "BAJA", 8.0, "NUMERIC"),
    ("PCM", "INCIDENTE", "MEDIA", 3.0, "NUMERIC"),
    ("PCM", "REQUERIMIENTO", "BAJA", 4.0, "NUMERIC"),
    ("ATU", "INCIDENTE", "MEDIA", 4.0, "NUMERIC"),
    ("ATU", "REQUERIMIENTO", "BAJA", 24.0, "NUMERIC"),
    ("PEIP-EB", "INCIDENTE", "ALTA", 2.0, "NUMERIC"),
    ("PEIP-EB", "CONSULTA", "MEDIA", 4.0, "NUMERIC"),
    ("PEIP-EB", "REQUERIMIENTO", "BAJA", 8.0, "NUMERIC"),
    ("PRONIS", "INCIDENTE", "ALTA", 2.0, "NUMERIC"),
    ("PRONIS", "CONSULTA", "MEDIA", 3.0, "NUMERIC"),
    ("PRONIS", "REQUERIMIENTO", "BAJA", 4.0, "NUMERIC"),
    ("MINCETUR", None, "PROPIOS", None, "OWN"),
    ("DEVIDA", None, "PROPIOS", None, "OWN"),
    ("INVERMET", None, "PROPIOS", None, "OWN"),
    ("CONTIGO", None, "PROPIOS", None, "OWN"),
    ("PAIS", None, "PROPIOS", None, "OWN"),
    ("JNE", None, "PROPIOS", None, "OWN"),
]

sla_seed = (
    spark.createDataFrame(SLA_ROWS, sla_input_schema)
    .withColumn("entity_code", F.upper(F.trim("entity_code")))
    .withColumn("request_type", F.upper(F.trim("request_type")))
    .withColumn("contractual_criticality", F.upper(F.trim("contractual_criticality")))
    .withColumn("target_response_hours", F.col("target_response_hours").cast(T.DecimalType(10, 2)))
    .withColumn("rule_id", F.sha2(F.concat_ws("|", "entity_code", F.coalesce("request_type", F.lit("*")), "sla_mode"), 256))
    .withColumn("effective_from", F.to_date(F.lit("2024-01-01")))
    .withColumn("effective_to", F.lit(None).cast("date"))
    .withColumn("is_active", F.lit(True))
    .withColumn("source_reference", F.lit("Matriz SLA proporcionada por negocio"))
    .withColumn("updated_at", F.current_timestamp())
    .select("rule_id", "entity_code", "request_type", "contractual_criticality",
            "target_response_hours", "sla_mode", "effective_from", "effective_to",
            "is_active", "source_reference", "updated_at")
)

(sla_seed.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(SLA_TABLE))
print(f"Reglas SLA publicadas={sla_seed.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalizar tipo contractual

# COMMAND ----------

tickets = spark.table(TICKET_SOURCE)

tickets_norm = (
    tickets
    .withColumn("request_type_raw", F.upper(F.trim(F.coalesce("line_type_name", "ticket_type_name", F.lit("")))))
    .withColumn(
        "request_type_contractual",
        F.when(F.col("request_type_raw").rlike("ISSUE|INCIDENT"), "INCIDENTE")
         .when(F.col("request_type_raw").rlike("QUESTION|CONSULT"), "CONSULTA")
         .when(F.col("request_type_raw").rlike("REQUER"), "REQUERIMIENTO")
         .otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "name_search_text",
        F.upper(F.concat_ws(" | ",
            F.coalesce("customer_name", F.lit("")),
            F.coalesce("customer_name_entered", F.lit("")),
            F.coalesce("commercial_partner_name", F.lit("")),
            F.coalesce("partner_name_m2o", F.lit(""))))
    )
)

# Validación de la homologación esperada
expected_mapping = {
    "ISSUE": "INCIDENTE",
    "QUESTION": "CONSULTA",
    "REQUERIMIENTO": "REQUERIMIENTO"
}
for raw_value, expected_value in expected_mapping.items():
    invalid = tickets_norm.filter(
        (F.col("request_type_raw") == raw_value) &
        (F.col("request_type_contractual") != expected_value)
    ).count()
    if invalid > 0:
        raise RuntimeError(f"Homologación incorrecta: {raw_value} -> {expected_value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identificar entidad con prioridad

# COMMAND ----------

mappings = spark.table(ENTITY_TABLE).filter("is_active = true")

candidates = (
    tickets_norm.alias("t")
    .join(
        mappings.alias("m"),
        (
            (F.col("m.email_domain").isNotNull() &
             (F.lower(F.col("t.customer_email_domain")) == F.lower(F.col("m.email_domain"))))
            |
            (F.col("m.customer_name_pattern").isNotNull() &
             F.col("t.name_search_text").contains(F.upper(F.col("m.customer_name_pattern"))))
        ),
        "left"
    )
)

match_window = Window.partitionBy(F.col("t.ticket_id")).orderBy(
    F.col("m.match_priority").asc_nulls_last(),
    F.col("m.entity_code").asc_nulls_last()
)

matched = (
    candidates
    .withColumn("_match_rank", F.row_number().over(match_window))
    .filter("_match_rank = 1")
    .select(
        "t.*",
        F.col("m.entity_code").alias("entity_code"),
        F.col("m.entity_name").alias("entity_name"),
        F.col("m.rule_type").alias("entity_match_method"),
        F.col("m.match_priority").alias("entity_match_priority")
    )
    .withColumn("entity_code", F.coalesce("entity_code", F.lit("SIN ENTIDAD")))
    .withColumn("entity_name", F.coalesce("entity_name", F.lit("SIN ENTIDAD")))
    .withColumn("entity_match_method", F.coalesce("entity_match_method", F.lit("UNMATCHED")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aplicar regla contractual

# COMMAND ----------

rules = spark.table(SLA_TABLE).filter("is_active = true")

contractual = (
    matched.alias("t")
    .join(
        rules.alias("r"),
        (F.col("t.entity_code") == F.col("r.entity_code")) &
        ((F.col("r.sla_mode") == "OWN") |
         (F.col("t.request_type_contractual") == F.col("r.request_type"))) &
        (F.col("t.created_date") >= F.col("r.effective_from")) &
        (F.col("r.effective_to").isNull() | (F.col("t.created_date") <= F.col("r.effective_to"))),
        "left"
    )
    .select(
        "t.*", "r.rule_id",
        F.col("r.request_type").alias("sla_rule_request_type"),
        "r.contractual_criticality", "r.target_response_hours", "r.sla_mode"
    )
    .withColumn(
        "contractual_sla_status",
        F.when(F.col("operational_status") == "CANCELADO", "EXCLUIDO")
         .when(F.col("entity_code") == "SIN ENTIDAD", "SIN ENTIDAD")
         .when(F.col("sla_mode") == "OWN", "SLA PROPIO")
         .when(F.col("request_type_contractual").isNull(), "SIN TIPO")
         .when(F.col("rule_id").isNull(), "SIN REGLA")
         .when(F.col("first_response_hours").isNull(), "SIN MEDICION")
         .when(F.col("first_response_hours") <= F.col("target_response_hours"), "SI SLA")
         .otherwise("NO SLA")
    )
    .withColumn(
        "sla_variance_hours",
        F.when(
            F.col("target_response_hours").isNotNull() & F.col("first_response_hours").isNotNull(),
            F.round(F.col("first_response_hours") - F.col("target_response_hours"), 4)
        )
    )
    .withColumn(
        "contractual_sla_met",
        F.when(F.col("contractual_sla_status") == "SI SLA", True)
         .when(F.col("contractual_sla_status") == "NO SLA", False)
         .otherwise(F.lit(None).cast("boolean"))
    )
    .withColumn("sla_config_run_id", F.lit(RUN_ID))
    .withColumn("sla_configured_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validaciones bloqueantes y publicación

# COMMAND ----------

quality = contractual.agg(
    F.count("*").alias("rows"),
    F.countDistinct("ticket_id").alias("unique_tickets"),
    F.sum(F.when(F.col("ticket_id").isNull(), 1).otherwise(0)).alias("null_keys")
).first()

if quality["null_keys"] != 0:
    raise RuntimeError(f"Hay {quality['null_keys']} ticket_id nulos")
if quality["rows"] != quality["unique_tickets"]:
    raise RuntimeError(f"El matching duplicó tickets: filas={quality['rows']}, únicos={quality['unique_tickets']}")
if quality["rows"] != source_count:
    raise RuntimeError(f"Conteo inconsistente: fuente={source_count}, salida={quality['rows']}")

(contractual.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE))
print(f"Publicado {OUTPUT_TABLE}: {quality['rows']} tickets únicos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Auditoría

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    run_id STRING,
    build_name STRING,
    source_ticket_count BIGINT,
    output_ticket_count BIGINT,
    matched_entity_count BIGINT,
    unmatched_entity_count BIGINT,
    numeric_rule_count BIGINT,
    own_sla_count BIGINT,
    no_rule_count BIGINT,
    si_sla_count BIGINT,
    no_sla_count BIGINT,
    excluded_count BIGINT,
    status STRING,
    built_at TIMESTAMP
) USING DELTA
""")

metrics = contractual.agg(
    F.count("*").alias("output_ticket_count"),
    F.sum(F.when(F.col("entity_code") != "SIN ENTIDAD", 1).otherwise(0)).alias("matched_entity_count"),
    F.sum(F.when(F.col("entity_code") == "SIN ENTIDAD", 1).otherwise(0)).alias("unmatched_entity_count"),
    F.sum(F.when(F.col("sla_mode") == "NUMERIC", 1).otherwise(0)).alias("numeric_rule_count"),
    F.sum(F.when(F.col("contractual_sla_status") == "SLA PROPIO", 1).otherwise(0)).alias("own_sla_count"),
    F.sum(F.when(F.col("contractual_sla_status").isin("SIN REGLA", "SIN TIPO", "SIN ENTIDAD"), 1).otherwise(0)).alias("no_rule_count"),
    F.sum(F.when(F.col("contractual_sla_status") == "SI SLA", 1).otherwise(0)).alias("si_sla_count"),
    F.sum(F.when(F.col("contractual_sla_status") == "NO SLA", 1).otherwise(0)).alias("no_sla_count"),
    F.sum(F.when(F.col("contractual_sla_status") == "EXCLUIDO", 1).otherwise(0)).alias("excluded_count")
).first()

audit_schema = "run_id string, build_name string, source_ticket_count long, output_ticket_count long, matched_entity_count long, unmatched_entity_count long, numeric_rule_count long, own_sla_count long, no_rule_count long, si_sla_count long, no_sla_count long, excluded_count long, status string"

audit_row = [(
    RUN_ID, BUILD_NAME, source_count, int(metrics["output_ticket_count"]),
    int(metrics["matched_entity_count"]), int(metrics["unmatched_entity_count"]),
    int(metrics["numeric_rule_count"]), int(metrics["own_sla_count"]),
    int(metrics["no_rule_count"]), int(metrics["si_sla_count"]),
    int(metrics["no_sla_count"]), int(metrics["excluded_count"]), "SUCCESS"
)]

(spark.createDataFrame(audit_row, audit_schema)
 .withColumn("built_at", F.current_timestamp())
 .write.mode("append").saveAsTable(AUDIT_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Controles V2

# COMMAND ----------

display(contractual.groupBy("ticket_type_name", "request_type_contractual").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT contractual_sla_status, COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC GROUP BY contractual_sla_status
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   request_type_contractual,
# MAGIC   contractual_sla_status,
# MAGIC   COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC GROUP BY request_type_contractual, contractual_sla_status
# MAGIC ORDER BY request_type_contractual, tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   entity_code,
# MAGIC   request_type_contractual,
# MAGIC   contractual_criticality,
# MAGIC   target_response_hours,
# MAGIC   contractual_sla_status,
# MAGIC   COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC GROUP BY entity_code, request_type_contractual, contractual_criticality,
# MAGIC          target_response_hours, contractual_sla_status
# MAGIC ORDER BY entity_code, request_type_contractual, contractual_sla_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_email_domain, customer_name, COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC WHERE entity_code = 'SIN ENTIDAD'
# MAGIC GROUP BY customer_email_domain, customer_name
# MAGIC ORDER BY tickets DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM corp_dailytech.audit.sla_build_runs
# MAGIC ORDER BY built_at DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS tickets_totales,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(total_hours_spent),
# MAGIC         6
# MAGIC     ) AS total_horas_decimales,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(total_hours_spent),
# MAGIC         6
# MAGIC     ) AS promedio_horas_decimales,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN contractual_sla_status = 'SI SLA'
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS si_sla,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN contractual_sla_status = 'NO SLA'
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS no_sla,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN contractual_sla_status NOT IN (
# MAGIC                 'SI SLA',
# MAGIC                 'NO SLA'
# MAGIC             )
# MAGIC             THEN 1 ELSE 0
# MAGIC         END
# MAGIC     ) AS no_evaluados
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC
# MAGIC WHERE created_at_utc >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC   AND created_at_utc <  TIMESTAMP '2026-09-08 00:00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tickets_lima AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC ),
# MAGIC
# MAGIC periodo AS (
# MAGIC     SELECT *
# MAGIC     FROM tickets_lima
# MAGIC     WHERE created_at_lima >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC       AND created_at_lima <  TIMESTAMP '2026-09-08 00:00:00'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS tickets,
# MAGIC
# MAGIC     ROUND(SUM(total_hours_spent), 6)
# MAGIC         AS suma_total_hours_spent,
# MAGIC
# MAGIC     ROUND(SUM(close_hours), 6)
# MAGIC         AS suma_close_hours,
# MAGIC
# MAGIC     ROUND(SUM(first_response_hours_custom), 6)
# MAGIC         AS suma_first_response_custom,
# MAGIC
# MAGIC     ROUND(SUM(resolution_timer_hours), 6)
# MAGIC         AS suma_resolution_timer,
# MAGIC
# MAGIC     ROUND(AVG(total_hours_spent), 6)
# MAGIC         AS promedio_total_hours_spent
# MAGIC
# MAGIC FROM periodo;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tickets_lima AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     ticket_ref,
# MAGIC     request_type_raw,
# MAGIC     request_type_contractual,
# MAGIC     entity_code,
# MAGIC     customer_email_domain,
# MAGIC     customer_name,
# MAGIC     contractual_criticality,
# MAGIC     target_response_hours,
# MAGIC     first_response_hours,
# MAGIC     total_hours_spent,
# MAGIC     contractual_sla_status,
# MAGIC     created_at_lima
# MAGIC
# MAGIC FROM tickets_lima
# MAGIC
# MAGIC WHERE ticket_ref IN (
# MAGIC     '1362', '1361', '1360', '1359',
# MAGIC     '1358', '1357', '1356', '1353',
# MAGIC     '1352', '1351', '1350', '1349',
# MAGIC     '1348', '1347'
# MAGIC )
# MAGIC
# MAGIC ORDER BY CAST(ticket_ref AS BIGINT) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_ref,
# MAGIC     ticket_type_name,
# MAGIC     request_type_contractual,
# MAGIC     ticket_subject
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC WHERE ticket_ref IN (
# MAGIC     '1362', '1361', '1360', '1359',
# MAGIC     '1358', '1357', '1356', '1353',
# MAGIC     '1352', '1351', '1350', '1349',
# MAGIC     '1348', '1347'
# MAGIC )
# MAGIC ORDER BY CAST(ticket_ref AS BIGINT) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     record_id,
# MAGIC     GET_JSON_OBJECT(payload, '$.id') AS payload_id
# MAGIC FROM corp_dailytech.bronze.raw_helpdesk_tickets
# MAGIC WHERE record_id <> CAST(
# MAGIC     GET_JSON_OBJECT(payload, '$.id')
# MAGIC     AS BIGINT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM corp_dailytech.bronze.raw_helpdesk_tickets)
# MAGIC         AS bronze_count,
# MAGIC
# MAGIC     (SELECT COUNT(*) FROM corp_dailytech.silver.helpdesk_ticket)
# MAGIC         AS silver_count,
# MAGIC
# MAGIC     (SELECT COUNT(*) FROM corp_dailytech.silver.helpdesk_ticket_contractual)
# MAGIC         AS contractual_count;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total,
# MAGIC     COUNT(DISTINCT ticket_id) AS unicos,
# MAGIC     COUNT(*) - COUNT(DISTINCT ticket_id) AS duplicados
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS agentes_huerfanos
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket t
# MAGIC LEFT JOIN corp_dailytech.silver.agent a
# MAGIC     ON t.assigned_user_id = a.user_id
# MAGIC WHERE t.assigned_user_id IS NOT NULL
# MAGIC   AND a.user_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS clientes_huerfanos
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket t
# MAGIC LEFT JOIN corp_dailytech.silver.customer c
# MAGIC     ON t.partner_id = c.partner_id
# MAGIC WHERE t.partner_id IS NOT NULL
# MAGIC   AND c.partner_id IS NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total,
# MAGIC
# MAGIC     SUM(CASE WHEN assigned_user_id IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS sin_asignado,
# MAGIC
# MAGIC     SUM(CASE WHEN partner_id IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS sin_cliente,
# MAGIC
# MAGIC     SUM(CASE WHEN request_type_contractual IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS sin_tipo,
# MAGIC
# MAGIC     SUM(CASE WHEN entity_code = 'SIN ENTIDAD' THEN 1 ELSE 0 END)
# MAGIC         AS sin_entidad,
# MAGIC
# MAGIC     SUM(CASE WHEN first_response_hours IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS sin_primera_respuesta
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     ticket_ref,
# MAGIC     created_at_utc,
# MAGIC     assigned_at_utc,
# MAGIC     closed_at_utc
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC WHERE assigned_at_utc < created_at_utc
# MAGIC    OR closed_at_utc < created_at_utc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     entity_code,
# MAGIC     request_type,
# MAGIC     COUNT(*) AS reglas
# MAGIC FROM corp_dailytech.config.sla_rules
# MAGIC WHERE is_active = TRUE
# MAGIC GROUP BY entity_code, request_type
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS corp_dailytech.audit.dashboard_reconciliation (
# MAGIC     validation_date TIMESTAMP,
# MAGIC     period_start DATE,
# MAGIC     period_end DATE,
# MAGIC     expected_ticket_count BIGINT,
# MAGIC     actual_ticket_count BIGINT,
# MAGIC     expected_total_hours DECIMAL(18,6),
# MAGIC     actual_total_hours DECIMAL(18,6),
# MAGIC     expected_sla_met BIGINT,
# MAGIC     actual_sla_met BIGINT,
# MAGIC     ticket_count_match BOOLEAN,
# MAGIC     total_hours_match BOOLEAN,
# MAGIC     sla_match BOOLEAN,
# MAGIC     comments STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC     WHERE FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC       AND FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) < TIMESTAMP '2026-09-08 00:00:00'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     operational_status,
# MAGIC     COUNT(*) AS tickets,
# MAGIC     ROUND(SUM(total_hours_spent), 6) AS total_horas,
# MAGIC     ROUND(AVG(total_hours_spent), 6) AS promedio_horas
# MAGIC FROM periodo
# MAGIC GROUP BY operational_status
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS tickets_totales,
# MAGIC     ROUND(SUM(total_hours_spent), 6) AS total_horas,
# MAGIC     ROUND(AVG(total_hours_spent), 6) AS promedio_horas,
# MAGIC
# MAGIC     CONCAT(
# MAGIC         CAST(
# MAGIC             FLOOR(
# MAGIC                 ROUND(SUM(total_hours_spent) * 3600, 0) / 3600
# MAGIC             ) AS STRING
# MAGIC         ),
# MAGIC         ':',
# MAGIC         LPAD(
# MAGIC             CAST(
# MAGIC                 FLOOR(
# MAGIC                     MOD(
# MAGIC                         CAST(
# MAGIC                             ROUND(SUM(total_hours_spent) * 3600, 0)
# MAGIC                             AS BIGINT
# MAGIC                         ),
# MAGIC                         3600
# MAGIC                     ) / 60
# MAGIC                 ) AS STRING
# MAGIC             ),
# MAGIC             2,
# MAGIC             '0'
# MAGIC         ),
# MAGIC         ':',
# MAGIC         LPAD(
# MAGIC             CAST(
# MAGIC                 MOD(
# MAGIC                     CAST(
# MAGIC                         ROUND(SUM(total_hours_spent) * 3600, 0)
# MAGIC                         AS BIGINT
# MAGIC                     ),
# MAGIC                     60
# MAGIC                 ) AS STRING
# MAGIC             ),
# MAGIC             2,
# MAGIC             '0'
# MAGIC         )
# MAGIC     ) AS total_horas_formato
# MAGIC
# MAGIC FROM periodo
# MAGIC
# MAGIC WHERE created_at_lima >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC   AND created_at_lima <  TIMESTAMP '2026-09-08 00:00:00'
# MAGIC   AND operational_status <> 'CANCELADO';

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC     WHERE operational_status <> 'CANCELADO'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS tickets,
# MAGIC     ROUND(AVG(total_hours_spent), 6) AS promedio_horas,
# MAGIC
# MAGIC     CONCAT(
# MAGIC         LPAD(
# MAGIC             CAST(
# MAGIC                 FLOOR(
# MAGIC                     ROUND(AVG(total_hours_spent) * 3600, 0) / 3600
# MAGIC                 ) AS STRING
# MAGIC             ),
# MAGIC             2,
# MAGIC             '0'
# MAGIC         ),
# MAGIC         ':',
# MAGIC         LPAD(
# MAGIC             CAST(
# MAGIC                 FLOOR(
# MAGIC                     MOD(
# MAGIC                         CAST(
# MAGIC                             ROUND(AVG(total_hours_spent) * 3600, 0)
# MAGIC                             AS BIGINT
# MAGIC                         ),
# MAGIC                         3600
# MAGIC                     ) / 60
# MAGIC                 ) AS STRING
# MAGIC             ),
# MAGIC             2,
# MAGIC             '0'
# MAGIC         ),
# MAGIC         ':',
# MAGIC         LPAD(
# MAGIC             CAST(
# MAGIC                 MOD(
# MAGIC                     CAST(
# MAGIC                         ROUND(AVG(total_hours_spent) * 3600, 0)
# MAGIC                         AS BIGINT
# MAGIC                     ),
# MAGIC                     60
# MAGIC                 ) AS STRING
# MAGIC             ),
# MAGIC             2,
# MAGIC             '0'
# MAGIC         )
# MAGIC     ) AS promedio_horas_formato
# MAGIC
# MAGIC FROM periodo
# MAGIC
# MAGIC WHERE created_at_lima >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC   AND created_at_lima <  TIMESTAMP '2026-09-08 00:00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tickets_lima AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS tickets_totales,
# MAGIC     ROUND(SUM(total_hours_spent), 6) AS total_horas_decimales,
# MAGIC     ROUND(AVG(total_hours_spent), 6) AS promedio_horas_decimales
# MAGIC
# MAGIC FROM tickets_lima
# MAGIC
# MAGIC WHERE created_at_lima >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC   AND created_at_lima <  TIMESTAMP '2026-09-08 00:00:00'
# MAGIC   AND operational_status <> 'CANCELADO';

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         from_utc_timestamp(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN UPPER(ticket_type_name) = 'ISSUE'
# MAGIC                 THEN 'SOLICITUD'
# MAGIC             WHEN UPPER(ticket_type_name) = 'QUESTION'
# MAGIC                 THEN 'CONSULTA'
# MAGIC             WHEN UPPER(ticket_type_name) LIKE '%REQUER%'
# MAGIC                 THEN 'REQUERIMIENTO'
# MAGIC             ELSE 'SIN TIPO'
# MAGIC         END AS report_type
# MAGIC
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     report_type,
# MAGIC     COUNT(*) AS tickets,
# MAGIC     ROUND(SUM(total_hours_spent), 6) AS total_horas
# MAGIC
# MAGIC FROM periodo
# MAGIC
# MAGIC WHERE created_at_lima >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC   AND created_at_lima <  TIMESTAMP '2026-09-08 00:00:00'
# MAGIC   AND operational_status <> 'CANCELADO'
# MAGIC
# MAGIC GROUP BY report_type
# MAGIC ORDER BY report_type;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_ref,
# MAGIC     operational_status,
# MAGIC     ticket_type_name,
# MAGIC     total_hours_spent
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC WHERE FROM_UTC_TIMESTAMP(
# MAGIC         created_at_utc,
# MAGIC         'America/Lima'
# MAGIC       )
# MAGIC       BETWEEN
# MAGIC       TIMESTAMP '2026-09-01 00:00:00'
# MAGIC       AND
# MAGIC       TIMESTAMP '2026-09-07 23:59:59'
# MAGIC ORDER BY ticket_ref DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         ticket_id,
# MAGIC         ticket_ref,
# MAGIC         ticket_subject,
# MAGIC
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima,
# MAGIC
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             closed_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS closed_at_lima,
# MAGIC
# MAGIC         operational_status,
# MAGIC         stage_name,
# MAGIC         team_name,
# MAGIC
# MAGIC         ticket_type_name,
# MAGIC         request_type_contractual,
# MAGIC
# MAGIC         entity_code,
# MAGIC         customer_name,
# MAGIC         customer_email_domain,
# MAGIC
# MAGIC         total_hours_spent,
# MAGIC         first_response_hours,
# MAGIC
# MAGIC         contractual_sla_status
# MAGIC
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC
# MAGIC     WHERE FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC
# MAGIC       AND FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) < TIMESTAMP '2026-09-08 00:00:00'
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM periodo
# MAGIC ORDER BY created_at_lima, ticket_ref;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC
# MAGIC     WHERE FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC
# MAGIC       AND FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) < TIMESTAMP '2026-09-08 00:00:00'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     'TODOS' AS escenario,
# MAGIC     COUNT(*) AS tickets,
# MAGIC     ROUND(SUM(total_hours_spent), 6) AS horas
# MAGIC FROM periodo
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'SIN CANCELADOS',
# MAGIC     COUNT(*),
# MAGIC     ROUND(SUM(total_hours_spent), 6)
# MAGIC FROM periodo
# MAGIC WHERE operational_status <> 'CANCELADO'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'SOLO CERRADOS',
# MAGIC     COUNT(*),
# MAGIC     ROUND(SUM(total_hours_spent), 6)
# MAGIC FROM periodo
# MAGIC WHERE operational_status = 'CERRADO'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'SOLO CON ENTIDAD',
# MAGIC     COUNT(*),
# MAGIC     ROUND(SUM(total_hours_spent), 6)
# MAGIC FROM periodo
# MAGIC WHERE operational_status <> 'CANCELADO'
# MAGIC   AND entity_code <> 'SIN ENTIDAD'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'SOLO CON TIPO',
# MAGIC     COUNT(*),
# MAGIC     ROUND(SUM(total_hours_spent), 6)
# MAGIC FROM periodo
# MAGIC WHERE operational_status <> 'CANCELADO'
# MAGIC   AND request_type_contractual IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'SOLO EVALUADOS SLA',
# MAGIC     COUNT(*),
# MAGIC     ROUND(SUM(total_hours_spent), 6)
# MAGIC FROM periodo
# MAGIC WHERE contractual_sla_status IN ('SI SLA', 'NO SLA');

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC
# MAGIC     WHERE FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) >= TIMESTAMP '2026-09-01 00:00:00'
# MAGIC
# MAGIC       AND FROM_UTC_TIMESTAMP(
# MAGIC               created_at_utc,
# MAGIC               'America/Lima'
# MAGIC           ) < TIMESTAMP '2026-09-08 00:00:00'
# MAGIC
# MAGIC       AND operational_status <> 'CANCELADO'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     entity_code,
# MAGIC     ticket_type_name,
# MAGIC     team_name,
# MAGIC     stage_name,
# MAGIC     COUNT(*) AS tickets,
# MAGIC     ROUND(SUM(total_hours_spent), 6) AS horas
# MAGIC
# MAGIC FROM periodo
# MAGIC
# MAGIC GROUP BY
# MAGIC     entity_code,
# MAGIC     ticket_type_name,
# MAGIC     team_name,
# MAGIC     stage_name
# MAGIC
# MAGIC ORDER BY horas DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periodo AS (
# MAGIC     SELECT
# MAGIC         ticket_ref,
# MAGIC         ticket_subject,
# MAGIC
# MAGIC         FROM_UTC_TIMESTAMP(
# MAGIC             created_at_utc,
# MAGIC             'America/Lima'
# MAGIC         ) AS created_at_lima,
# MAGIC
# MAGIC         total_hours_spent,
# MAGIC         operational_status
# MAGIC
# MAGIC     FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM periodo
# MAGIC
# MAGIC WHERE created_at_lima >= TIMESTAMP '2026-08-31 19:00:00'
# MAGIC   AND created_at_lima <  TIMESTAMP '2026-09-08 05:00:00'
# MAGIC
# MAGIC ORDER BY created_at_lima;