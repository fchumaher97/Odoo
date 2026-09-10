# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03_helpdesk_config_sla_rules
# MAGIC Configuración gobernada de entidades y reglas SLA contractuales para Helpdesk.
# MAGIC
# MAGIC ## Decisiones funcionales
# MAGIC - La entidad se identifica primero por dominio de correo y luego por alias controlados.
# MAGIC - La métrica evaluada es `first_response_hours`, originada en el campo custom de Odoo.
# MAGIC - Los tickets cancelados se conservan, pero quedan `EXCLUIDO`.
# MAGIC - Los SLA propios sin horas explícitas quedan `SLA PROPIO`, no se inventa un umbral.
# MAGIC - La criticidad contractual proviene de la regla aplicable por entidad y tipo.
# MAGIC - No se fuerza una equivalencia para los códigos 0/1/2/3 de Odoo sin metadata oficial.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable
import uuid
from datetime import datetime, timezone

CATALOG = "corp_dailytech"
CONFIG = f"{CATALOG}.config"
SILVER = f"{CATALOG}.silver"
AUDIT = f"{CATALOG}.audit"
BUILD_NAME = "03_helpdesk_config_sla_rules"
RUN_ID = str(uuid.uuid4())

TICKET_SOURCE = f"{SILVER}.helpdesk_ticket"
ENTITY_TABLE = f"{CONFIG}.entity_mapping"
SLA_TABLE = f"{CONFIG}.sla_rules"
CONTRACTUAL_TABLE = f"{SILVER}.helpdesk_ticket_contractual"
AUDIT_TABLE = f"{AUDIT}.sla_build_runs"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONFIG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT}")

if not spark.catalog.tableExists(TICKET_SOURCE):
    raise RuntimeError(f"No existe la fuente requerida: {TICKET_SOURCE}")
if spark.table(TICKET_SOURCE).limit(1).count() == 0:
    raise RuntimeError(f"La fuente está vacía: {TICKET_SOURCE}")

print(f"RUN_ID={RUN_ID}")
print(f"Fuente={TICKET_SOURCE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tablas de configuración gobernadas

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ENTITY_TABLE} (
    entity_code STRING NOT NULL,
    entity_name STRING NOT NULL,
    email_domain STRING,
    customer_name_pattern STRING,
    match_priority INT NOT NULL,
    rule_type STRING NOT NULL,
    is_active BOOLEAN NOT NULL,
    source_reference STRING,
    updated_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SLA_TABLE} (
    rule_id STRING NOT NULL,
    entity_code STRING NOT NULL,
    request_type STRING,
    contractual_criticality STRING,
    target_response_hours DECIMAL(10,2),
    sla_mode STRING NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_active BOOLEAN NOT NULL,
    source_reference STRING,
    updated_at TIMESTAMP
)
USING DELTA
""")

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
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Catálogo de entidades
# MAGIC El dominio es la regla preferida. Los patrones de nombre son fallback y deben mantenerse controlados.

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
    # Fallbacks explícitos vistos en los datos
    ("SMV", "SMV", None, "SUPERINTENDENCIA DEL MERCADO DE VALORES", 50, "NAME", True, "Nombre cliente Odoo"),
    ("ONPE", "ONPE", None, "ONPE", 50, "NAME", True, "Nombre cliente Odoo"),
    ("INGEMMET", "INGEMMET", None, "INGEMMET", 50, "NAME", True, "Nombre cliente Odoo"),
    ("INACAL", "INACAL", None, "INACAL", 50, "NAME", True, "Nombre cliente Odoo"),
    ("PEIP-EB", "PEIP-EB", None, "PEIP-EB", 50, "NAME", True, "Nombre cliente Odoo"),
]

entity_seed = spark.createDataFrame(ENTITY_ROWS, entity_schema).withColumn("updated_at", F.current_timestamp())

# Reemplazo controlado de la configuración administrada por este notebook
entity_seed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(ENTITY_TABLE)
print(f"Entidades publicadas: {entity_seed.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Matriz contractual SLA

# COMMAND ----------

sla_schema = T.StructType([
    T.StructField("entity_code", T.StringType(), False),
    T.StructField("request_type", T.StringType(), True),
    T.StructField("contractual_criticality", T.StringType(), True),
    T.StructField("target_response_hours", T.DoubleType(), True),
    T.StructField("sla_mode", T.StringType(), False),
])

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
    spark.createDataFrame(SLA_ROWS, sla_schema)
    .withColumn(
        "rule_id",
        F.sha2(F.concat_ws("|", "entity_code", F.coalesce("request_type", F.lit("*")), "sla_mode"), 256)
    )
    .withColumn("target_response_hours", F.col("target_response_hours").cast(T.DecimalType(10,2)))
    .withColumn("effective_from", F.to_date(F.lit("2024-01-01")))
    .withColumn("effective_to", F.lit(None).cast("date"))
    .withColumn("is_active", F.lit(True))
    .withColumn("source_reference", F.lit("Matriz SLA proporcionada por negocio"))
    .withColumn("updated_at", F.current_timestamp())
    .select(
        "rule_id", "entity_code", "request_type", "contractual_criticality",
        "target_response_hours", "sla_mode", "effective_from", "effective_to",
        "is_active", "source_reference", "updated_at"
    )
)

sla_seed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SLA_TABLE)
print(f"Reglas SLA publicadas: {sla_seed.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Normalización de entidad y tipo

# COMMAND ----------

tickets = spark.table(TICKET_SOURCE)

# Tipo efectivo. line_type tiene prioridad porque representa Incidente/Consulta/Requerimiento cuando existe.
tickets_norm = (
    tickets
    .withColumn(
        "request_type_raw",
        F.upper(F.trim(F.coalesce("line_type_name", "ticket_type_name", F.lit(""))))
    )
    .withColumn(
        "request_type_contractual",
        F.when(F.col("request_type_raw").contains("INCIDENT"), "INCIDENTE")
         .when(F.col("request_type_raw").contains("CONSULT"), "CONSULTA")
         .when(F.col("request_type_raw").contains("REQUER"), "REQUERIMIENTO")
         .otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "name_search_text",
        F.upper(F.concat_ws(" | ",
            F.coalesce("customer_name", F.lit("")),
            F.coalesce("customer_name_entered", F.lit("")),
            F.coalesce("commercial_partner_name", F.lit("")),
            F.coalesce("partner_name_m2o", F.lit(""))
        ))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Matching de entidad con prioridad y sin duplicar tickets

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

from pyspark.sql.window import Window
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
# MAGIC ## 6. Aplicación de regla contractual

# COMMAND ----------

rules = spark.table(SLA_TABLE).filter("is_active = true")

contractual = (
    matched.alias("t")
    .join(
        rules.alias("r"),
        (F.col("t.entity_code") == F.col("r.entity_code")) &
        (
            (F.col("r.sla_mode") == "OWN") |
            (F.col("t.request_type_contractual") == F.col("r.request_type"))
        ) &
        (F.col("t.created_date") >= F.col("r.effective_from")) &
        (F.col("r.effective_to").isNull() | (F.col("t.created_date") <= F.col("r.effective_to"))),
        "left"
    )
    .select(
        "t.*",
        "r.rule_id",
        F.col("r.request_type").alias("sla_rule_request_type"),
        "r.contractual_criticality",
        "r.target_response_hours",
        "r.sla_mode"
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

# Validación bloqueante: un ticket por fila
quality = contractual.agg(
    F.count("*").alias("rows"),
    F.countDistinct("ticket_id").alias("unique_tickets"),
    F.sum(F.when(F.col("ticket_id").isNull(), 1).otherwise(0)).alias("null_keys")
).first()

if quality["null_keys"] != 0:
    raise RuntimeError(f"Hay {quality['null_keys']} ticket_id nulos")
if quality["rows"] != quality["unique_tickets"]:
    raise RuntimeError(
        f"El matching generó duplicados: filas={quality['rows']}, tickets={quality['unique_tickets']}"
    )

contractual.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(CONTRACTUAL_TABLE)
print(f"Publicado {CONTRACTUAL_TABLE}: {quality['rows']} tickets únicos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Auditoría

# COMMAND ----------

metrics = contractual.agg(
    F.count("*").alias("source_ticket_count"),
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

audit_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("build_name", T.StringType(), False),
    T.StructField("source_ticket_count", T.LongType(), False),
    T.StructField("output_ticket_count", T.LongType(), False),
    T.StructField("matched_entity_count", T.LongType(), False),
    T.StructField("unmatched_entity_count", T.LongType(), False),
    T.StructField("numeric_rule_count", T.LongType(), False),
    T.StructField("own_sla_count", T.LongType(), False),
    T.StructField("no_rule_count", T.LongType(), False),
    T.StructField("si_sla_count", T.LongType(), False),
    T.StructField("no_sla_count", T.LongType(), False),
    T.StructField("excluded_count", T.LongType(), False),
    T.StructField("status", T.StringType(), False),
])

audit_row = [(
    RUN_ID, BUILD_NAME,
    int(metrics["source_ticket_count"]), int(metrics["output_ticket_count"]),
    int(metrics["matched_entity_count"]), int(metrics["unmatched_entity_count"]),
    int(metrics["numeric_rule_count"]), int(metrics["own_sla_count"]),
    int(metrics["no_rule_count"]), int(metrics["si_sla_count"]),
    int(metrics["no_sla_count"]), int(metrics["excluded_count"]), "SUCCESS"
)]

spark.createDataFrame(audit_row, audit_schema).withColumn("built_at", F.current_timestamp()).write.mode("append").saveAsTable(AUDIT_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Controles de salida

# COMMAND ----------

display(contractual.groupBy("entity_code").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT contractual_sla_status, COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket_contractual
# MAGIC GROUP BY contractual_sla_status
# MAGIC ORDER BY tickets DESC;

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
# MAGIC     line_type_name,
# MAGIC     ticket_type_name,
# MAGIC     COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY
# MAGIC     line_type_name,
# MAGIC     ticket_type_name
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     criticality_code,
# MAGIC     COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY criticality_code
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     line_type_name,
# MAGIC     ticket_type_name,
# MAGIC     COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY line_type_name, ticket_type_name
# MAGIC ORDER BY tickets DESC;