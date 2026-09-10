# Databricks notebook source
# MAGIC %md
# MAGIC # 02_helpdesk_silver_build
# MAGIC Construye la capa Silver tipada desde las tablas Bronze de Odoo Producción.
# MAGIC
# MAGIC Salidas:
# MAGIC - `corp_dailytech.silver.helpdesk_ticket`
# MAGIC - `corp_dailytech.silver.customer`
# MAGIC - `corp_dailytech.silver.agent`
# MAGIC - `corp_dailytech.silver.helpdesk_stage`
# MAGIC - `corp_dailytech.silver.helpdesk_team`
# MAGIC - `corp_dailytech.silver.helpdesk_ticket_type`
# MAGIC
# MAGIC Principios:
# MAGIC - Bronze permanece inmutable y conserva el JSON original.
# MAGIC - Silver aplana relaciones many2one y tipa fechas, métricas y booleanos.
# MAGIC - Se conservan por separado el SLA nativo de Odoo y el SLA esperado del ticket.
# MAGIC - No se inventa la etiqueta de `x_studio_criticidad`; se conserva el código raw.
# MAGIC - La ejecución es idempotente mediante `CREATE OR REPLACE TABLE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

CATALOG = "corp_dailytech"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
AUDIT = f"{CATALOG}.audit"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT}")

SOURCE_TABLES = {
    "tickets": f"{BRONZE}.raw_helpdesk_tickets",
    "stages": f"{BRONZE}.raw_helpdesk_stages",
    "teams": f"{BRONZE}.raw_helpdesk_teams",
    "ticket_types": f"{BRONZE}.raw_helpdesk_ticket_types",
    "partners": f"{BRONZE}.raw_partners",
    "users": f"{BRONZE}.raw_users",
    "employees": f"{BRONZE}.raw_employees",
}

for logical_name, table_name in SOURCE_TABLES.items():
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Falta la tabla Bronze requerida: {table_name}")
    if spark.table(table_name).limit(1).count() == 0:
        raise RuntimeError(f"La tabla Bronze está vacía: {table_name}")
    print(f"OK fuente {logical_name}: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers JSON y calidad

# COMMAND ----------

def j(path):
    return F.get_json_object(F.col("payload"), f"$.{path}")


def null_if_false(column):
    return F.when(
        column.isNull() | F.lower(F.trim(column)).isin("false", "null", ""),
        F.lit(None)
    ).otherwise(column)


def j_string(path):
    return null_if_false(j(path))


def j_long(path):
    return null_if_false(j(path)).cast("long")


def j_double(path):
    return null_if_false(j(path)).cast("double")


def j_boolean(path):
    raw = F.lower(F.trim(j(path)))
    return (
        F.when(raw == "true", F.lit(True))
         .when(raw == "false", F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )


def j_timestamp(path):
    return F.to_timestamp(null_if_false(j(path)))


def m2o_id(path):
    return null_if_false(F.get_json_object(F.col("payload"), f"$.{path}[0]")).cast("long")


def m2o_name(path):
    return null_if_false(F.get_json_object(F.col("payload"), f"$.{path}[1]"))


def json_id_array(path):
    raw = j(path)
    return (
        F.when(raw.isNull() | F.lower(F.trim(raw)).isin("false", "null", ""), F.array().cast("array<long>"))
         .otherwise(F.from_json(raw, T.ArrayType(T.LongType())))
    )


def latest_bronze(table_name):
    """Protección adicional: conserva el registro Bronze más reciente por record_id."""
    source = spark.table(table_name)
    w = Window.partitionBy("source_model", "record_id").orderBy(
        F.to_timestamp("source_write_date").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last()
    )
    return (
        source
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimensión Customer desde `res.partner`

# COMMAND ----------

partners_bronze = latest_bronze(SOURCE_TABLES["partners"])

customer_df = (
    partners_bronze
    .select(
        F.col("record_id").alias("partner_id"),
        j_string("name").alias("partner_name"),
        j_string("display_name").alias("display_name"),
        j_boolean("active").alias("is_active"),
        j_boolean("is_company").alias("is_company"),
        m2o_id("parent_id").alias("parent_partner_id"),
        m2o_name("parent_id").alias("parent_partner_name"),
        m2o_id("commercial_partner_id").alias("commercial_partner_id"),
        m2o_name("commercial_partner_id").alias("commercial_partner_name"),
        F.lower(F.trim(j_string("email"))).alias("email"),
        j_string("phone").alias("phone"),
        j_string("mobile").alias("mobile"),
        j_string("website").alias("website"),
        j_string("street").alias("street"),
        j_string("street2").alias("street2"),
        j_string("city").alias("city"),
        m2o_id("state_id").alias("state_id"),
        m2o_name("state_id").alias("state_name"),
        m2o_id("country_id").alias("country_id"),
        m2o_name("country_id").alias("country_name"),
        j_string("zip").alias("zip"),
        j_string("company_type").alias("company_type"),
        j_string("vat").alias("vat"),
        j_timestamp("create_date").alias("created_at_utc"),
        j_timestamp("write_date").alias("updated_at_utc"),
        F.col("ingested_at").alias("bronze_ingested_at"),
        F.col("run_id").alias("bronze_run_id"),
    )
    .withColumn(
        "email_domain",
        F.lower(F.regexp_extract(F.coalesce(F.col("email"), F.lit("")), r"@([^> ]+)$", 1))
    )
    .withColumn("silver_updated_at", F.current_timestamp())
)

(customer_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{SILVER}.customer"))

print(f"customer: {customer_df.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensión Agent consolidada (`res.users` + `hr.employee`)

# COMMAND ----------

users_bronze = latest_bronze(SOURCE_TABLES["users"])
employees_bronze = latest_bronze(SOURCE_TABLES["employees"])

users_df = (
    users_bronze
    .select(
        F.col("record_id").alias("user_id"),
        j_string("name").alias("user_name"),
        F.lower(F.trim(j_string("login"))).alias("login"),
        F.lower(F.trim(j_string("email"))).alias("user_email"),
        j_boolean("active").alias("user_is_active"),
        j_boolean("share").alias("is_portal_user"),
        m2o_id("partner_id").alias("user_partner_id"),
        m2o_id("company_id").alias("company_id"),
        m2o_name("company_id").alias("company_name"),
        json_id_array("company_ids").alias("company_ids"),
        j_timestamp("create_date").alias("user_created_at_utc"),
        j_timestamp("write_date").alias("user_updated_at_utc"),
        F.col("ingested_at").alias("user_bronze_ingested_at"),
        F.col("run_id").alias("user_bronze_run_id"),
    )
)

employees_df = (
    employees_bronze
    .select(
        F.col("record_id").alias("employee_id"),
        j_string("name").alias("employee_name"),
        j_boolean("active").alias("employee_is_active"),
        m2o_id("user_id").alias("employee_user_id"),
        F.lower(F.trim(j_string("work_email"))).alias("work_email"),
        j_string("work_phone").alias("work_phone"),
        j_string("mobile_phone").alias("mobile_phone"),
        m2o_id("job_id").alias("job_id"),
        m2o_name("job_id").alias("job_name"),
        m2o_id("department_id").alias("department_id"),
        m2o_name("department_id").alias("department_name"),
        m2o_id("parent_id").alias("manager_employee_id"),
        m2o_name("parent_id").alias("manager_employee_name"),
        m2o_id("coach_id").alias("coach_employee_id"),
        m2o_name("coach_id").alias("coach_employee_name"),
        j_timestamp("create_date").alias("employee_created_at_utc"),
        j_timestamp("write_date").alias("employee_updated_at_utc"),
        F.col("ingested_at").alias("employee_bronze_ingested_at"),
        F.col("run_id").alias("employee_bronze_run_id"),
    )
)

agent_df = (
    users_df.alias("u")
    .join(employees_df.alias("e"), F.col("u.user_id") == F.col("e.employee_user_id"), "left")
    .select(
        F.col("u.user_id"),
        F.col("e.employee_id"),
        F.coalesce(F.col("e.employee_name"), F.col("u.user_name")).alias("agent_name"),
        F.col("u.user_name"),
        F.col("e.employee_name"),
        F.col("u.login"),
        F.coalesce(F.col("e.work_email"), F.col("u.user_email"), F.col("u.login")).alias("agent_email"),
        F.col("u.user_is_active"),
        F.col("e.employee_is_active"),
        F.col("u.is_portal_user"),
        F.col("u.user_partner_id"),
        F.col("u.company_id"),
        F.col("u.company_name"),
        F.col("e.job_id"),
        F.col("e.job_name"),
        F.col("e.department_id"),
        F.col("e.department_name"),
        F.col("e.manager_employee_id"),
        F.col("e.manager_employee_name"),
        F.col("e.coach_employee_id"),
        F.col("e.coach_employee_name"),
        F.col("e.work_phone"),
        F.col("e.mobile_phone"),
        F.col("u.user_created_at_utc"),
        F.col("u.user_updated_at_utc"),
        F.col("e.employee_created_at_utc"),
        F.col("e.employee_updated_at_utc"),
    )
    .withColumn("has_employee_match", F.col("employee_id").isNotNull())
    .withColumn("silver_updated_at", F.current_timestamp())
)

(agent_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{SILVER}.agent"))

print(f"agent: {agent_df.count()} usuarios; vinculados a empleado: {agent_df.filter('has_employee_match').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dimensiones Stage, Team y Ticket Type

# COMMAND ----------

stage_df = (
    latest_bronze(SOURCE_TABLES["stages"])
    .select(
        F.col("record_id").alias("stage_id"),
        j_string("name").alias("stage_name"),
        j_long("sequence").alias("sequence"),
        j_boolean("fold").alias("is_folded"),
        json_id_array("team_ids").alias("team_ids"),
        j_timestamp("create_date").alias("created_at_utc"),
        j_timestamp("write_date").alias("updated_at_utc"),
    )
    .withColumn("silver_updated_at", F.current_timestamp())
)

team_df = (
    latest_bronze(SOURCE_TABLES["teams"])
    .select(
        F.col("record_id").alias("team_id"),
        j_string("name").alias("team_name"),
        j_boolean("active").alias("is_active"),
        m2o_id("company_id").alias("company_id"),
        m2o_name("company_id").alias("company_name"),
        json_id_array("member_ids").alias("member_user_ids"),
        j_boolean("use_sla").alias("use_sla"),
        j_timestamp("create_date").alias("created_at_utc"),
        j_timestamp("write_date").alias("updated_at_utc"),
    )
    .withColumn("silver_updated_at", F.current_timestamp())
)

ticket_type_df = (
    latest_bronze(SOURCE_TABLES["ticket_types"])
    .select(
        F.col("record_id").alias("ticket_type_id"),
        j_string("name").alias("ticket_type_name"),
        j_long("sequence").alias("sequence"),
        j_timestamp("create_date").alias("created_at_utc"),
        j_timestamp("write_date").alias("updated_at_utc"),
    )
    .withColumn("silver_updated_at", F.current_timestamp())
)

for name, dataframe in {
    "helpdesk_stage": stage_df,
    "helpdesk_team": team_df,
    "helpdesk_ticket_type": ticket_type_df,
}.items():
    (dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{SILVER}.{name}"))
    print(f"{name}: {dataframe.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tabla Silver principal `helpdesk_ticket`

# COMMAND ----------

tickets_bronze = latest_bronze(SOURCE_TABLES["tickets"])

helpdesk_ticket_df = (
    tickets_bronze
    .select(
        # Identificación
        F.col("record_id").alias("ticket_id"),
        j_string("ticket_ref").alias("ticket_ref"),
        j_string("name").alias("ticket_subject"),
        j_boolean("active").alias("is_active"),
        j_string("description").alias("description_html"),

        # Clasificación
        j_string("priority").alias("priority_code"),
        j_string("ticket_origin").alias("ticket_origin_code"),
        j_string("kanban_state").alias("kanban_state_code"),
        j_string("x_studio_criticidad").alias("criticality_code"),
        j_string("x_studio_sla").alias("sla_html"),
        json_id_array("tag_ids").alias("tag_ids"),

        # Relaciones many2one aplanadas
        m2o_id("ticket_type_id").alias("ticket_type_id"),
        m2o_name("ticket_type_id").alias("ticket_type_name"),
        m2o_id("line_type_id").alias("line_type_id"),
        m2o_name("line_type_id").alias("line_type_name"),
        m2o_id("user_id").alias("assigned_user_id"),
        m2o_name("user_id").alias("assigned_user_name"),
        m2o_id("team_id").alias("team_id"),
        m2o_name("team_id").alias("team_name"),
        m2o_id("stage_id").alias("stage_id"),
        m2o_name("stage_id").alias("stage_name"),
        m2o_id("company_id").alias("company_id"),
        m2o_name("company_id").alias("company_name"),
        m2o_id("partner_id").alias("partner_id"),
        m2o_name("partner_id").alias("partner_name_m2o"),
        m2o_id("commercial_partner_id").alias("commercial_partner_id"),
        m2o_name("commercial_partner_id").alias("commercial_partner_name"),
        j_string("partner_name").alias("customer_name_entered"),
        j_string("partner_email").alias("customer_email_raw"),
        j_string("partner_phone").alias("customer_phone"),
        m2o_id("project_id").alias("project_id"),
        m2o_name("project_id").alias("project_name"),
        m2o_id("project_sale_order_id").alias("project_sale_order_id"),
        m2o_name("project_sale_order_id").alias("project_sale_order_name"),
        m2o_id("sale_order_id").alias("sale_order_id"),
        m2o_name("sale_order_id").alias("sale_order_name"),
        m2o_id("sale_line_id").alias("sale_line_id"),
        m2o_name("sale_line_id").alias("sale_line_name"),
        m2o_id("analytic_account_id").alias("analytic_account_id"),
        m2o_name("analytic_account_id").alias("analytic_account_name"),

        # Fechas UTC recibidas desde Odoo
        j_timestamp("create_date").alias("created_at_utc"),
        j_timestamp("write_date").alias("updated_at_utc"),
        j_timestamp("assign_date").alias("assigned_at_utc"),
        j_timestamp("close_date").alias("closed_at_utc"),
        j_timestamp("date_last_stage_update").alias("last_stage_updated_at_utc"),
        j_timestamp("sla_deadline").alias("sla_deadline_utc"),

        # Métricas temporales
        j_double("assign_hours").alias("assign_hours"),
        j_double("close_hours").alias("close_hours"),
        j_double("open_hours").alias("open_hours"),
        j_double("first_response_hours").alias("first_response_hours_odoo"),
        j_double("avg_response_hours").alias("avg_response_hours"),
        j_double("total_response_hours").alias("total_response_hours"),
        j_double("total_hours_spent").alias("total_hours_spent"),
        j_double("count_first_response_timer").alias("first_response_timer_hours"),
        j_double("count_resolution_timer").alias("resolution_timer_hours"),
        j_double("tiempo_primera_respuesta").alias("first_response_hours_custom"),
        j_double("tiempo_primera_respuesta_esperado").alias("expected_first_response_hours"),
        j_double("tiempo_resolucion_esperado").alias("expected_resolution_hours"),
        j_string("tiempo_primera_respuesta_formatted").alias("first_response_formatted_source"),
        j_string("total_hours_spent_formatted").alias("total_hours_spent_formatted_source"),

        # SLA nativo
        j_boolean("use_sla").alias("use_sla"),
        json_id_array("sla_ids").alias("sla_ids"),
        json_id_array("sla_status_ids").alias("sla_status_ids"),
        j_double("sla_deadline_hours").alias("sla_deadline_hours"),
        j_boolean("sla_reached").alias("sla_reached"),
        j_boolean("sla_reached_late").alias("sla_reached_late"),
        j_boolean("sla_fail").alias("sla_fail"),
        j_boolean("sla_success").alias("sla_success"),

        # Comunicación y satisfacción
        json_id_array("timesheet_ids").alias("timesheet_ids"),
        json_id_array("message_ids").alias("message_ids"),
        json_id_array("activity_ids").alias("activity_ids"),
        j_double("rating_last_value").alias("rating_last_value"),
        j_string("rating_last_feedback").alias("rating_last_feedback"),

        # Linaje
        F.col("source_write_date"),
        F.col("ingested_at").alias("bronze_ingested_at"),
        F.col("run_id").alias("bronze_run_id"),
    )
    # Nombre de cliente preferente
    .withColumn(
        "customer_name",
        F.coalesce(
            F.col("commercial_partner_name"),
            F.col("partner_name_m2o"),
            F.col("customer_name_entered"),
            F.lit("SIN CLIENTE")
        )
    )
    # Normalización de email
    .withColumn(
        "customer_email",
        F.lower(
            F.regexp_extract(
                F.coalesce(F.col("customer_email_raw"), F.lit("")),
                r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})",
                1
            )
        )
    )
    .withColumn(
        "customer_email",
        F.when(F.col("customer_email") == "", F.lit(None)).otherwise(F.col("customer_email"))
    )
    .withColumn(
        "customer_email_domain",
        F.lower(F.regexp_extract(F.coalesce(F.col("customer_email"), F.lit("")), r"@(.+)$", 1))
    )
    .withColumn(
        "customer_email_domain",
        F.when(F.col("customer_email_domain") == "", F.lit(None)).otherwise(F.col("customer_email_domain"))
    )
    # Tiempo de primera respuesta preferido: custom DailyTech, luego nativo Odoo
    .withColumn(
        "first_response_hours",
        F.coalesce(F.col("first_response_hours_custom"), F.col("first_response_hours_odoo"))
    )
    .withColumn("first_response_minutes", F.round(F.col("first_response_hours") * 60, 2))
    .withColumn("total_minutes_spent", F.round(F.col("total_hours_spent") * 60, 2))
    .withColumn("resolution_hours", F.coalesce(F.col("close_hours"), F.col("total_hours_spent")))
    # Estados operativos
    .withColumn("stage_name", F.coalesce(F.col("stage_name"), F.lit("NO DEFINIDO")))
    .withColumn(
        "is_closed",
        F.col("closed_at_utc").isNotNull()
        | F.lower(F.col("stage_name")).rlike("solved|closed|cerrado|resuelto|canceled|cancelado")
    )
    .withColumn(
        "operational_status",
        F.when(F.lower(F.col("stage_name")).rlike("canceled|cancelado"), "CANCELADO")
         .when(F.lower(F.col("stage_name")).rlike("solved|closed|cerrado|resuelto"), "CERRADO")
         .when(F.lower(F.col("stage_name")).rlike("hold|esperando|tercer"), "EN ESPERA")
         .otherwise("ABIERTO")
    )
    # SLA contractual embebido en ticket. No sustituye la futura tabla config.sla_rules.
    .withColumn(
        "response_sla_status_ticket",
        F.when(F.col("expected_first_response_hours").isNull(), "SIN REGLA")
         .when(F.col("first_response_hours").isNull(), "SIN MEDICIÓN")
         .when(F.col("first_response_hours") <= F.col("expected_first_response_hours"), "SI SLA")
         .otherwise("NO SLA")
    )
    .withColumn(
        "resolution_sla_status_ticket",
        F.when(F.col("expected_resolution_hours").isNull(), "SIN REGLA")
         .when(F.col("resolution_hours").isNull(), "SIN MEDICIÓN")
         .when(F.col("resolution_hours") <= F.col("expected_resolution_hours"), "SI SLA")
         .otherwise("NO SLA")
    )
    # SLA nativo Odoo conservado por separado
    .withColumn(
        "odoo_sla_status",
        F.when(F.col("sla_fail") == True, "NO SLA")
         .when(F.col("sla_reached_late") == True, "NO SLA")
         .when(F.col("sla_success") == True, "SI SLA")
         .when(F.col("sla_reached") == True, "SI SLA")
         .otherwise("SIN EVALUAR")
    )
    # Calidad y conveniencia analítica
    .withColumn("has_assignee", F.col("assigned_user_id").isNotNull())
    .withColumn("has_customer", F.col("partner_id").isNotNull())
    .withColumn("has_ticket_type", F.col("ticket_type_id").isNotNull() | F.col("line_type_id").isNotNull())
    .withColumn("has_project", F.col("project_id").isNotNull())
    .withColumn("timesheet_count", F.size(F.col("timesheet_ids")))
    .withColumn("message_count", F.size(F.col("message_ids")))
    .withColumn("activity_count", F.size(F.col("activity_ids")))
    .withColumn("created_date", F.to_date("created_at_utc"))
    .withColumn("closed_date", F.to_date("closed_at_utc"))
    .withColumn("created_year", F.year("created_at_utc"))
    .withColumn("created_month", F.month("created_at_utc"))
    .withColumn("created_year_month", F.date_format("created_at_utc", "yyyy-MM"))
    .withColumn("ticket_count", F.lit(1).cast("long"))
    .withColumn("silver_updated_at", F.current_timestamp())
)

# Validar unicidad antes de publicar
quality = helpdesk_ticket_df.agg(
    F.count("*").alias("total"),
    F.countDistinct("ticket_id").alias("unique_ids"),
    F.sum(F.when(F.col("ticket_id").isNull(), 1).otherwise(0)).alias("null_ids"),
).collect()[0]

if quality["null_ids"] != 0:
    raise RuntimeError(f"Silver contiene {quality['null_ids']} ticket_id nulos.")
if quality["total"] != quality["unique_ids"]:
    raise RuntimeError(
        f"Silver contiene duplicados: total={quality['total']}, únicos={quality['unique_ids']}"
    )

(helpdesk_ticket_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{SILVER}.helpdesk_ticket"))

print(f"helpdesk_ticket: {quality['total']} registros únicos publicados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Auditoría Silver

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT}.silver_build_runs (
    build_name STRING,
    target_table STRING,
    row_count BIGINT,
    distinct_key_count BIGINT,
    null_key_count BIGINT,
    build_status STRING,
    built_at TIMESTAMP
)
USING DELTA
""")

build_results = [
    ("02_helpdesk_silver_build", f"{SILVER}.helpdesk_ticket", helpdesk_ticket_df.count(), helpdesk_ticket_df.select("ticket_id").distinct().count(), helpdesk_ticket_df.filter(F.col("ticket_id").isNull()).count(), "SUCCESS"),
    ("02_helpdesk_silver_build", f"{SILVER}.customer", customer_df.count(), customer_df.select("partner_id").distinct().count(), customer_df.filter(F.col("partner_id").isNull()).count(), "SUCCESS"),
    ("02_helpdesk_silver_build", f"{SILVER}.agent", agent_df.count(), agent_df.select("user_id").distinct().count(), agent_df.filter(F.col("user_id").isNull()).count(), "SUCCESS"),
    ("02_helpdesk_silver_build", f"{SILVER}.helpdesk_stage", stage_df.count(), stage_df.select("stage_id").distinct().count(), stage_df.filter(F.col("stage_id").isNull()).count(), "SUCCESS"),
    ("02_helpdesk_silver_build", f"{SILVER}.helpdesk_team", team_df.count(), team_df.select("team_id").distinct().count(), team_df.filter(F.col("team_id").isNull()).count(), "SUCCESS"),
    ("02_helpdesk_silver_build", f"{SILVER}.helpdesk_ticket_type", ticket_type_df.count(), ticket_type_df.select("ticket_type_id").distinct().count(), ticket_type_df.filter(F.col("ticket_type_id").isNull()).count(), "SUCCESS"),
]

build_schema = T.StructType([
    T.StructField("build_name", T.StringType(), False),
    T.StructField("target_table", T.StringType(), False),
    T.StructField("row_count", T.LongType(), False),
    T.StructField("distinct_key_count", T.LongType(), False),
    T.StructField("null_key_count", T.LongType(), False),
    T.StructField("build_status", T.StringType(), False),
])

(
    spark.createDataFrame(build_results, build_schema)
    .withColumn("built_at", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(f"{AUDIT}.silver_build_runs")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumen de calidad

# COMMAND ----------

quality_summary = helpdesk_ticket_df.agg(
    F.count("*").alias("total_tickets"),
    F.countDistinct("ticket_id").alias("unique_ticket_ids"),
    F.sum(F.when(~F.col("has_assignee"), 1).otherwise(0)).alias("without_assignee"),
    F.sum(F.when(~F.col("has_customer"), 1).otherwise(0)).alias("without_customer"),
    F.sum(F.when(~F.col("has_ticket_type"), 1).otherwise(0)).alias("without_ticket_type"),
    F.sum(F.when(F.col("customer_email").isNull(), 1).otherwise(0)).alias("without_customer_email"),
    F.sum(F.when(F.col("first_response_hours").isNull(), 1).otherwise(0)).alias("without_first_response"),
    F.sum(F.when(F.col("criticality_code").isNull(), 1).otherwise(0)).alias("without_criticality"),
    F.sum(F.when(F.col("odoo_sla_status") == "NO SLA", 1).otherwise(0)).alias("odoo_no_sla"),
    F.sum(F.when(F.col("odoo_sla_status") == "SI SLA", 1).otherwise(0)).alias("odoo_si_sla"),
    F.sum(F.when(F.col("odoo_sla_status") == "SIN EVALUAR", 1).otherwise(0)).alias("odoo_sin_evaluar"),
    F.round(F.avg("first_response_hours"), 4).alias("avg_first_response_hours"),
    F.round(F.avg("total_hours_spent"), 4).alias("avg_total_hours_spent"),
)

display(quality_summary)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT operational_status, COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY operational_status
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT odoo_sla_status, COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY odoo_sla_status
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT criticality_code, COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY criticality_code
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM corp_dailytech.audit.silver_build_runs
# MAGIC ORDER BY built_at DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_tickets,
# MAGIC
# MAGIC     COUNT(fir*t_response_hours_custom)
# MAGIC         A* con_respuesta_custom,
# MAGIC
# MAGIC     COUNT(*irst_response_hours_odoo)
# MAGIC         *S con_respuesta_odoo,
# MAGIC
# MAGIC     ROUND(A*G(first_response_hours_custom), 4)*        AS promedio_custom,
# MAGIC
# MAGIC     R*UND(AVG(first_response_hours_odoo)* 4)
# MAGIC         AS promedio_odoo,
# MAGIC
# MAGIC    *ROUND(AVG(first_response_hours), 4*
# MAGIC         AS promedio_preferido,
# MAGIC
# MAGIC   COUNT_IF(
# MAGIC         first_response*hours_custom IS NOT NULL
# MAGIC         A*D first_response_hours_odoo IS NOT*NULL
# MAGIC         AND ABS(
# MAGIC             *irst_response_hours_custom
# MAGIC        *    - first_response_hours_odoo
# MAGIC       ) > 0.01
# MAGIC     ) AS valores_dif*rentes
# MAGIC
# MAGIC FROM corp_dailytech.silver*helpdesk_ticket;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_tickets,
# MAGIC
# MAGIC     COUNT(first_response_hours_custom) AS con_respuesta_custom,
# MAGIC
# MAGIC     COUNT(first_response_hours_odoo) AS con_respuesta_odoo,
# MAGIC
# MAGIC     ROUND(AVG(first_response_hours_custom), 4) AS promedio_custom,
# MAGIC
# MAGIC     ROUND(AVG(first_response_hours_odoo), 4) AS promedio_odoo,
# MAGIC
# MAGIC     ROUND(AVG(first_response_hours), 4) AS promedio_preferido,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN first_response_hours_custom IS NOT NULL
# MAGIC              AND first_response_hours_odoo IS NOT NULL
# MAGIC              AND ABS(
# MAGIC                     first_response_hours_custom
# MAGIC                     - first_response_hours_odoo
# MAGIC                  ) > 0.01
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS valores_diferentes
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     ticket_ref,
# MAGIC     ticket_subject,
# MAGIC     created_at_utc,
# MAGIC     first_response_hours_custom,
# MAGIC     first_response_hours_odoo,
# MAGIC     first_response_hours,
# MAGIC
# MAGIC     ROUND(
# MAGIC         ABS(
# MAGIC             first_response_hours_custom
# MAGIC             - first_response_hours_odoo
# MAGIC         ),
# MAGIC         4
# MAGIC     ) AS diferencia_horas
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC
# MAGIC WHERE first_response_hours_custom IS NOT NULL
# MAGIC   AND first_response_hours_odoo IS NOT NULL
# MAGIC
# MAGIC ORDER BY diferencia_horas DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN first_response_hours_custom IS NOT NULL
# MAGIC             THEN 'CUSTOM DAILYTECH'
# MAGIC         WHEN first_response_hours_odoo IS NOT NULL
# MAGIC             THEN 'NATIVO ODOO'
# MAGIC         ELSE 'SIN MEDICIÓN'
# MAGIC     END AS fuente_primera_respuesta,
# MAGIC
# MAGIC     COUNT(*) AS tickets,
# MAGIC
# MAGIC     ROUND(AVG(first_response_hours), 4)
# MAGIC         AS promedio_horas
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN first_response_hours_custom IS NOT NULL
# MAGIC             THEN 'CUSTOM DAILYTECH'
# MAGIC         WHEN first_response_hours_odoo IS NOT NULL
# MAGIC             THEN 'NATIVO ODOO'
# MAGIC         ELSE 'SIN MEDICIÓN'
# MAGIC     END
# MAGIC
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     operational_status,
# MAGIC     COUNT(*) AS tickets,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(first_response_hours_custom),
# MAGIC         4
# MAGIC     ) AS promedio_custom,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(first_response_hours_odoo),
# MAGIC         4
# MAGIC     ) AS promedio_odoo,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(first_response_hours),
# MAGIC         4
# MAGIC     ) AS promedio_preferido
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC
# MAGIC GROUP BY operational_status
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS tickets_no_cancelados,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(first_response_hours_custom),
# MAGIC         4
# MAGIC     ) AS promedio_custom,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(first_response_hours_odoo),
# MAGIC         4
# MAGIC     ) AS promedio_odoo,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(first_response_hours),
# MAGIC         4
# MAGIC     ) AS promedio_preferido
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC
# MAGIC WHERE operational_status <> 'CANCELADO';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     ticket_ref,
# MAGIC     operational_status,
# MAGIC     created_at_utc,
# MAGIC     assigned_at_utc,
# MAGIC     closed_at_utc,
# MAGIC     first_response_hours_odoo,
# MAGIC     first_response_hours_custom,
# MAGIC     first_response_timer_hours,
# MAGIC     avg_response_hours,
# MAGIC     total_response_hours
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC ORDER BY ABS(
# MAGIC     first_response_hours_custom
# MAGIC     - first_response_hours_odoo
# MAGIC ) DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     operational_status,
# MAGIC
# MAGIC     ROUND(
# MAGIC         percentile_approx(
# MAGIC             first_response_hours_odoo,
# MAGIC             0.50
# MAGIC         ),
# MAGIC         4
# MAGIC     ) AS mediana_odoo,
# MAGIC
# MAGIC     ROUND(
# MAGIC         percentile_approx(
# MAGIC             first_response_hours_odoo,
# MAGIC             0.90
# MAGIC         ),
# MAGIC         4
# MAGIC     ) AS percentil_90_odoo,
# MAGIC
# MAGIC     ROUND(
# MAGIC         percentile_approx(
# MAGIC             first_response_hours_custom,
# MAGIC             0.50
# MAGIC         ),
# MAGIC         4
# MAGIC     ) AS mediana_custom,
# MAGIC
# MAGIC     ROUND(
# MAGIC         percentile_approx(
# MAGIC             first_response_hours_custom,
# MAGIC             0.90
# MAGIC         ),
# MAGIC         4
# MAGIC     ) AS percentil_90_custom
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC
# MAGIC GROUP BY operational_status
# MAGIC ORDER BY operational_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     ticket_ref,
# MAGIC     ticket_subject,
# MAGIC     operational_status,
# MAGIC     first_response_hours_custom,
# MAGIC     first_response_formatted_source,
# MAGIC     first_response_hours_odoo
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC WHERE first_response_hours_custom > 24
# MAGIC ORDER BY first_response_hours_custom DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     ticket_ref,
# MAGIC     first_response_hours_custom,
# MAGIC
# MAGIC     CONCAT(
# MAGIC         CAST(FLOOR(first_response_hours_custom) AS STRING),
# MAGIC         ':',
# MAGIC         LPAD(
# MAGIC             CAST(
# MAGIC                 FLOOR(
# MAGIC                     (first_response_hours_custom
# MAGIC                     - FLOOR(first_response_hours_custom)) * 60
# MAGIC                 ) AS STRING
# MAGIC             ),
# MAGIC             2,
# MAGIC             '0'
# MAGIC         )
# MAGIC     ) AS primera_respuesta_hh_mm
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC ORDER BY first_response_hours_custom DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticket_id,
# MAGIC     ticket_ref,
# MAGIC     first_response_hours_custom,
# MAGIC
# MAGIC     CONCAT(
# MAGIC         CAST(
# MAGIC             FLOOR(
# MAGIC                 ROUND(first_response_hours_custom * 60, 0) / 60
# MAGIC             ) AS STRING
# MAGIC         ),
# MAGIC         ':',
# MAGIC         LPAD(
# MAGIC             CAST(
# MAGIC                 MOD(
# MAGIC                     CAST(
# MAGIC                         ROUND(first_response_hours_custom * 60, 0)
# MAGIC                         AS BIGINT
# MAGIC                     ),
# MAGIC                     60
# MAGIC                 ) AS STRING
# MAGIC             ),
# MAGIC             2,
# MAGIC             '0'
# MAGIC         )
# MAGIC     ) AS primera_respuesta_hh_mm_redondeada
# MAGIC
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC ORDER BY first_response_hours_custom DESC
# MAGIC LIMIT 100;