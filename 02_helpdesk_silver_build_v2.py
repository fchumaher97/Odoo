# Databricks notebook source
# MAGIC %md
# MAGIC # 02_helpdesk_silver_build V2
# MAGIC Construcción idempotente de la capa Silver desde Odoo Bronze.
# MAGIC
# MAGIC **Cambio V2:** la métrica oficial provisional de primera respuesta utiliza `tiempo_primera_respuesta` (campo custom), porque coincide con el valor formateado exportado por Odoo. El campo nativo se conserva para auditoría.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

CATALOG = "corp_dailytech"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
AUDIT = f"{CATALOG}.audit"
BUILD_NAME = "02_helpdesk_silver_build_v2"

SOURCES = {
    "tickets": f"{BRONZE}.raw_helpdesk_tickets",
    "stages": f"{BRONZE}.raw_helpdesk_stages",
    "teams": f"{BRONZE}.raw_helpdesk_teams",
    "ticket_types": f"{BRONZE}.raw_helpdesk_ticket_types",
    "partners": f"{BRONZE}.raw_partners",
    "users": f"{BRONZE}.raw_users",
    "employees": f"{BRONZE}.raw_employees",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT}")

for label, table in SOURCES.items():
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Falta la tabla Bronze requerida: {table}")
    count = spark.table(table).count()
    if count == 0:
        raise RuntimeError(f"La tabla Bronze está vacía: {table}")
    print(f"OK {label}: {table} ({count} filas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers de lectura y normalización JSON

# COMMAND ----------

def j(path):
    return F.get_json_object(F.col("payload"), f"$.{path}")

def clean(c):
    return F.when(c.isNull() | F.lower(F.trim(c)).isin("", "false", "null"), F.lit(None)).otherwise(c)

def js(path): return clean(j(path))
def jl(path): return clean(j(path)).cast("long")
def jd(path): return clean(j(path)).cast("double")
def jt(path): return F.to_timestamp(clean(j(path)))

def jb(path):
    x = F.lower(F.trim(j(path)))
    return F.when(x == "true", True).when(x == "false", False).otherwise(F.lit(None).cast("boolean"))

def m2o_id(path):
    return clean(F.get_json_object(F.col("payload"), f"$.{path}[0]")).cast("long")

def m2o_name(path):
    return clean(F.get_json_object(F.col("payload"), f"$.{path}[1]"))

def id_array(path):
    x = j(path)
    return F.when(
        x.isNull() | F.lower(F.trim(x)).isin("", "false", "null"),
        F.array().cast("array<long>")
    ).otherwise(F.from_json(x, T.ArrayType(T.LongType())))

def latest(table):
    w = Window.partitionBy("source_model", "record_id").orderBy(
        F.to_timestamp("source_write_date").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last()
    )
    return spark.table(table).withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def write_table(df, name):
    target = f"{SILVER}.{name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"Publicado {target}: {df.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer

# COMMAND ----------

customer = (
    latest(SOURCES["partners"]).select(
        F.col("record_id").alias("partner_id"),
        js("name").alias("partner_name"), js("display_name").alias("display_name"),
        jb("active").alias("is_active"), jb("is_company").alias("is_company"),
        m2o_id("parent_id").alias("parent_partner_id"), m2o_name("parent_id").alias("parent_partner_name"),
        m2o_id("commercial_partner_id").alias("commercial_partner_id"),
        m2o_name("commercial_partner_id").alias("commercial_partner_name"),
        F.lower(F.trim(js("email"))).alias("email"), js("phone").alias("phone"),
        js("mobile").alias("mobile"), js("website").alias("website"),
        js("street").alias("street"), js("street2").alias("street2"), js("city").alias("city"),
        m2o_id("state_id").alias("state_id"), m2o_name("state_id").alias("state_name"),
        m2o_id("country_id").alias("country_id"), m2o_name("country_id").alias("country_name"),
        js("zip").alias("zip"), js("company_type").alias("company_type"), js("vat").alias("vat"),
        jt("create_date").alias("created_at_utc"), jt("write_date").alias("updated_at_utc"),
        F.col("ingested_at").alias("bronze_ingested_at"), F.col("run_id").alias("bronze_run_id")
    )
    .withColumn("email_domain", F.lower(F.regexp_extract(F.coalesce("email", F.lit("")), r"@([^> ]+)$", 1)))
    .withColumn("silver_updated_at", F.current_timestamp())
)
write_table(customer, "customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent consolidado: res.users + hr.employee

# COMMAND ----------

users = latest(SOURCES["users"]).select(
    F.col("record_id").alias("user_id"), js("name").alias("user_name"),
    F.lower(F.trim(js("login"))).alias("login"), F.lower(F.trim(js("email"))).alias("user_email"),
    jb("active").alias("user_is_active"), jb("share").alias("is_portal_user"),
    m2o_id("partner_id").alias("user_partner_id"),
    m2o_id("company_id").alias("company_id"), m2o_name("company_id").alias("company_name"),
    jt("create_date").alias("user_created_at_utc"), jt("write_date").alias("user_updated_at_utc")
)

employees = latest(SOURCES["employees"]).select(
    F.col("record_id").alias("employee_id"), js("name").alias("employee_name"),
    jb("active").alias("employee_is_active"), m2o_id("user_id").alias("employee_user_id"),
    F.lower(F.trim(js("work_email"))).alias("work_email"), js("work_phone").alias("work_phone"),
    js("mobile_phone").alias("mobile_phone"), m2o_id("job_id").alias("job_id"),
    m2o_name("job_id").alias("job_name"), m2o_id("department_id").alias("department_id"),
    m2o_name("department_id").alias("department_name"), m2o_id("parent_id").alias("manager_employee_id"),
    m2o_name("parent_id").alias("manager_employee_name"), m2o_id("coach_id").alias("coach_employee_id"),
    m2o_name("coach_id").alias("coach_employee_name"), jt("create_date").alias("employee_created_at_utc"),
    jt("write_date").alias("employee_updated_at_utc")
)

agent = (
    users.alias("u").join(employees.alias("e"), F.col("u.user_id") == F.col("e.employee_user_id"), "left")
    .select(
        "u.user_id", "e.employee_id", F.coalesce("e.employee_name", "u.user_name").alias("agent_name"),
        "u.user_name", "e.employee_name", "u.login",
        F.coalesce("e.work_email", "u.user_email", "u.login").alias("agent_email"),
        "u.user_is_active", "e.employee_is_active", "u.is_portal_user", "u.user_partner_id",
        "u.company_id", "u.company_name", "e.job_id", "e.job_name", "e.department_id", "e.department_name",
        "e.manager_employee_id", "e.manager_employee_name", "e.coach_employee_id", "e.coach_employee_name",
        "e.work_phone", "e.mobile_phone", "u.user_created_at_utc", "u.user_updated_at_utc",
        "e.employee_created_at_utc", "e.employee_updated_at_utc"
    )
    .withColumn("has_employee_match", F.col("employee_id").isNotNull())
    .withColumn("silver_updated_at", F.current_timestamp())
)
write_table(agent, "agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage, Team y Ticket Type

# COMMAND ----------

stage = (
    latest(SOURCES["stages"]).select(
        F.col("record_id").alias("stage_id"), js("name").alias("stage_name"), jl("sequence").alias("sequence"),
        jb("fold").alias("is_folded"), id_array("team_ids").alias("team_ids"),
        jt("create_date").alias("created_at_utc"), jt("write_date").alias("updated_at_utc")
    ).withColumn("silver_updated_at", F.current_timestamp())
)

team = (
    latest(SOURCES["teams"]).select(
        F.col("record_id").alias("team_id"), js("name").alias("team_name"), jb("active").alias("is_active"),
        m2o_id("company_id").alias("company_id"), m2o_name("company_id").alias("company_name"),
        id_array("member_ids").alias("member_user_ids"), jb("use_sla").alias("use_sla"),
        jt("create_date").alias("created_at_utc"), jt("write_date").alias("updated_at_utc")
    ).withColumn("silver_updated_at", F.current_timestamp())
)

ticket_type = (
    latest(SOURCES["ticket_types"]).select(
        F.col("record_id").alias("ticket_type_id"), js("name").alias("ticket_type_name"),
        jl("sequence").alias("sequence"), jt("create_date").alias("created_at_utc"),
        jt("write_date").alias("updated_at_utc")
    ).withColumn("silver_updated_at", F.current_timestamp())
)

write_table(stage, "helpdesk_stage")
write_table(team, "helpdesk_team")
write_table(ticket_type, "helpdesk_ticket_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpdesk Ticket V2

# COMMAND ----------

tickets = latest(SOURCES["tickets"])

helpdesk_ticket = (
    tickets.select(
        F.col("record_id").alias("ticket_id"), js("ticket_ref").alias("ticket_ref"),
        js("name").alias("ticket_subject"), jb("active").alias("is_active"),
        js("description").alias("description_html"), js("priority").alias("priority_code"),
        js("ticket_origin").alias("ticket_origin_code"), js("kanban_state").alias("kanban_state_code"),
        js("x_studio_criticidad").alias("criticality_code"), js("x_studio_sla").alias("sla_html"),
        id_array("tag_ids").alias("tag_ids"),
        m2o_id("ticket_type_id").alias("ticket_type_id"), m2o_name("ticket_type_id").alias("ticket_type_name"),
        m2o_id("line_type_id").alias("line_type_id"), m2o_name("line_type_id").alias("line_type_name"),
        m2o_id("user_id").alias("assigned_user_id"), m2o_name("user_id").alias("assigned_user_name"),
        m2o_id("team_id").alias("team_id"), m2o_name("team_id").alias("team_name"),
        m2o_id("stage_id").alias("stage_id"), m2o_name("stage_id").alias("stage_name"),
        m2o_id("company_id").alias("company_id"), m2o_name("company_id").alias("company_name"),
        m2o_id("partner_id").alias("partner_id"), m2o_name("partner_id").alias("partner_name_m2o"),
        m2o_id("commercial_partner_id").alias("commercial_partner_id"),
        m2o_name("commercial_partner_id").alias("commercial_partner_name"),
        js("partner_name").alias("customer_name_entered"), js("partner_email").alias("customer_email_raw"),
        js("partner_phone").alias("customer_phone"),
        m2o_id("project_id").alias("project_id"), m2o_name("project_id").alias("project_name"),
        m2o_id("project_sale_order_id").alias("project_sale_order_id"),
        m2o_name("project_sale_order_id").alias("project_sale_order_name"),
        m2o_id("sale_order_id").alias("sale_order_id"), m2o_name("sale_order_id").alias("sale_order_name"),
        m2o_id("sale_line_id").alias("sale_line_id"), m2o_name("sale_line_id").alias("sale_line_name"),
        m2o_id("analytic_account_id").alias("analytic_account_id"),
        m2o_name("analytic_account_id").alias("analytic_account_name"),
        jt("create_date").alias("created_at_utc"), jt("write_date").alias("updated_at_utc"),
        jt("assign_date").alias("assigned_at_utc"), jt("close_date").alias("closed_at_utc"),
        jt("date_last_stage_update").alias("last_stage_updated_at_utc"), jt("sla_deadline").alias("sla_deadline_utc"),
        jd("assign_hours").alias("assign_hours"), jd("close_hours").alias("close_hours"),
        jd("open_hours").alias("open_hours"), jd("first_response_hours").alias("first_response_hours_odoo"),
        jd("avg_response_hours").alias("avg_response_hours"), jd("total_response_hours").alias("total_response_hours"),
        jd("total_hours_spent").alias("total_hours_spent"),
        jd("count_first_response_timer").alias("first_response_timer_hours"),
        jd("count_resolution_timer").alias("resolution_timer_hours"),
        jd("tiempo_primera_respuesta").alias("first_response_hours_custom"),
        jd("tiempo_primera_respuesta_esperado").alias("expected_first_response_hours"),
        jd("tiempo_resolucion_esperado").alias("expected_resolution_hours"),
        js("tiempo_primera_respuesta_formatted").alias("first_response_formatted_source"),
        js("total_hours_spent_formatted").alias("total_hours_spent_formatted_source"),
        jb("use_sla").alias("use_sla"), id_array("sla_ids").alias("sla_ids"),
        id_array("sla_status_ids").alias("sla_status_ids"), jd("sla_deadline_hours").alias("sla_deadline_hours"),
        jb("sla_reached").alias("sla_reached"), jb("sla_reached_late").alias("sla_reached_late"),
        jb("sla_fail").alias("sla_fail"), jb("sla_success").alias("sla_success"),
        id_array("timesheet_ids").alias("timesheet_ids"), id_array("message_ids").alias("message_ids"),
        id_array("activity_ids").alias("activity_ids"), jd("rating_last_value").alias("rating_last_value"),
        js("rating_last_feedback").alias("rating_last_feedback"), F.col("source_write_date"),
        F.col("ingested_at").alias("bronze_ingested_at"), F.col("run_id").alias("bronze_run_id")
    )
    .withColumn("customer_name", F.coalesce("commercial_partner_name", "partner_name_m2o", "customer_name_entered", F.lit("SIN CLIENTE")))
    .withColumn("customer_email", F.lower(F.regexp_extract(F.coalesce("customer_email_raw", F.lit("")), r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", 1)))
    .withColumn("customer_email", F.when(F.col("customer_email") == "", None).otherwise(F.col("customer_email")))
    .withColumn("customer_email_domain", F.lower(F.regexp_extract(F.coalesce("customer_email", F.lit("")), r"@(.+)$", 1)))
    .withColumn("customer_email_domain", F.when(F.col("customer_email_domain") == "", None).otherwise(F.col("customer_email_domain")))
    .withColumn("stage_name", F.coalesce("stage_name", F.lit("NO DEFINIDO")))
    .withColumn("is_closed", F.col("closed_at_utc").isNotNull() | F.lower("stage_name").rlike("solved|closed|cerrado|resuelto|canceled|cancelado"))
    .withColumn("operational_status",
        F.when(F.lower("stage_name").rlike("canceled|cancelado"), "CANCELADO")
         .when(F.lower("stage_name").rlike("solved|closed|cerrado|resuelto"), "CERRADO")
         .when(F.lower("stage_name").rlike("hold|esperando|tercer"), "EN ESPERA")
         .otherwise("ABIERTO"))
    # V2: fuente explícita y estable
    .withColumn("first_response_hours", F.col("first_response_hours_custom"))
    .withColumn("first_response_metric_source", F.lit("ODOO_CUSTOM_TIEMPO_PRIMERA_RESPUESTA"))
    .withColumn("first_response_minutes", F.round(F.col("first_response_hours") * 60, 2))
    .withColumn("first_response_total_minutes_rounded", F.round(F.col("first_response_hours") * 60, 0).cast("long"))
    .withColumn("first_response_hh_mm_rounded",
        F.concat(
            F.floor(F.col("first_response_total_minutes_rounded") / 60).cast("string"), F.lit(":"),
            F.lpad(F.pmod(F.col("first_response_total_minutes_rounded"), F.lit(60)).cast("string"), 2, "0")
        ))
    .withColumn("first_response_difference_hours", F.round(F.abs(F.col("first_response_hours_custom") - F.col("first_response_hours_odoo")), 4))
    .withColumn("has_first_response_discrepancy", F.col("first_response_difference_hours") > 0.01)
    .withColumn("first_response_over_24h", F.col("first_response_hours") > 24)
    .withColumn("first_response_over_48h", F.col("first_response_hours") > 48)
    .withColumn("first_response_over_72h", F.col("first_response_hours") > 72)
    .withColumn("total_minutes_spent", F.round(F.col("total_hours_spent") * 60, 2))
    .withColumn("resolution_hours", F.coalesce("close_hours", "total_hours_spent"))
    .withColumn("is_sla_eligible", (F.col("operational_status") != "CANCELADO") & F.col("first_response_hours").isNotNull())
    .withColumn("sla_exclusion_reason",
        F.when(F.col("operational_status") == "CANCELADO", "TICKET CANCELADO")
         .when(F.col("first_response_hours").isNull(), "SIN MEDICION DE PRIMERA RESPUESTA")
         .otherwise(F.lit(None).cast("string")))
    .withColumn("response_sla_status_ticket",
        F.when(~F.col("is_sla_eligible"), "EXCLUIDO")
         .when(F.col("expected_first_response_hours").isNull(), "SIN REGLA")
         .when(F.col("first_response_hours") <= F.col("expected_first_response_hours"), "SI SLA")
         .otherwise("NO SLA"))
    .withColumn("resolution_sla_status_ticket",
        F.when(F.col("operational_status") == "CANCELADO", "EXCLUIDO")
         .when(F.col("expected_resolution_hours").isNull(), "SIN REGLA")
         .when(F.col("resolution_hours").isNull(), "SIN MEDICION")
         .when(F.col("resolution_hours") <= F.col("expected_resolution_hours"), "SI SLA")
         .otherwise("NO SLA"))
    .withColumn("odoo_sla_status",
        F.when(F.col("sla_fail") == True, "NO SLA")
         .when(F.col("sla_reached_late") == True, "NO SLA")
         .when(F.col("sla_success") == True, "SI SLA")
         .when(F.col("sla_reached") == True, "SI SLA")
         .otherwise("SIN EVALUAR"))
    .withColumn("has_assignee", F.col("assigned_user_id").isNotNull())
    .withColumn("has_customer", F.col("partner_id").isNotNull())
    .withColumn("has_ticket_type", F.col("ticket_type_id").isNotNull() | F.col("line_type_id").isNotNull())
    .withColumn("has_project", F.col("project_id").isNotNull())
    .withColumn("timesheet_count", F.size("timesheet_ids"))
    .withColumn("message_count", F.size("message_ids"))
    .withColumn("activity_count", F.size("activity_ids"))
    .withColumn("created_date", F.to_date("created_at_utc"))
    .withColumn("closed_date", F.to_date("closed_at_utc"))
    .withColumn("created_year", F.year("created_at_utc"))
    .withColumn("created_month", F.month("created_at_utc"))
    .withColumn("created_year_month", F.date_format("created_at_utc", "yyyy-MM"))
    .withColumn("ticket_count", F.lit(1).cast("long"))
    .withColumn("silver_updated_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones bloqueantes y publicación

# COMMAND ----------

q = helpdesk_ticket.agg(
    F.count("*").alias("total"), F.countDistinct("ticket_id").alias("unique_ids"),
    F.sum(F.when(F.col("ticket_id").isNull(), 1).otherwise(0)).alias("null_ids")
).first()

if q["null_ids"] != 0:
    raise RuntimeError(f"Hay {q['null_ids']} ticket_id nulos")
if q["total"] != q["unique_ids"]:
    raise RuntimeError(f"Hay duplicados: total={q['total']}, únicos={q['unique_ids']}")

write_table(helpdesk_ticket, "helpdesk_ticket")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditoría V2

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
) USING DELTA
""")

results = [
    (BUILD_NAME, f"{SILVER}.helpdesk_ticket", helpdesk_ticket.count(), helpdesk_ticket.select("ticket_id").distinct().count(), helpdesk_ticket.filter("ticket_id IS NULL").count(), "SUCCESS"),
    (BUILD_NAME, f"{SILVER}.customer", customer.count(), customer.select("partner_id").distinct().count(), customer.filter("partner_id IS NULL").count(), "SUCCESS"),
    (BUILD_NAME, f"{SILVER}.agent", agent.count(), agent.select("user_id").distinct().count(), agent.filter("user_id IS NULL").count(), "SUCCESS"),
    (BUILD_NAME, f"{SILVER}.helpdesk_stage", stage.count(), stage.select("stage_id").distinct().count(), stage.filter("stage_id IS NULL").count(), "SUCCESS"),
    (BUILD_NAME, f"{SILVER}.helpdesk_team", team.count(), team.select("team_id").distinct().count(), team.filter("team_id IS NULL").count(), "SUCCESS"),
    (BUILD_NAME, f"{SILVER}.helpdesk_ticket_type", ticket_type.count(), ticket_type.select("ticket_type_id").distinct().count(), ticket_type.filter("ticket_type_id IS NULL").count(), "SUCCESS"),
]

schema = "build_name string, target_table string, row_count long, distinct_key_count long, null_key_count long, build_status string"
spark.createDataFrame(results, schema).withColumn("built_at", F.current_timestamp()).write.mode("append").saveAsTable(f"{AUDIT}.silver_build_runs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Controles de salida V2

# COMMAND ----------

display(helpdesk_ticket.agg(
    F.count("*").alias("total_tickets"),
    F.countDistinct("ticket_id").alias("unique_ticket_ids"),
    F.sum(F.when(F.col("is_sla_eligible"), 1).otherwise(0)).alias("sla_eligible"),
    F.sum(F.when(~F.col("is_sla_eligible"), 1).otherwise(0)).alias("sla_excluded"),
    F.sum(F.when(F.col("has_first_response_discrepancy"), 1).otherwise(0)).alias("first_response_discrepancies"),
    F.sum(F.when(F.col("first_response_over_24h"), 1).otherwise(0)).alias("first_response_over_24h"),
    F.sum(F.when(F.col("first_response_over_48h"), 1).otherwise(0)).alias("first_response_over_48h"),
    F.sum(F.when(F.col("first_response_over_72h"), 1).otherwise(0)).alias("first_response_over_72h"),
    F.round(F.avg("first_response_hours"), 4).alias("avg_first_response_hours_all"),
    F.round(F.expr("percentile_approx(first_response_hours, 0.5)"), 4).alias("median_first_response_hours_all"),
    F.round(F.expr("percentile_approx(first_response_hours, 0.9)"), 4).alias("p90_first_response_hours_all")
))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   operational_status,
# MAGIC   COUNT(*) AS tickets,
# MAGIC   ROUND(AVG(first_response_hours), 4) AS promedio_horas,
# MAGIC   ROUND(PERCENTILE_APPROX(first_response_hours, 0.5), 4) AS mediana_horas,
# MAGIC   ROUND(PERCENTILE_APPROX(first_response_hours, 0.9), 4) AS percentil_90_horas
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY operational_status
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   is_sla_eligible,
# MAGIC   sla_exclusion_reason,
# MAGIC   COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY is_sla_eligible, sla_exclusion_reason
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   response_sla_status_ticket,
# MAGIC   COUNT(*) AS tickets
# MAGIC FROM corp_dailytech.silver.helpdesk_ticket
# MAGIC GROUP BY response_sla_status_ticket
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
# MAGIC     build_name,
# MAGIC     built_at,
# MAGIC     COUNT(*) AS objetos_publicados
# MAGIC FROM corp_dailytech.audit.silver_build_runs
# MAGIC GROUP BY build_name, built_at
# MAGIC ORDER BY built_at DESC;