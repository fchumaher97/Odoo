# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Databricks notebook source
# MAGIC %md
# MAGIC # 04_helpdesk_gold_build
# MAGIC
# MAGIC Construye el modelo dimensional Gold para Power BI.
# MAGIC
# MAGIC Objetivos:
# MAGIC - Mantener modelo historico (dashboard legado)
# MAGIC - Mantener modelo contractual SLA
# MAGIC - Crear dimensiones conformadas
# MAGIC - Crear FactHelpdeskTicket

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG='corp_dailytech'
SILVER=f'{CATALOG}.silver'
GOLD=f'{CATALOG}.gold'
AUDIT=f'{CATALOG}.audit'

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {GOLD}')

SOURCE=f'{SILVER}.helpdesk_ticket_contractual'
if not spark.catalog.tableExists(SOURCE):
    raise RuntimeError(f'No existe {SOURCE}')

df=spark.table(SOURCE)

# DIM FECHA
minmax=df.select(F.min('created_date').alias('d1'),F.max('created_date').alias('d2')).first()
fecha=spark.sql(f"""
select explode(sequence(to_date('{minmax['d1']}'),to_date('{minmax['d2']}'),interval 1 day)) as fecha
""").withColumn('date_key',F.date_format('fecha','yyyyMMdd').cast('int')).withColumn('anio',F.year('fecha')).withColumn('mes',F.month('fecha')).withColumn('dia',F.dayofmonth('fecha')).withColumn('anio_mes',F.date_format('fecha','yyyy-MM'))

fecha.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{GOLD}.dim_fecha')

# DIM CLIENTE
cliente=(df.select('partner_id','customer_name','customer_email','customer_email_domain')
 .dropDuplicates(['partner_id']))
cliente.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{GOLD}.dim_cliente')

# DIM AGENTE
agente=(df.select('assigned_user_id','assigned_user_name').dropDuplicates(['assigned_user_id']))
agente.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{GOLD}.dim_agente')

# DIM ENTIDAD
entidad=(df.select('entity_code','entity_name').dropDuplicates())
entidad.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{GOLD}.dim_entidad')

# DIM TIPO
tipo=(df.select('ticket_type_name','request_type_contractual')
 .withColumn('report_type',
    F.when(F.upper(F.col('ticket_type_name'))=='ISSUE','SOLICITUD')
     .when(F.upper(F.col('ticket_type_name'))=='QUESTION','CONSULTA')
     .when(F.upper(F.col('ticket_type_name')).contains('REQUER'),'REQUERIMIENTO')
     .otherwise('SIN TIPO'))
 .dropDuplicates())

tipo.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{GOLD}.dim_ticket_type')

# DIM SLA
sla=(df.select('rule_id','entity_code','contractual_criticality','target_response_hours','sla_mode').dropDuplicates())
sla.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{GOLD}.dim_sla_rule')

# FACT
fact=(df
 .withColumn('date_key',F.date_format('created_date','yyyyMMdd').cast('int'))
 .withColumn('report_type',
    F.when(F.upper(F.col('ticket_type_name'))=='ISSUE','SOLICITUD')
     .when(F.upper(F.col('ticket_type_name'))=='QUESTION','CONSULTA')
     .when(F.upper(F.col('ticket_type_name')).contains('REQUER'),'REQUERIMIENTO')
     .otherwise('SIN TIPO'))
 .withColumn('ticket_count',F.lit(1))
 .withColumn('sla_si',F.when(F.col('contractual_sla_status')=='SI SLA',1).otherwise(0))
 .withColumn('sla_no',F.when(F.col('contractual_sla_status')=='NO SLA',1).otherwise(0))
 .withColumn('sla_propio',F.when(F.col('contractual_sla_status')=='SLA PROPIO',1).otherwise(0))
 .withColumn('sin_entidad',F.when(F.col('entity_code')=='SIN ENTIDAD',1).otherwise(0))
 .select(
 'ticket_id','ticket_ref','date_key','entity_code','customer_name',
 'assigned_user_id','assigned_user_name','ticket_type_name','report_type',
 'request_type_contractual','contractual_sla_status','operational_status',
 'contractual_criticality','target_response_hours','first_response_hours',
 'total_hours_spent','ticket_count','sla_si','sla_no','sla_propio','sin_entidad'))

fact.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{GOLD}.fact_helpdesk_ticket')

print('Gold generado correctamente')
print('fact_helpdesk_ticket:', fact.count())


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM corp_dailytech.gold.dim_fecha;
# MAGIC SELECT COUNT(*) FROM corp_dailytech.gold.dim_cliente;
# MAGIC SELECT COUNT(*) FROM corp_dailytech.gold.dim_agente;
# MAGIC SELECT COUNT(*) FROM corp_dailytech.gold.dim_entidad;
# MAGIC SELECT COUNT(*) FROM corp_dailytech.gold.dim_ticket_type;
# MAGIC SELECT COUNT(*) FROM corp_dailytech.gold.dim_sla_rule;
# MAGIC SELECT COUNT(*) FROM corp_dailytech.gold.fact_helpdesk_ticket;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) total,
# MAGIC     COUNT(DISTINCT ticket_id) unicos
# MAGIC FROM corp_dailytech.gold.fact_helpdesk_ticket;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     contractual_sla_status,
# MAGIC     COUNT(*) tickets
# MAGIC FROM corp_dailytech.gold.fact_helpdesk_ticket
# MAGIC GROUP BY contractual_sla_status
# MAGIC ORDER BY tickets DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     report_type,
# MAGIC     COUNT(*) tickets
# MAGIC FROM corp_dailytech.gold*fact_helpdesk_ticket
# MAGIC GROUP BY repo*t_type
# MAGIC ORDER BY tickets DESC;