-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Setup do Unity Catalog (Lakehouse de Saúde)
-- MAGIC
-- MAGIC Este notebook é responsável pela inicialização da estrutura de governança no **Unity Catalog** para o ambiente de desenvolvimento (`catalog_dev`).
-- MAGIC
-- MAGIC > **Instruções de Execução:**
-- MAGIC > - Execute com um *Storage Admin* ou *Metastore Admin*.
-- MAGIC > - Certifique-se de ajustar os caminhos do Azure Data Lake Storage Gen2 (ADLS Gen2) de acordo com o ambiente.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_dev
COMMENT 'Catálogo de desenvolvimento do lakehouse de Saúde';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schemas do Sistema SISCAD (Cadastro de Beneficiários e Vidas)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.siscad_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/siscad/bronze/'
COMMENT 'Ingestão bruta e auditável do SISCAD';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.siscad_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/siscad/silver/'
COMMENT 'Dados validados e conformados do SISCAD';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schemas do Sistema SISGUIAS (Autorizador de Guias e Eventos Médicos)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisguias_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/bronze/'
COMMENT 'Ingestão bruta e auditável do SISGUIAS';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisguias_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/silver/'
COMMENT 'Dados validados e conformados do SISGUIAS';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schemas do Sistema SISREDE (Gestão da Rede Credenciada)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisrede_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/bronze/'
COMMENT 'Ingestão bruta e auditável do SISREDE';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisrede_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/silver/'
COMMENT 'Dados validados e conformados do SISREDE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema da Camada Gold (Modelo Dimensional Unificado)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.gold
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/gold/'
COMMENT 'Camada analítica unificada de dados de Saúde';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Governança e Locais Externos (Referência e Exemplo)
-- MAGIC
-- MAGIC > **Nota de Infraestrutura:**
-- MAGIC > Os Locais Externos (*External Locations*) e Credenciais de Armazenamento (*Storage Credentials*) devem ser configurados uma única vez pela equipe de plataforma/Infra.
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Exemplo de criação de External Location:
-- MAGIC -- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_health_dev
-- MAGIC -- URL 'abfss://dev@sthealthdev.dfs.core.windows.net/'
-- MAGIC -- WITH (STORAGE CREDENTIAL health_dev_credential);
-- MAGIC
-- MAGIC -- Exemplo de RBAC / Concessões de Acesso:
-- MAGIC -- GRANT USE CATALOG ON CATALOG catalog_dev TO `group-health-data-engineers`;
-- MAGIC -- GRANT USE SCHEMA ON SCHEMA catalog_dev.gold TO `group-health-analysts`;
-- MAGIC -- GRANT SELECT ON SCHEMA catalog_dev.gold TO `group-health-analysts`;