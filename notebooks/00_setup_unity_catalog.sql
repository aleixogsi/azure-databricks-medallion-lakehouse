-- DATABRICKS SOURCE
-- COMMAND ----------

%md
# 00 - Setup do Unity Catalog (Lakehouse de Saúde)

Este notebook é responsável pela inicialização da estrutura de governança no **Unity Catalog** para o ambiente de desenvolvimento (`catalog_dev`).

> **Instruções de Execução:**
> - Execute com um *Storage Admin* ou *Metastore Admin*.
> - Certifique-se de ajustar os caminhos do Azure Data Lake Storage Gen2 (ADLS Gen2) de acordo com o ambiente.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_dev
COMMENT 'Catálogo de desenvolvimento do lakehouse de Saúde';

-- COMMAND ----------

%md
### Schemas do Sistema SISCAD (Cadastro de Beneficiários e Vidas)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.siscad_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/siscad/bronze/'
COMMENT 'Ingestão bruta e auditável do SISCAD';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.siscad_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/siscad/silver/'
COMMENT 'Dados validados e conformados do SISCAD';

-- COMMAND ----------

%md
### Schemas do Sistema SISGUIAS (Autorizador de Guias e Eventos Médicos)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisguias_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/bronze/'
COMMENT 'Ingestão bruta e auditável do SISGUIAS';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisguias_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/silver/'
COMMENT 'Dados validados e conformados do SISGUIAS';

-- COMMAND ----------

%md
### Schemas do Sistema SISREDE (Gestão da Rede Credenciada)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisrede_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/bronze/'
COMMENT 'Ingestão bruta e auditável do SISREDE';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisrede_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/silver/'
COMMENT 'Dados validados e conformados do SISREDE';

-- COMMAND ----------

%md
### Schema da Camada Gold (Modelo Dimensional Unificado)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.gold
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/gold/'
COMMENT 'Camada analítica unificada de dados de Saúde';

-- COMMAND ----------

%md
### Governança e Locais Externos (Referência e Exemplo)

> **Nota de Infraestrutura:**
> Os Locais Externos (*External Locations*) e Credenciais de Armazenamento (*Storage Credentials*) devem ser configurados uma única vez pela equipe de plataforma/Infra.

```sql
-- Exemplo de criação de External Location:
-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_health_dev
-- URL 'abfss://dev@sthealthdev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL health_dev_credential);

-- Exemplo de RBAC / Concessões de Acesso:
-- GRANT USE CATALOG ON CATALOG catalog_dev TO `group-health-data-engineers`;
-- GRANT USE SCHEMA ON SCHEMA catalog_dev.gold TO `group-health-analysts`;
-- GRANT SELECT ON SCHEMA catalog_dev.gold TO `group-health-analysts`;
