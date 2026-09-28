-- Configuração do Unity Catalog para o lakehouse de Saúde.
-- Execute com um principal administrativo e substitua o nome da credencial de armazenamento.

CREATE CATALOG IF NOT EXISTS catalog_dev
COMMENT 'Catálogo de desenvolvimento do lakehouse de Saúde';

CREATE SCHEMA IF NOT EXISTS catalog_dev.siscad_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/siscad/bronze/'
COMMENT 'Ingestão bruta e auditável do SISCAD';

CREATE SCHEMA IF NOT EXISTS catalog_dev.siscad_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/siscad/silver/'
COMMENT 'Dados validados e conformados do SISCAD';

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisguias_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/bronze/'
COMMENT 'Ingestão bruta e auditável do SISGUIAS';

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisguias_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/silver/'
COMMENT 'Dados validados e conformados do SISGUIAS';

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisrede_bronze
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/bronze/'
COMMENT 'Ingestão bruta e auditável do SISREDE';

CREATE SCHEMA IF NOT EXISTS catalog_dev.sisrede_silver
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/silver/'
COMMENT 'Dados validados e conformados do SISREDE';

CREATE SCHEMA IF NOT EXISTS catalog_dev.gold
MANAGED LOCATION 'abfss://dev@sthealthdev.dfs.core.windows.net/gold/'
COMMENT 'Camada analítica unificada de dados de Saúde';

-- Locais externos e credenciais de armazenamento devem ser criados uma única vez
-- pela equipe de plataforma. Exemplo:
-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_health_dev
-- URL 'abfss://dev@sthealthdev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL health_dev_credential);

-- Revise as concessões de acesso conforme o modelo de segurança do workspace antes de habilitá-las.
-- GRANT USE CATALOG ON CATALOG catalog_dev TO `group-health-data-engineers`;
-- GRANT USE SCHEMA ON SCHEMA catalog_dev.gold TO `group-health-analysts`;
-- GRANT SELECT ON SCHEMA catalog_dev.gold TO `group-health-analysts`;
