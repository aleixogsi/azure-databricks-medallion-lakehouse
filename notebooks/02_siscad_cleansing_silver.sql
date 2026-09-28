-- Silver do SISCAD: tipagem, padronização e deduplicação determinística.

CREATE OR REPLACE TEMP VIEW beneficiario_ranked AS
SELECT
  TRIM(id_beneficiario) AS id_beneficiario,
  INITCAP(TRIM(nome)) AS nome,
  TO_DATE(data_nascimento, 'yyyy-MM-dd') AS data_nascimento,
  UPPER(TRIM(sexo)) AS sexo,
  UPPER(TRIM(plano)) AS plano,
  INITCAP(TRIM(cidade)) AS cidade,
  UPPER(TRIM(estado)) AS estado,
  TO_DATE(data_cadastro, 'yyyy-MM-dd') AS data_cadastro,
  UPPER(TRIM(status)) AS status,
  dt_ingestao,
  source_file,
  ingestion_run_id,
  ROW_NUMBER() OVER (
    PARTITION BY TRIM(id_beneficiario)
    ORDER BY dt_ingestao DESC,
             source_file DESC,
             ingestion_run_id DESC
  ) AS row_number
FROM catalog_dev.siscad_bronze.beneficiario
WHERE _rescued_data IS NULL
  AND id_beneficiario IS NOT NULL
  AND TRIM(id_beneficiario) <> ''
  AND TRY_TO_DATE(data_nascimento, 'yyyy-MM-dd') IS NOT NULL;

CREATE TABLE IF NOT EXISTS catalog_dev.siscad_silver.beneficiario (
  id_beneficiario STRING,
  nome STRING,
  data_nascimento DATE,
  sexo STRING,
  plano STRING,
  cidade STRING,
  estado STRING,
  data_cadastro DATE,
  status STRING,
  dt_processamento TIMESTAMP,
  source_file STRING,
  ingestion_run_id STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'silver'
)
COMMENT 'Beneficiários do SISCAD padronizados, validados e deduplicados';

MERGE INTO catalog_dev.siscad_silver.beneficiario AS target
USING (
  SELECT
    id_beneficiario,
    nome,
    data_nascimento,
    sexo,
    plano,
    cidade,
    estado,
    data_cadastro,
    status,
    current_timestamp() AS dt_processamento,
    source_file,
    ingestion_run_id
  FROM beneficiario_ranked
  WHERE row_number = 1
) AS source
ON target.id_beneficiario = source.id_beneficiario
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Verificação básica de qualidade para a carga processada.
SELECT COUNT(*) AS invalid_keys
FROM catalog_dev.siscad_silver.beneficiario
WHERE id_beneficiario IS NULL;

OPTIMIZE catalog_dev.siscad_silver.beneficiario
ZORDER BY (id_beneficiario, plano);

CREATE OR REPLACE TEMP VIEW prestador_ranked AS
SELECT
  TRIM(id_prestador) AS id_prestador,
  INITCAP(TRIM(nome_prestador)) AS nome_prestador,
  INITCAP(TRIM(especialidade)) AS especialidade,
  INITCAP(TRIM(cidade)) AS cidade,
  UPPER(TRIM(estado)) AS estado,
  UPPER(TRIM(tipo_prestador)) AS tipo_prestador,
  UPPER(TRIM(status)) AS status,
  dt_ingestao,
  source_file,
  ingestion_run_id,
  ROW_NUMBER() OVER (PARTITION BY TRIM(id_prestador) ORDER BY dt_ingestao DESC, source_file DESC) AS row_number
FROM catalog_dev.sisrede_bronze.prestador
WHERE _rescued_data IS NULL
  AND id_prestador IS NOT NULL
  AND TRIM(id_prestador) <> '';

CREATE OR REPLACE TABLE catalog_dev.sisrede_silver.prestador AS
SELECT * EXCEPT (row_number)
FROM prestador_ranked
WHERE row_number = 1;

CREATE OR REPLACE TABLE catalog_dev.sisguias_silver.atendimento AS
SELECT
  TRIM(id_atendimento) AS id_atendimento,
  NULLIF(TRIM(id_beneficiario), '') AS id_beneficiario,
  NULLIF(TRIM(id_prestador), '') AS id_prestador,
  COALESCE(TRY_TO_DATE(data_atendimento, 'yyyy-MM-dd'), TRY_TO_DATE(data_atendimento, 'dd/MM/yyyy')) AS data_atendimento,
  UPPER(TRIM(tipo_atendimento)) AS tipo_atendimento,
  INITCAP(TRIM(procedimento)) AS procedimento,
  TRY_CAST(REPLACE(TRIM(valor), ',', '.') AS DECIMAL(18, 2)) AS valor,
  UPPER(TRIM(status)) AS status,
  dt_ingestao,
  source_file,
  ingestion_run_id
FROM catalog_dev.sisguias_bronze.atendimento
WHERE _rescued_data IS NULL
  AND id_atendimento IS NOT NULL
  AND TRIM(id_atendimento) <> '';

CREATE OR REPLACE TABLE catalog_dev.sisguias_silver.sinistro AS
SELECT
  TRIM(id_sinistro) AS id_sinistro,
  NULLIF(TRIM(id_beneficiario), '') AS id_beneficiario,
  NULLIF(TRIM(id_prestador), '') AS id_prestador,
  TRY_TO_DATE(data_sinistro, 'yyyy-MM-dd') AS data_sinistro,
  UPPER(TRIM(tipo_despesa)) AS tipo_despesa,
  TRY_CAST(REPLACE(TRIM(valor), ',', '.') AS DECIMAL(18, 2)) AS valor,
  UPPER(TRIM(status)) AS status,
  dt_ingestao,
  source_file,
  ingestion_run_id
FROM catalog_dev.sisguias_bronze.sinistro
WHERE _rescued_data IS NULL
  AND id_sinistro IS NOT NULL
  AND TRIM(id_sinistro) <> '';

OPTIMIZE catalog_dev.sisrede_silver.prestador ZORDER BY (id_prestador, especialidade);
OPTIMIZE catalog_dev.sisguias_silver.atendimento ZORDER BY (data_atendimento, id_beneficiario);
OPTIMIZE catalog_dev.sisguias_silver.sinistro ZORDER BY (data_sinistro, id_beneficiario);
