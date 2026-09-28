-- Bronze: ingestão auditável dos quatro arquivos CSV de Saúde em Delta.
-- Publique a pasta data/ na landing zone e informe o caminho em ${landing_path}.

CREATE TABLE IF NOT EXISTS catalog_dev.siscad_bronze.beneficiario (
  id_beneficiario STRING,
  nome STRING,
  data_nascimento STRING,
  sexo STRING,
  plano STRING,
  cidade STRING,
  estado STRING,
  data_cadastro STRING,
  status STRING,
  dt_ingestao TIMESTAMP,
  source_file STRING,
  source_system STRING,
  ingestion_run_id STRING,
  _rescued_data STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'bronze'
)
COMMENT 'Cadastro de beneficiários do SISCAD, preservado para auditoria';

-- COPY INTO é idempotente para cargas por arquivo: arquivos já registrados não são
-- processados novamente. O identificador da execução permite rastrear cada lote.
COPY INTO catalog_dev.siscad_bronze.beneficiario
FROM (
  SELECT
    id_beneficiario, nome, data_nascimento, sexo, plano, cidade, estado,
    data_cadastro, status, current_timestamp(), _metadata.file_path,
    'siscad', '${ingestion_run_id}', _rescued_data
  FROM '${landing_path}/beneficiarios.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false', 'rescuedDataColumn' = '_rescued_data');

CREATE TABLE IF NOT EXISTS catalog_dev.sisrede_bronze.prestador (
  id_prestador STRING,
  nome_prestador STRING,
  especialidade STRING,
  cidade STRING,
  estado STRING,
  tipo_prestador STRING,
  status STRING,
  dt_ingestao TIMESTAMP,
  source_file STRING,
  source_system STRING,
  ingestion_run_id STRING,
  _rescued_data STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')
COMMENT 'Rede credenciada do SISREDE, preservada para auditoria';

COPY INTO catalog_dev.sisrede_bronze.prestador
FROM (
  SELECT
    id_prestador, nome_prestador, especialidade, cidade, estado,
    tipo_prestador, status, current_timestamp(), _metadata.file_path,
    'sisrede', '${ingestion_run_id}', _rescued_data
  FROM '${landing_path}/prestadores.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false', 'rescuedDataColumn' = '_rescued_data');

CREATE TABLE IF NOT EXISTS catalog_dev.sisguias_bronze.atendimento (
  id_atendimento STRING,
  id_beneficiario STRING,
  id_prestador STRING,
  data_atendimento STRING,
  tipo_atendimento STRING,
  procedimento STRING,
  valor STRING,
  status STRING,
  dt_ingestao TIMESTAMP,
  source_file STRING,
  source_system STRING,
  ingestion_run_id STRING,
  _rescued_data STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')
COMMENT 'Atendimentos do SISGUIAS, preservados para auditoria';

COPY INTO catalog_dev.sisguias_bronze.atendimento
FROM (
  SELECT
    id_atendimento, id_beneficiario, id_prestador, data_atendimento,
    tipo_atendimento, procedimento, valor, status, current_timestamp(),
    _metadata.file_path, 'sisguias', '${ingestion_run_id}', _rescued_data
  FROM '${landing_path}/atendimentos.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false', 'rescuedDataColumn' = '_rescued_data');

CREATE TABLE IF NOT EXISTS catalog_dev.sisguias_bronze.sinistro (
  id_sinistro STRING,
  id_beneficiario STRING,
  id_prestador STRING,
  data_sinistro STRING,
  tipo_despesa STRING,
  valor STRING,
  status STRING,
  dt_ingestao TIMESTAMP,
  source_file STRING,
  source_system STRING,
  ingestion_run_id STRING,
  _rescued_data STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')
COMMENT 'Sinistros do SISGUIAS, preservados para auditoria';

COPY INTO catalog_dev.sisguias_bronze.sinistro
FROM (
  SELECT
    id_sinistro, id_beneficiario, id_prestador, data_sinistro,
    tipo_despesa, valor, status, current_timestamp(), _metadata.file_path,
    'sisguias', '${ingestion_run_id}', _rescued_data
  FROM '${landing_path}/sinistros.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false', 'rescuedDataColumn' = '_rescued_data');

DESCRIBE HISTORY catalog_dev.siscad_bronze.beneficiario;
