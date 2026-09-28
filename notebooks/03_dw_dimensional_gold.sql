-- Modelo dimensional Gold unificado com histórico SCD Tipo 2 de beneficiários.

CREATE TABLE IF NOT EXISTS catalog_dev.gold.dim_beneficiario (
  beneficiario_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  id_beneficiario STRING NOT NULL,
  nome STRING,
  data_nascimento DATE,
  sexo STRING,
  plano STRING,
  cidade STRING,
  estado STRING,
  data_cadastro DATE,
  status STRING,
  atributos_hash STRING,
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP NOT NULL,
  is_current BOOLEAN NOT NULL,
  dt_processamento TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (id_beneficiario, is_current)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
)
COMMENT 'Dimensão conformada de beneficiários com histórico SCD Tipo 2';

CREATE OR REPLACE TEMP VIEW beneficiario_changes AS
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
  SHA2(CONCAT_WS('||',
    COALESCE(nome, ''),
    COALESCE(data_nascimento, ''),
    COALESCE(sexo, ''),
    COALESCE(plano, ''),
    COALESCE(cidade, ''),
    COALESCE(estado, ''),
    COALESCE(data_cadastro, ''),
    COALESCE(status, '')
  ), 256) AS atributos_hash,
  CAST(dt_processamento AS TIMESTAMP) AS change_timestamp
FROM catalog_dev.siscad_silver.beneficiario;

-- A primeira etapa encerra as versões correntes alteradas.
-- A segunda etapa insere as novas versões.
MERGE INTO catalog_dev.gold.dim_beneficiario AS target
USING beneficiario_changes AS source
ON target.id_beneficiario = source.id_beneficiario
AND target.is_current = true
WHEN MATCHED AND target.atributos_hash <> source.atributos_hash THEN
  UPDATE SET
    valid_to = source.change_timestamp,
    is_current = false,
    dt_processamento = current_timestamp();

INSERT INTO catalog_dev.gold.dim_beneficiario (
  id_beneficiario,
  nome,
  data_nascimento,
  sexo,
  plano,
  cidade,
  estado,
  data_cadastro,
  status,
  atributos_hash,
  valid_from,
  valid_to,
  is_current,
  dt_processamento
)
SELECT
  source.id_beneficiario,
  source.nome,
  source.data_nascimento,
  source.sexo,
  source.plano,
  source.cidade,
  source.estado,
  source.data_cadastro,
  source.status,
  source.atributos_hash,
  source.change_timestamp,
  TIMESTAMP '9999-12-31 23:59:59',
  true,
  current_timestamp()
FROM beneficiario_changes AS source
LEFT JOIN catalog_dev.gold.dim_beneficiario AS target
  ON target.id_beneficiario = source.id_beneficiario
 AND target.is_current = true
 AND target.atributos_hash = source.atributos_hash
WHERE target.id_beneficiario IS NULL;

CREATE TABLE IF NOT EXISTS catalog_dev.gold.dim_prestador (
  prestador_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  id_prestador STRING NOT NULL,
  nome_prestador STRING,
  especialidade STRING,
  cidade STRING,
  estado STRING,
  tipo_prestador STRING,
  status STRING,
  dt_processamento TIMESTAMP
)
USING DELTA
CLUSTER BY (id_prestador, especialidade)
COMMENT 'Dimensão conformada da rede credenciada';

MERGE INTO catalog_dev.gold.dim_prestador AS target
USING catalog_dev.sisrede_silver.prestador AS source
ON target.id_prestador = source.id_prestador
WHEN MATCHED THEN UPDATE SET
  nome_prestador = source.nome_prestador,
  especialidade = source.especialidade,
  cidade = source.cidade,
  estado = source.estado,
  tipo_prestador = source.tipo_prestador,
  status = source.status,
  dt_processamento = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
  id_prestador, nome_prestador, especialidade, cidade, estado,
  tipo_prestador, status, dt_processamento
) VALUES (
  source.id_prestador, source.nome_prestador, source.especialidade,
  source.cidade, source.estado, source.tipo_prestador, source.status,
  current_timestamp()
);

CREATE TABLE IF NOT EXISTS catalog_dev.gold.fct_atendimento (
  atendimento_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  beneficiario_sk BIGINT,
  num_guia STRING NOT NULL,
  prestador_sk BIGINT,
  data_atendimento DATE,
  valor DECIMAL(18, 2),
  tipo_atendimento STRING,
  status STRING,
  dt_processamento TIMESTAMP
)
USING DELTA
CLUSTER BY (data_atendimento, beneficiario_sk)
COMMENT 'Fato de eventos assistenciais na granularidade da guia';

-- Ao carregar o fato, associe cada atendimento às versões das dimensões
-- vigentes na data do evento.
INSERT INTO catalog_dev.gold.fct_atendimento (
  beneficiario_sk, num_guia, prestador_sk, data_atendimento, valor,
  tipo_atendimento, status, dt_processamento
)
SELECT
  beneficiario.beneficiario_sk,
  atendimento.id_atendimento,
  prestador.prestador_sk,
  atendimento.data_atendimento,
  atendimento.valor,
  atendimento.tipo_atendimento,
  atendimento.status,
  current_timestamp()
FROM catalog_dev.sisguias_silver.atendimento AS atendimento
LEFT JOIN catalog_dev.gold.dim_beneficiario AS beneficiario
  ON atendimento.id_beneficiario = beneficiario.id_beneficiario
 AND atendimento.data_atendimento >= CAST(beneficiario.valid_from AS DATE)
 AND atendimento.data_atendimento < CAST(beneficiario.valid_to AS DATE)
LEFT JOIN catalog_dev.gold.dim_prestador AS prestador
  ON atendimento.id_prestador = prestador.id_prestador;

OPTIMIZE catalog_dev.gold.dim_beneficiario
ZORDER BY (id_beneficiario, is_current);

OPTIMIZE catalog_dev.gold.fct_atendimento
ZORDER BY (data_atendimento, beneficiario_sk);

CREATE TABLE IF NOT EXISTS catalog_dev.gold.fct_sinistro (
  sinistro_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  beneficiario_sk BIGINT,
  prestador_sk BIGINT,
  id_sinistro STRING NOT NULL,
  data_sinistro DATE,
  tipo_despesa STRING,
  valor DECIMAL(18, 2),
  status STRING,
  dt_processamento TIMESTAMP
)
USING DELTA
CLUSTER BY (data_sinistro, beneficiario_sk)
COMMENT 'Fato de despesas assistenciais na granularidade do sinistro';

INSERT INTO catalog_dev.gold.fct_sinistro (
  beneficiario_sk, prestador_sk, id_sinistro, data_sinistro,
  tipo_despesa, valor, status, dt_processamento
)
SELECT
  beneficiario.beneficiario_sk,
  prestador.prestador_sk,
  sinistro.id_sinistro,
  sinistro.data_sinistro,
  sinistro.tipo_despesa,
  sinistro.valor,
  sinistro.status,
  current_timestamp()
FROM catalog_dev.sisguias_silver.sinistro AS sinistro
LEFT JOIN catalog_dev.gold.dim_beneficiario AS beneficiario
  ON sinistro.id_beneficiario = beneficiario.id_beneficiario
 AND sinistro.data_sinistro >= CAST(beneficiario.valid_from AS DATE)
 AND sinistro.data_sinistro < CAST(beneficiario.valid_to AS DATE)
LEFT JOIN catalog_dev.gold.dim_prestador AS prestador
  ON sinistro.id_prestador = prestador.id_prestador;

OPTIMIZE catalog_dev.gold.dim_prestador
ZORDER BY (id_prestador, especialidade);

OPTIMIZE catalog_dev.gold.fct_sinistro
ZORDER BY (data_sinistro, beneficiario_sk);

-- Execute somente após validar a política de retenção, os consumidores dependentes
-- e a necessidade de consultar versões anteriores dos dados.
VACUUM catalog_dev.gold.dim_beneficiario RETAIN 168 HOURS;
VACUUM catalog_dev.gold.dim_prestador RETAIN 168 HOURS;
VACUUM catalog_dev.gold.fct_atendimento RETAIN 168 HOURS;
VACUUM catalog_dev.gold.fct_sinistro RETAIN 168 HOURS;
