-- Databricks notebook source
-- COMMAND ----------

%md
# 03 - Modelagem Dimensional Conformada (Camada Gold)

Este notebook implementa o modelo dimensional Star Schema da Camada Gold, unificando as entidades de negócio a partir da Camada Silver.

> **Destaques de Arquitetura e Engenharia de Dados:**
> - **SCD Tipo 2 (Slowly Changing Dimensions):** Modelagem de histórico temporal na `dim_beneficiario` utilizando hashing MD5/SHA2 (`atributos_hash`), controle de vigência (`valid_from`, `valid_to`) e flag de versão corrente (`is_current`).
> - **Surrogate Keys Nativas:** Utilização de `BIGINT GENERATED ALWAYS AS IDENTITY` para geração automática de chaves substitutas.
> - **Point-in-Time Joins:** Associação nas tabelas de fato (`fct_atendimento` e `fct_sinistro`) ligando os eventos à versão exata do beneficiário vigente na data da ocorrência.
> - **Otimizações do Delta Lake:** Uso de `CLUSTER BY` (Liquid Clustering) / `Z-ORDER` e limpeza periódica de arquivos legados com `VACUUM`.

-- COMMAND ----------

%md
## 1. Dimensão Beneficiário (`dim_beneficiario`) - SCD Tipo 2

-- COMMAND ----------

%md
### 1.1. DDL da Tabela Dimensão Beneficiário

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 1.2. Temp View com Cálculo de Hash dos Atributos

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 1.3. Carga SCD Tipo 2 - Etapa 1: Expiração das Versões Anteriores Alteradas

-- COMMAND ----------

MERGE INTO catalog_dev.gold.dim_beneficiario AS target
USING beneficiario_changes AS source
ON target.id_beneficiario = source.id_beneficiario
AND target.is_current = true
WHEN MATCHED AND target.atributos_hash <> source.atributos_hash THEN
  UPDATE SET
    valid_to = source.change_timestamp,
    is_current = false,
    dt_processamento = current_timestamp();

-- COMMAND ----------

%md
### 1.4. Carga SCD Tipo 2 - Etapa 2: Inserção das Novas Versões e Novos Registros

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 1.5. Otimização de Armazenamento - Dimensão Beneficiário

-- COMMAND ----------

OPTIMIZE catalog_dev.gold.dim_beneficiario
ZORDER BY (id_beneficiario, is_current);

-- COMMAND ----------

%md
## 2. Dimensão Prestador (`dim_prestador`) - SCD Tipo 1 (Upsert)

-- COMMAND ----------

%md
### 2.1. DDL da Tabela Dimensão Prestador

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 2.2. Upsert (MERGE) na Dimensão Prestador

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 2.3. Otimização de Armazenamento - Dimensão Prestador

-- COMMAND ----------

OPTIMIZE catalog_dev.gold.dim_prestador
ZORDER BY (id_prestador, especialidade);

-- COMMAND ----------

%md
## 3. Tabela Fato de Atendimentos (`fct_atendimento`)

-- COMMAND ----------

%md
### 3.1. DDL da Tabela Fato de Atendimentos

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 3.2. Carga do Fato com Lookup Temporal (Point-in-Time Join)

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 3.3. Otimização de Armazenamento - Fato Atendimento

-- COMMAND ----------

OPTIMIZE catalog_dev.gold.fct_atendimento
ZORDER BY (data_atendimento, beneficiario_sk);

-- COMMAND ----------

%md
## 4. Tabela Fato de Sinistros (`fct_sinistro`)

-- COMMAND ----------

%md
### 4.1. DDL da Tabela Fato de Sinistros

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 4.2. Carga do Fato com Lookup Temporal (Point-in-Time Join)

-- COMMAND ----------

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

-- COMMAND ----------

%md
### 4.3. Otimização de Armazenamento - Fato Sinistro

-- COMMAND ----------

OPTIMIZE catalog_dev.gold.fct_sinistro
ZORDER BY (data_sinistro, beneficiario_sk);

-- COMMAND ----------

%md
## 5. Manutenção e Retenção de Dados (VACUUM)

> **Nota:** Execute os comandos abaixo apenas após validar a política de retenção, consumidores dependentes e a necessidade de time-travel.

-- COMMAND ----------

VACUUM catalog_dev.gold.dim_beneficiario RETAIN 168 HOURS;
VACUUM catalog_dev.gold.dim_prestador RETAIN 168 HOURS;
VACUUM catalog_dev.gold.fct_atendimento RETAIN 168 HOURS;
VACUUM catalog_dev.gold.fct_sinistro RETAIN 168 HOURS;
