# Arquitetura do Lakehouse de Saúde

Este documento registra as principais decisões arquiteturais deste projeto de portfólio e o motivo de cada camada existir. A proposta é mostrar um caminho consistente para organizar dados de diferentes sistemas de Saúde sem acoplar o modelo analítico a uma única fonte.

## Visão geral

A arquitetura usa um catálogo por ambiente e schemas segregados por sistema de origem e camada. A Gold é unificada porque suas dimensões conformadas e fatos precisam combinar entidades de cadastro, autorizações, eventos médicos e rede credenciada.

```text
ADLS Gen2 / fontes de saúde
          |
          v
        catalog_dev.<sistema>_bronze  -- dados Delta + auditoria
          |
          v
  catalog_dev.<sistema>_silver  -- qualidade + tipagem + deduplicação
          |
          v
  catalog_dev.gold              -- dimensões conformadas + fatos
```

## Convenção de nomenclatura

- Catálogo: `catalog_<ambiente>`; neste exemplo, `catalog_dev`.
- Sistema de origem: `siscad`, `sisguias`, `sisrede`.
- Camada: `<sistema>_bronze`, `<sistema>_silver`; a Gold é compartilhada.
- Tabelas: `snake_case`, com prefixos `dim_` e `fct_` na Gold.
- Chaves técnicas: `<entidade>_sk` (`BIGINT`), geradas por identidade ou outra estratégia de chave substituta.
- Chaves de negócio: identificadores provenientes do sistema de origem, preservados como colunas explícitas.

## Mapa de schemas e caminhos

| Schema | Localização gerenciada |
|---|---|
| `catalog_dev.siscad_bronze` | `abfss://dev@sthealthdev.dfs.core.windows.net/siscad/bronze/` |
| `catalog_dev.siscad_silver` | `abfss://dev@sthealthdev.dfs.core.windows.net/siscad/silver/` |
| `catalog_dev.sisguias_bronze` | `abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/bronze/` |
| `catalog_dev.sisguias_silver` | `abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/silver/` |
| `catalog_dev.sisrede_bronze` | `abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/bronze/` |
| `catalog_dev.sisrede_silver` | `abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/silver/` |
| `catalog_dev.gold` | `abfss://dev@sthealthdev.dfs.core.windows.net/gold/` |

Cada schema possui um local gerenciado próprio, organizado por sistema e camada. Essa separação facilita a aplicação de permissões, políticas de retenção, auditoria e manutenção sem misturar dados de origens diferentes. O diretório raiz `dev` representa o ambiente; em outros ambientes, o segmento pode ser alterado para `hml` ou `prd` por parametrização.

## Contratos por camada

Os arquivos sintéticos da pasta `data/` representam quatro fontes lógicas. `beneficiarios.csv` alimenta o SISCAD; `atendimentos.csv` e `sinistros.csv` representam eventos do SISGUIAS; e `prestadores.csv` representa o SISREDE. No ambiente Azure, esses arquivos devem ser publicados na landing zone antes da execução do Bronze.

Quando a origem é um banco de dados, a extração JDBC deve ser tratada como responsabilidade da ingestão. Azure Data Factory, um job do Databricks ou outra ferramenta de integração pode executar a leitura incremental e gravar os arquivos na landing zone. O notebook Bronze não precisa conhecer a senha, o driver ou a topologia do banco; ele recebe o lote publicado e o registra em Delta.

### Bronze

A Bronze mantém o dado de origem em Delta e adiciona `dt_ingestao`, `source_file`, `source_system`, `ingestion_run_id` e `_rescued_data`. O `COPY INTO` é usado neste projeto para cargas incrementais por arquivo, pois mantém o controle dos arquivos já processados e permite reprocessamentos controlados. Ele não substitui a extração JDBC; representa a fronteira entre a landing zone e a camada Bronze.

### Silver

A Silver converte tipos, normaliza textos e datas, valida chaves obrigatórias e deduplica por chave de negócio. A seleção do registro vencedor usa `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY event_timestamp DESC, source_file DESC, ingestion_run_id DESC)`, evitando que o resultado dependa da ordem acidental de chegada dos arquivos.

### Gold

A Gold contém dimensões conformadas e fatos com granularidade documentada. `dim_beneficiario` usa SCD Tipo 2: alterações cadastrais encerram a versão corrente e inserem uma nova versão, preservando o histórico.

## SCD Tipo 2

A chave natural `id_beneficiario` identifica a entidade; `beneficiario_sk` identifica a versão. O intervalo é semiaberto: `valid_from <= data_referencia` e `data_referencia < valid_to`. A linha corrente possui `valid_to = TIMESTAMP '9999-12-31 23:59:59'` e `is_current = true`.

O processo deve:

1. Comparar o hash dos atributos historizados entre Silver e dimensão corrente.
2. Encerrar versões alteradas com `valid_to` igual ao timestamp da mudança.
3. Inserir a nova versão com `is_current = true`.
4. Inserir novos beneficiários.
5. Garantir idempotência usando o identificador da execução e a chave natural.

## Governança e segurança

- Aplicar o princípio do menor privilégio em catálogo, schemas, volumes e tabelas.
- Classificar e mascarar dados pessoais conforme políticas internas e LGPD.
- Separar identidades de execução por ambiente.
- Registrar linhagem, proprietário, SLA e contrato de cada tabela.
- Evitar dados sensíveis em logs e mensagens de erro.

## Operação Delta

`OPTIMIZE` reduz a fragmentação causada por arquivos pequenos; `ZORDER BY` melhora filtros seletivos em tabelas Delta compatíveis. Em versões recentes, `CLUSTER BY` pode ser preferível quando a estratégia de agrupamento automático for adotada. `VACUUM` deve respeitar a janela de retenção e os requisitos de histórico e recuperação.

Exemplo operacional:

```sql
OPTIMIZE catalog_dev.gold.dim_beneficiario ZORDER BY (id_beneficiario, is_current);
VACUUM catalog_dev.gold.dim_beneficiario RETAIN 168 HOURS;
```
