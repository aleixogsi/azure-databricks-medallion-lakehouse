# Azure Databricks Medallion Lakehouse

Este repositório apresenta uma arquitetura de lakehouse para dados de Saúde usando Azure Databricks, Unity Catalog, ADLS Gen2 e o padrão Medallion. O objetivo é tornar visíveis as decisões de engenharia por trás de uma plataforma de dados organizada, governável e preparada para crescer.

O projeto foi construído como portfólio técnico: os scripts representam uma implementação de referência e os caminhos de armazenamento são exemplos parametrizáveis, não uma configuração pronta para produção.

## Objetivos

- Separar ingestão e processamento por sistema de origem e camada.
- Preservar rastreabilidade e metadados de ingestão na camada Bronze.
- Aplicar qualidade, normalização e deduplicação determinística na Silver.
- Disponibilizar um modelo dimensional integrado na Gold.
- Explicitar práticas de governança, segurança e otimização Delta.

## Arquitetura de dados

| Camada | Responsabilidade | Schemas |
|---|---|---|
| Bronze | Ingestão auditável e reprocessável, preservando o dado original | `siscad_bronze`, `sisguias_bronze`, `sisrede_bronze` |
| Silver | Tipagem, padronização, qualidade e deduplicação | `siscad_silver`, `sisguias_silver`, `sisrede_silver` |
| Gold | Dimensões conformadas e fatos para consumo analítico | `gold` |

Catálogo do ambiente: `catalog_dev`.

## Localizações ADLS Gen2

- Base: `abfss://dev@sthealthdev.dfs.core.windows.net/`
- SISCAD Bronze: `abfss://dev@sthealthdev.dfs.core.windows.net/siscad/bronze/`
- SISCAD Silver: `abfss://dev@sthealthdev.dfs.core.windows.net/siscad/silver/`
- SISGUIAS Bronze: `abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/bronze/`
- SISGUIAS Silver: `abfss://dev@sthealthdev.dfs.core.windows.net/sisguias/silver/`
- SISREDE Bronze: `abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/bronze/`
- SISREDE Silver: `abfss://dev@sthealthdev.dfs.core.windows.net/sisrede/silver/`
- Gold: `abfss://dev@sthealthdev.dfs.core.windows.net/gold/`

Cada schema possui uma localização gerenciada própria, separada por sistema e camada. Os caminhos são exemplos parametrizáveis por ambiente. Em ambientes reais, credenciais, segredos e permissões devem ser gerenciados pelo Azure Key Vault, pelo Unity Catalog e por grupos de acesso. Nenhuma credencial deve ser versionada neste repositório.

## Dados sintéticos

A pasta `data/` contém quatro CSVs sintéticos para reproduzir a entrada do processo sem utilizar dados reais de Saúde:

- `beneficiarios.csv`: cadastro e histórico cadastral de beneficiários.
- `prestadores.csv`: rede credenciada e especialidades.
- `atendimentos.csv`: eventos assistenciais na granularidade do atendimento.
- `sinistros.csv`: despesas assistenciais e seus status de processamento.

Os arquivos incluem situações intencionalmente inválidas, como duplicidades, campos obrigatórios ausentes, datas fora do padrão, valores negativos e referências sem correspondência. Esses casos permitem observar o papel da Silver e das verificações de qualidade.

Para executar no Databricks, publique os quatro arquivos em uma landing zone acessível pelo cluster, por exemplo em `abfss://dev@sthealthdev.dfs.core.windows.net/landing/health/`. A variável `${landing_path}` usada no notebook Bronze deve apontar para esse diretório. A pasta local não é lida diretamente pelo Unity Catalog; ela funciona como fonte de demonstração e deve ser copiada para o armazenamento de entrada por um processo de publicação controlado.

Em uma arquitetura corporativa, a origem pode ser um banco operacional conectado por JDBC. Nesse caso, uma ferramenta de ingestão, como Azure Data Factory ou um job do Databricks, extrai os dados para a landing zone. O notebook Bronze começa a partir desse ponto e usa `COPY INTO` para carregar os arquivos de forma incremental e idempotente. Essa separação evita acoplar a transformação a credenciais do banco e permite reprocessar o mesmo lote a partir do dado recebido.

### Fluxo de ingestão e processamento

```text
Banco operacional
      |
      | JDBC via ADF, Job ou ferramenta de ingestão
      v
ADLS Gen2 - Landing Zone
      |
      | COPY INTO
      v
Delta Bronze
      |
      v
Silver -> Gold
```

O JDBC representa a conexão com a fonte operacional. A landing zone funciona como ponto de desacoplamento e permite controlar o lote recebido antes do processamento. A Bronze registra esse lote em Delta com os metadados de auditoria; Silver e Gold concentram, respectivamente, a qualidade dos dados e o consumo analítico.

## Como ler o projeto

1. Comece por `notebooks/00_setup_unity_catalog.sql` para entender a organização do catálogo e dos schemas.
2. Publique os arquivos de `data/` na landing zone ou substitua `${landing_path}` por um caminho de teste acessível pelo workspace.
3. Consulte `notebooks/01_siscad_ingestion_bronze.sql` para ver como `COPY INTO` carrega os arquivos publicados na landing zone e preserva o contexto operacional da origem.
4. Em `notebooks/02_siscad_cleansing_silver.sql`, observe a padronização, a validação e a deduplicação determinística.
5. Finalize em `notebooks/03_dw_dimensional_gold.sql`, onde o histórico SCD Tipo 2 e o modelo dimensional são apresentados.

Os scripts são idempotentes onde aplicável e usam `CREATE ... IF NOT EXISTS`, `MERGE` ou `CREATE OR REPLACE` conforme a responsabilidade de cada tabela. Em um ambiente corporativo, esses passos seriam executados por pipelines orquestrados, com parâmetros por ambiente e controles de qualidade.

## Qualidade e operação

As tabelas Silver rejeitam ou sinalizam registros inválidos, preservam a chave de negócio e usam `ROW_NUMBER()` com ordenação determinística para deduplicação. A dimensão de beneficiário implementa SCD Tipo 2 com `valid_from`, `valid_to` e `is_current`, permitindo consultar o cadastro como ele era no momento de um atendimento.

Para levar este modelo à produção, ainda devem ser definidos orquestração, testes automatizados, observabilidade, contratos de dados, CI/CD, políticas de retenção e classificação de dados pessoais e sensíveis. Esses pontos fazem parte da arquitetura operacional, não são resolvidos apenas pelo SQL.

Consulte [docs/architecture.md](docs/architecture.md) para o desenho detalhado.
