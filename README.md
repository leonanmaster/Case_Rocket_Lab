# Case Rocket Lab — V-Credit

Projeto de Engenharia de Dados e análise da operação de atendimento da V-Credit, desenvolvido em grupo no programa Rocket Lab 2026. A solução utiliza Databricks Free Edition, PySpark e tabelas Delta, organizadas nas camadas Bronze, Silver e Gold.

O objetivo é reunir bases de diferentes sistemas, tratar inconsistências e preparar indicadores para investigar aumento de demanda, espera, custos e dificuldades de resolução. O período informado no case vai de **janeiro de 2025 a julho de 2026**.

## Dashboard e apresentação

- [Dashboard interativo no Data Studio](https://datastudio.google.com/reporting/c93a46b1-c5f6-4773-94d4-7e6c96562c28/page/p_o1tnlidl8d/edit)
- [Apresentação do projeto no Google Slides](https://docs.google.com/presentation/d/1WRvcJuxFE6zaGwZwGhUNiIzMSX6J6Hx98dejIqZI3HI/edit?slide=id.h2eee4ec9dbede6fc_3_8#slide=id.h2eee4ec9dbede6fc_3_8)

## Arquitetura

```text
CSVs no Volume de entrada
          ↓
Bronze: ingestão das oito bases e registro da data de carga
          ↓
Silver: tratamento, padronização e verificações de qualidade
          ↓
Gold: fato de chamados, dimensões e tabelas de indicadores
          ↓
Consultas, exportações e painéis de análise
```

O catálogo padrão é `case`, com os schemas `bronze`, `silver` e `gold`. A integração das bases é centralizada na `case.gold.ft_chamados`, cuja granularidade esperada é uma linha por chamado. As KPIs consomem essa fato e, quando necessário, suas dimensões.

## Dados de entrada

Os arquivos devem estar em `/Volumes/case/bronze/inputs`, com os nomes esperados pelo notebook de ingestão:

| Arquivo                   | Conteúdo                                                               |
| ------------------------- | ---------------------------------------------------------------------- |
| `chamados.csv`            | Identificação, canal, motivo, resolução, atendente e campos de duração |
| `Chamados_Hora.CSV`       | Datas e horários de abertura, início e finalização                     |
| `clientes.csv`            | Cadastro dos clientes                                                  |
| `custos.csv`              | Custos associados aos chamados                                         |
| `pesquisa_satisfacao.csv` | Pesquisas e notas de atendimento                                       |
| `canais.csv`              | Cadastro dos canais                                                    |
| `base_motivos.csv`        | Motivos, categorias e criticidade                                      |
| `base_atendentes.csv`     | Cadastro e nível dos atendentes                                        |

Os CSVs de origem não estão incluídos no repositório. A grafia dos nomes, incluindo a extensão de `Chamados_Hora.CSV`, deve ser respeitada.

## Organização dos notebooks

### Bronze e Silver

| Notebook                                                               | Responsabilidade                                                                                                                      |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [01_Bronze.ipynb](01_Bronze.ipynb)                                     | Prepara os objetos de entrada, lê os CSVs e grava as tabelas Bronze                                                                   |
| [02_Silver.ipynb](02_Silver.ipynb)                                     | Executa os notebooks Silver em sequência                                                                                              |
| [Silver_chamados.ipynb](Silver_chamados.ipynb)                         | Deduplica chamados e padroniza canal, motivo, resolução e durações                                                                    |
| [Silver_chamados_hora_canais.ipynb](Silver_chamados_hora_canais.ipynb) | Converte horários, trata o ano de abertura/início, classifica a cronologia e verifica a correspondência de clientes; padroniza canais |
| [Silver_custos_atendentes.ipynb](Silver_custos_atendentes.ipynb)       | Trata custos e cadastro dos atendentes                                                                                                |
| [Silver_clientes.ipynb](Silver_clientes.ipynb)                         | Trata nomes, e-mails, região, idade e data de cadastro                                                                                |
| [Silver_motivos.ipynb](Silver_motivos.ipynb)                           | Padroniza categorias, criticidade e a chave textual dos motivos                                                                       |
| [Silver_pesquisa_satisfacao.ipynb](Silver_pesquisa_satisfacao.ipynb)   | Valida notas e IDs, separa quarentena e consolida pesquisas por chamado                                                               |

As saídas Silver incluem as oito bases tratadas, além das tabelas de pesquisas por chamado, quarentena e métricas de qualidade. A base de motivos é publicada como `case.silver.tb_motivos` e a de atendentes como `case.silver.tb_base_atendentes`.

### Gold

| Notebook                                                                   | Tabelas publicadas em `case.gold`                                                                                 |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| [Gold_ft_chamados_v2.ipynb](Gold_ft_chamados_v2.ipynb)                     | `ft_chamados`, `dim_cliente`, `dim_atendente`, `dim_canal`, `dim_motivo`, `dim_tempo` e `bridge_chamado_pesquisa` |
| [Gold_kpi_csat_qualidade.ipynb](Gold_kpi_csat_qualidade.ipynb)             | `kpi_csat`, `kpi_csat_mensal` e `qualidade_dados`                                                                 |
| [Gold_kpi_motivos_clientes.ipynb](Gold_kpi_motivos_clientes.ipynb)         | `kpi_motivos_clientes`                                                                                            |
| [Gold_kpi_demanda_horaria.ipynb](Gold_kpi_demanda_horaria.ipynb)           | `kpi_demanda_horaria`                                                                                             |
| [Gold_kpi_mensal.ipynb](Gold_kpi_mensal.ipynb)                             | `kpi_mensal`                                                                                                      |
| [Gold_kpi_custos_produtividade.ipynb](Gold_kpi_custos_produtividade.ipynb) | `kpi_custos_produtividade` e `kpi_produtividade_atendente`                                                        |
| [03_Gold.ipynb](03_Gold.ipynb)                                             | Orquestra a criação da fato/dimensões e a execução das KPIs                                                       |

## Como executar

### Preparação no Databricks

1. Conectar o repositório a uma pasta Git do workspace ou importar os notebooks, mantendo-os na mesma pasta para preservar as referências relativas.
2. Usar um ambiente com PySpark, Delta e acesso ao Unity Catalog.
3. Garantir permissão para criar e utilizar os objetos do catálogo `case`.
4. Disponibilizar os oito CSVs no Volume de entrada. Na primeira execução, o bloco inicial de `01_Bronze` cria os objetos antes do upload; depois, executar a ingestão.

Os notebooks dependem de recursos do Databricks, como `spark`, `dbutils` e Volumes. Não basta executá-los em um Python local sem configurar esse ambiente.

### Execução manual

Executar nesta ordem:

```text
01_Bronze → 02_Silver → 03_Gold
```

Para testar uma KPI isolada, sua fato e as dimensões utilizadas precisam estar atualizadas e validadas. `kpi_mensal` e `kpi_demanda_horaria` não dependem uma da outra, embora o orquestrador atual as execute em sequência.

Alguns notebooks oferecem widgets para catálogo, schema e outros parâmetros; outros usam caminhos fixos. Para mudar de ambiente, revisar todos os caminhos envolvidos. **Trocar a branch do Git não isola as tabelas:** duas branches com os mesmos destinos podem sobrescrever os mesmos dados no workspace.

### Execução por Job

[job.yml](job.yml) contém a definição do `Job Case`, com tarefas dependentes desde a Bronze até a última KPI. A configuração aponta para a branch `main` deste repositório e prevê execução diária às **03:00:41**, no fuso `America/Sao_Paulo`.

O arquivo descreve o Job; sua presença no Git não confirma que ele está implantado ou ativo no workspace. A configuração de acesso ao repositório, ambiente e agendamento deve ser conferida no Databricks. Os orquestradores `02_Silver` e `03_Gold` servem à execução manual; o YAML lista os notebooks de processamento como tarefas individuais.

## Indicadores e regras de interpretação

### Operação mensal

`kpi_mensal` tem uma linha por **mês de abertura, canal e nível do atendente**. Reúne volume, resolução, tempos médios, custos e cobertura dos registros financeiros.

- Espera média: soma das esperas válidas / quantidade de esperas válidas / 60.
- Atendimento médio: soma das durações válidas / quantidade válida / 60.
- Resolução: chamados com `resolvido = sim` / total de chamados × 100. Resolução ausente é contada separadamente.
- Custo médio por chamado: soma dos custos conhecidos / total de chamados.
- Custo médio dos chamados com custo: soma dos custos conhecidos / quantidade com custo registrado.

As duas médias financeiras usam denominadores diferentes. Custo ausente não é estimado. Nesta tabela, grupos sem nenhum custo conhecido têm total e médias financeiras nulos, com contagens de cobertura disponíveis.

### Demanda horária

`kpi_demanda_horaria` tem uma linha por **mês, dia da semana, hora de abertura e canal**. Uma linha de segunda-feira às 10h reúne os chamados das 10h às 10h59 de todas as segundas daquele mês.

Inclui volume, espera média e mediana aproximada, duração média, resolução e percentual de esperas acima de um limite configurável. O padrão é **300 segundos**: somente esperas estritamente maiores que cinco minutos entram no numerador. Esse limite é um corte analítico, não um SLA estabelecido no case.

Domingo recebe 1 e sábado recebe 7. A mediana é calculada com `percentile_approx` e não pode ser combinada por soma ou média entre grupos.

### Horários e qualidade

Os timestamps de `tb_chamados_hora` são a referência para as durações. Nas KPIs mensal e horária, o cálculo exige cronologia consistente, horários necessários preenchidos e ausência de inversões entre os horários disponíveis. Espera zero é válida. Um chamado sem duração válida permanece no volume, acompanhado das contagens que explicam os denominadores.

A Silver substitui o ano 2027 na abertura e no início pelo ano da finalização quando esta está em 2025 ou 2026, preservando timestamps originais e flags. As duas KPIs não fazem novas correções de datas. Os campos `tempo_espera_original` e `tempo_atendimento_original` da fato vêm da Silver tratada, não diretamente dos CSVs.

Início ausente não comprova abandono. Da mesma forma, volumes por canal não demonstram que os mesmos chamados percorreram um funil de atendimento.

### Níveis e custos: diferenças entre tabelas

O diagnóstico da origem encontrou IDs de atendentes associados a URA e chatbot. A `kpi_mensal` mantém o nível do atendente cadastrado; essa associação não comprova atendimento humano em autosserviço.

Já `kpi_custos_produtividade` classifica o nível pelo canal (`Autosservico`, `N1` e `N2`) e usa zero como apoio à soma de custos ausentes. As regras e os rótulos diferem da mensal e devem ser considerados ao comparar as tabelas. A contagem de atendentes nessa visão representa IDs presentes nos registros, não uma escala de trabalho ou capacidade disponível.

## Consumo em BI

As tabelas Gold podem alimentar consultas e painéis. Para ferramentas externas, utilizar exportações compatíveis com o ambiente e controlar o acesso aos dados.

O notebook de custos/produtividade já exporta suas duas tabelas para diretórios em `/Volumes/case/gold/export/`. A escrita CSV do Spark gera um diretório com um arquivo `part-...csv` e arquivos auxiliares; não um arquivo com o nome do diretório. As demais KPIs precisam de exportação própria caso sejam consumidas por CSV.

Nos painéis:

- Recalcular médias com as somas e os respectivos denominadores; não tirar média simples de médias.
- Recalcular percentuais a partir das contagens, sem somar os percentuais das linhas.
- Usar meses completos nos filtros das tabelas mensais; elas não permitem recuperar um intervalo de dias dentro do mês.
- Apresentar custo e satisfação junto de suas coberturas.
- Conferir o fuso dos timestamps antes de interpretar horários de pico.
- Quantidades de clientes distintos em recortes diferentes não são necessariamente somáveis.

## Reexecução e validação

As tabelas são reconstruídas com `overwrite`, evitando acúmulo de linhas por reexecução. Isso não substitui a necessidade de validar chaves e regras de seleção de duplicatas. Colunas de auditoria com timestamp podem mudar a cada carga.

As verificações variam por notebook. As KPIs mensal e horária validam contratos, unicidade, denominadores e reconciliação antes da gravação. Outros notebooks possuem conferências após a publicação ou validações mais restritas. Conferir o resultado das tarefas no Databricks antes de utilizar a carga no painel.

## Trabalho em equipe

Desenvolver em branches individuais e integrar à `main` por Pull Request. Antes da integração, conferir as alterações e executar os notebooks afetados no Databricks. Manter arquivos de origem, credenciais e material de apoio local fora dos commits.
