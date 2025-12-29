# -MVP-PUC-Rio- Engenharia de Dados
Aluno: Pablo Hierro Gouvea Calixto 
Graduado em Direito pela FND UFRJ / Pós Graduaçao em Ciencias de Dados Puc-Rio

Análise do processo tributário do municipio do Rio de Janeiro e no Brasil. 

A Tese "Fora da Caixa": O Sistema Tributário como Sensor Social

https://github.com/user-attachments/assets/99785fcc-d098-4985-91b6-db61a8a0c89e



https://github.com/user-attachments/assets/f1f9a843-93fd-49f5-8e55-ce6355920442



O pensamento convencional (a "caixa") enxerga dados tributários como registros contábeis. O pensamento de vanguarda — o seu — enxerga o sistema tributário como o maior sensor de comportamento econômico e social do país.

Nossa tese é: A flutuação na arrecadação e os padrões de fraude não são apenas problemas de compliance; são indicadores preditivos de saúde econômica setorial, desigualdade social e eficiência da gestão pública.
Vamos provar isso.

1. Orquestração do Pipeline de Dados (O MVP)
Para responder perguntas complexas, precisamos de um pipeline robusto. No ecossistema Databricks (Spark/PySpark), isso se traduz na Arquitetura Medallion, que já incorpora suas etapas (Coleta, Modelagem, Carga, Análise) e a garantia de qualidade.

<img width="1024" height="559" alt="Image" src="https://github.com/user-attachments/assets/7dc66dd2-6246-4475-97c2-46b7fab61337" />

O Processo Tributário como Ontologia WebVowl 
<img width="1756" height="1050" alt="Image" src="https://github.com/user-attachments/assets/bb93cb98-c9df-4f4a-87b6-038220c4fa4f" />

📈 Fase 1: Coleta (Camada BRONZE - O "Pântano" de Dados Brutos)
Aqui, o objetivo é a ingestão (Coleta) de dados brutos, sem tratamento.
Fontes de Dados Essenciais:
Públicas (Governo): Portal da Transparência (Arrecadação de Estados/Municípios), dados agregados da Receita Federal (RFB), dados tributários do municipio do Rio de Janeiro.
Setoriais: Dados do CAGED (emprego por setor).
Litígios: Dados públicos do CARF (Conselho Administrativo de Recursos Fiscais) e dos TJs (Tribunais de Justiça) sobre contencioso tributário.
Fontes "Fora da Caixa" (O Diferencial):
Dados Filantrópicos: Relatórios de ONGs e dados de balanço social de empresas (para cruzar com incentivos fiscais).
Ferramental: Ingestão via Databricks Autoloader ou Apache Kafka e Data Bricks  para streaming de notícias.

✨ Fase 2: Modelagem (Camada SILVER - O Refino e a Limpeza)
Aqui ocorre a Modelagem e a Limpeza (Apuração de qualidade). O PySpark é o protagonista.
Limpeza (Data Quality):
Tratamento de nulos (ex: municípios sem arrecadação reportada).
Padronização de chaves (ex: CNPJs/CPFs hasheados para anonimização, nomes de municípios).
Detecção de outliers (ex: uma PME com arrecadação de imposto de multinacional).
Enriquecimento (O "Pulo do Gato"):
Georreferenciamento: Cruzar dados de arrecadação de ISS (Imposto Sobre Serviços) com a localização de empresas e o IDH (Índice de Desenvolvimento Humano) do bairro/município e IOF (Imposto sobre Operaçoes Financeiras).
Classificação (NLP): Usar Spark NLP para classificar o tipo de litígio tributário (ex: "fraude", "elisão", "erro contábil").
Ferramental: PySpark DataFrames para transformação, Delta Lake para versionamento e garantia de qualidade (constraints).


🏆 Fase 3: Carga (Camada GOLD - O Ativo Monetizável)
Aqui os dados estão limpos, agregados e prontos para a Carga em Data Marts otimizados para Análise.
Visão de Negócio: Não entregamos tabelas; entregamos respostas pré-processadas.
Exemplos de Tabelas GOLD (Ativos):
dm_risco_fiscal_setorial: Um score de risco de fraude por setor e região.
dm_impacto_social_incentivos: Correlação entre incentivos fiscais (ex: Lei Rouanet) e indicadores sociais locais.
dm_simulador_reforma_tributaria: Modelo preditivo do impacto da unificação de impostos (IBS/CBS) por CNAE (Classificação Nacional de Atividades Econômicas).
Ferramental: Spark SQL para criar as views agregadas, armazenadas em formato Delta.


____________________________________________________________________

1. Visão executiva (resumida)
Valor Monetário: identificar sub‑arrecadação e oportunidades de compliance que aumentam receita sem criar novos impostos.
Valor Social: avaliar impacto de políticas tributárias e redistribuição por programas públicos.
Sustentabilidade: incluir métricas ESG e economia circular (impostos verdes, incentivos) e reduzir custo energético do pipeline.
Prova de pensamento fora da caixa: integrar dados fiscais, financeiros, geoespaciais, telecom e satélite para detectar padrão de atividade econômica não declarada.

2. Diagrama do pipeline (visão geral)
     flowchart 
 A[Fontes de Dados]
 A -->|API| B(Coleta - Ingest)
 A -->|FTP / Batch| B
 B --> C(Bronze - Raw storage)
 C --> D(Limpeza & Enriquecimento - Silver)
 D --> E(Modelagem & Agregações - Gold)
 E --> F(ML - Detecção de Fraude / Forecast)
 F --> G(Visualização & Relatórios)
 G --> H(Produtos: Relatórios fiscais, APIs, Dashboards)
 F --> I(Feedback loop de auditoria)
 style C fill:#f9f,stroke:#333,stroke-width:1px

3. Fontes de dados (exemplos e prioridade)
Receita Federal (séries históricas de arrecadação, DCTF, DIPJ, GFIP) — essencial.
Secretarias Estadual/Municipal de Fazenda (ICMS, IPTU, ISS) — prioridade por UF.
Notas Fiscais Eletrônicas (NF‑e / NFC‑e / CT‑e) — alto volume; chave para detecção de fraude.
Dados de empresas (CNPJ, CNAE, balanços, Sintegra, SPED) — correlacionar atividade
econômica. Dados bancários agregados (open banking/fluxos) — quando permitido/anonimizado.
Geo‑dados / Imagens de satélite (uso do solo, atividade industrial) — sinal alternativo de atividade econômica. Dados socioeconômicos (IBGE, RAIS, CAGED) — para análises sociais.

4. Arquitetura técnica (Databricks + Delta Lake)
Ingestão: Databricks Jobs / AutoLoader para streaming/batch das NF‑e, APIs da Receita.
Armazenamento: Delta Lake (Bronze/Silver/Gold) em S3/ADLS.
Processamento: PySpark para ETL; SparkSQL para consultas analíticas e exploração.
ML: MLflow para treino/registro; modelos em PySpark ML e frameworks compatíveis (XGBoost,
LightGBM via Spark integration).
Orquestração: Databricks Workflows / Airflow (opcional).
Governança: Unity Catalog / Data Lineage; políticas de acesso; masking e anonimização.

5. Bronze → Silver → Gold (exemplo de tabelas e transformações)
Bronze: raw_nfe (json raw), raw_arrecadacao (csv), raw_cnpj (xml/csv). Preservar origem,
ts_ingest.
Silver (limpeza): nfe_clean (campos normalizados: cnpj_emit, cnpj_dest, valor_total, itens, cnae),
arrecadacao_monthly (UF, imposto, valor, periodo).
Gold (modelada): tax_revenue_fact (dim_date, dim_uf, dim_imposto, receita), fraud_signals
(entity_id, score, reasons), sector_aggregation (cnae, receita_estimada, discrepancia_pct).

6. Exemplos de transformações e trechos PySpark

# Exemplo: leitura Delta/parquet e limpeza simples
from pyspark.sql.functions import col, to_date, regexp_replace
raw = spark.read.json('/mnt/bronze/nfe/*')
clean = (raw
.withColumn('valor_total', col('total').cast('double'))
.withColumn('data_emissao', to_date(col('dhEmi')))
.withColumn('cnpj_emit', regexp_replace(col('emit.CNPJ'), '[^0-9]', ''))
.filter(col('valor_total') > 0))
(clean.write.format('delta').mode('overwrite').save('/mnt/silver/nfe_clean')
-- Exemplo SparkSQL: agregação mensal por UF
CREATE OR REPLACE TEMP VIEW v_nfe AS
SELECT uf_emit as uf, date_format(data_emissao,'yyyy-MM') as ym,
sum(valor_total) as receita
FROM delta.`/mnt/silver/nfe_clean`
GROUP BY uf, ym;
SELECT uf, ym, receita FROM v_nfe WHERE uf='RJ' ORDER BY ym DESC LIMIT 12;

7. Modelagem ML e detecção de fraude
Features sugeridas: razão receitas declaradas vs estimadas por satélite, variação de emissão
por item, tempo entre emissão e pagamento, frequência de notas por CNPJ, correlação entre
CNAE e itens fiscais.
Modelos: Isolation Forest (anomaly score), XGBoost (classificação supervisada se houver labels),
Autoencoder para séries temporais.
Avaliação: AUC, precision@k, recall@k, custo monetário estimado por falso positivo/negativo.

8. Qualidade dos dados e apuração de erros
Checks automáticos: esquema (schema validation), duplicados, checagem de somas (vouchers),
regras de negocio (ex.: ICMS > 0 quando produto sujeito).
Métrica de qualidade: Data Quality Score = weighted(sum of completeness, accuracy,
timeliness, uniqueness).
Feedback loop: gerar tickets automáticos para auditoria e reingestão após correção.

9. Monetização e impacto social
Monetização direta: serviços de compliance para governos e empresas; APIs de predição de
riscos; licenciamento de dashboards analíticos.
Monetização indireta: redução de evasão tributária que libera caixa para investimentos sociais.
Impacto social/sustentável: direcionar parte das receitas recuperadas para programas ESG;
relatórios públicos que incentivem transparência.

10. Sustentabilidade do pipeline (operacional & ética)
Redução de custo energético: uso de clusters elásticos, spot instances, execução windowed.
Privacidade: anonimização, agregação mínima necessária, consentimento quando aplicável.
Filantropia: dashboards públicos com indicadores sociais; colaborar com universidades/ONGs.

11. Prova de pensamento fora da caixa (exemplos concretos)
Integrar imagens de satélite (NOAA/Sentinel) para estimar atividade industrial e confrontar com
notas fiscais.
Usar dados de consumo de energia elétrica (agregado por área) como sinal de atividade
econômica não declarada.
Aplicar técnicas de NLP em descrições de notas para detectar padrões de subfaturamento.

12. Entregáveis (MVP) — checklist
Pipeline básico (bronze → silver → gold) ingestando NF‑e e arrecadação mensal.
Notebook Databricks com ETL PySpark funcional.
Modelo de detecção de anomalias com avaliação e registro no MLflow.
Dashboard (Power BI / Tableau / Databricks SQL) com 5 KPIs: arrecadação por imposto, top‑10
UF discrepantes, top‑20 empresas com score de risco, economia estimada por compliance, efeito
social simulado.
Relatório técnico (PDF) com arquitetura, decisões de modelagem e justificativas sustentáveis.

13. Próximos passos:
Mapear fontes e conseguir amostras de NF‑e e arrecadação (CSV/JSON/XML).
Implementar AutoLoader para ingestão incremental.
Construir primeira versão do feature store e treinar Isolation Forest.
Preparar apresentação/defesa do projeto com evidências (logs, métricas, visualizações).
Análise Graficas dos dados abertos tributários

Perguntas: Quais foram os bairros que mais arrecadam com o IPTU no Rio? Como seque a tentencia de arrecadaçao do IPTU? É possivel oquestrar um análise epistemica e logaratmica dos dados do IPTU? Como funciona a arrecadaçao de IOF? e a de ISS? 


Gráfico de Bairros que Mais arrecadaram: 

<img width="700" height="600" alt="Image" src="https://github.com/user-attachments/assets/f36a67ad-bf46-4983-ac2b-7ee158a9865c" />

Tendencia de Arrecadação de IPTU: 

<img width="700" height="600" alt="Image" src="https://github.com/user-attachments/assets/f3f47c04-45b6-458b-ab01-8c5a80208a00" />

Escala de Distribuiçao Logaritmica: 

<img width="700" height="600" alt="Image" src="https://github.com/user-attachments/assets/13f41358-3196-4500-89f3-8cbbc4655299" />

Arrecadação de IOF vs Pib per Cabita e o Sensor Social: 

<img width="700" height="600" alt="Image" src="https://github.com/user-attachments/assets/dfb52836-2bbb-4480-b054-4153dcdbc663" />

Receita de imposto de ISS em milhões

<img width="700" height="600" alt="Image" src="https://github.com/user-attachments/assets/a228aa3b-474c-4352-8a38-0e9f6a64e6d8" />

Mapa de Calor ao longo dos meses do ano e a arrecadação sobre serviços. 

<img width="700" height="600" alt="Image" src="https://github.com/user-attachments/assets/7256ea92-2f3d-4865-8c42-dab221d993b7" />

Notas Finais: 

Este projeto propõe uma nova base epistemológica para o Direito e o Processo Tributário brasileiro. Ao integrar a análise de dados no contexto da Reforma Tributária, o trabalho oferece soluções práticas para os desafios cotidianos de empresas e pessoas físicas, garantindo maior conformidade e adequação ao ordenamento jurídico vigente.
