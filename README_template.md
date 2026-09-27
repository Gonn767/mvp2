# MVP: Pipeline de Dados na Nuvem — Débito de Sono e Uso de Celular à Noite

> Preencha cada seção abaixo com o resultado real do seu ambiente Databricks (prints, números,
> discussão). Os notebooks referenciados estão neste mesmo repositório.

## Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

### Problema

Hábitos de uso de celular antes de dormir (rolar redes sociais, brilho de tela, exposição à luz
azul) são frequentemente apontados como causa de pior qualidade de sono e maior fadiga no dia
seguinte. Este MVP busca **entender quais fatores comportamentais (uso de celular à noite,
cafeína, atividade física, cronotipo, ocupação) mais se associam ao débito de sono e à fadiga
relatada no dia seguinte**, usando um dataset público do Kaggle com mais de 8.500 registros de
usuários.

**Nota sobre escopo:** o dataset é uma foto única por usuário (não há série temporal — cada
pessoa aparece uma única vez, com valores que representam seu padrão típico). Por isso, as
perguntas abaixo comparam grupos de usuários entre si, e não a evolução de uma mesma pessoa ao
longo do tempo.

### Perguntas de negócio

1. Mais tempo de celular antes de dormir está associado a maior débito de sono?
2. O uso de filtro de luz azul reduz a fadiga relatada no dia seguinte?
3. O cronotipo (coruja, cotovia ou intermediário) influencia o total de horas dormidas e o
   débito de sono?
4. Consumo de cafeína após as 17h está associado a maior latência para pegar no sono?
5. Qual app usado antes de dormir (Instagram/Reddit, TikTok/Reels, YouTube, streaming, notícias,
   mensagens) está mais associado a débito severo de sono?
6. A atividade física ao longo do dia reduz o efeito do uso noturno de celular sobre a fadiga?
7. Certas ocupações (ex.: profissionais de saúde em plantão) apresentam maior prevalência de
   débito severo de sono?

### Por que estas perguntas fazem sentido para Engenharia de Dados

Essas perguntas definem decisões técnicas concretas do pipeline: (a) é necessário padronizar as
categorias de app e de ocupação (textos com barras e parênteses) em um domínio fechado e
validado, para que agrupamentos (perguntas 5 e 7) não fiquem fragmentados por pequenas variações
de grafia; (b) a granularidade do fato precisa ser por usuário (não há chave temporal), então a
tabela fato carrega diretamente as métricas de uma "sessão típica" por pessoa; (c) campos
percentuais (brilho de tela, sono profundo, sono REM) precisam de validação de intervalo (0–100)
antes de qualquer análise, para não distorcer médias; (d) outliers em minutos de celular à noite
precisam ser sinalizados (não descartados), pois podem ser tanto erro de digitação quanto um
caso real de uso extremo — relevante para a pergunta 1.

### Fonte de dados e licença

Kaggle — ["Sleep Debt & Screen Time: Late Night Phone Habits"](https://www.kaggle.com/datasets/samartalwar/sleep-debt-and-screen-time-late-night-phone-habits),
por samartalwar. Mais de 8.500 registros relacionando uso de app antes de dormir, luz azul e
fadiga no dia seguinte. **Confira a licença exata na aba "License" da página do Kaggle e
transcreva aqui** antes de entregar (ex.: CC0, CC-BY, ou licença específica do autor) — isso é
exigido explicitamente pelo enunciado do MVP. Uso deste trabalho é estritamente acadêmico.

### Estrutura dos dados brutos (colunas do CSV)

| Coluna | Descrição |
|---|---|
| `user_id` | Identificador único do usuário |
| `age` | Idade |
| `gender` | Gênero (Female / Male / Non-Binary) |
| `occupation_type` | Ocupação (Student, Remote Tech, Corporate 9-to-5, Healthcare / Shift Worker, Freelance / Creative) |
| `chronotype` | Cronotipo (Morning Lark, Night Owl, Intermediate) |
| `bedtime_phone_minutes` | Minutos de uso de celular antes de dormir |
| `primary_bedtime_app` | App mais usado antes de dormir |
| `screen_brightness_pct` | Brilho médio da tela (%) |
| `blue_light_filter_active` | Se o filtro de luz azul estava ativo (0/1) |
| `caffeine_post_5pm_mg` | Cafeína consumida após as 17h (mg) |
| `physical_activity_min` | Minutos de atividade física no dia |
| `sleep_latency_min` | Minutos para pegar no sono |
| `total_sleep_hours` | Total de horas dormidas |
| `deep_sleep_pct` | % do sono em fase profunda |
| `rem_sleep_pct` | % do sono em fase REM |
| `morning_alarm_snoozes` | Quantas vezes soneca o despertador |
| `next_day_fatigue_score` | Fadiga relatada no dia seguinte (escala numérica) |
| `sleep_debt_category` | Categoria final de débito de sono (Optimal Recovery, Mild Deficit, Moderate Debt, Severe Sleep Debt) |

## Carga dos Dados (Etapa 4.2)

Coleta pelo **caso simples** do enunciado: o CSV foi baixado manualmente do Kaggle e enviado por
upload para um Volume do Unity Catalog no Databricks (script: `01_bronze_ingestion.py`). Não há
API pública para este dataset, então não há automação de coleta — apenas leitura do arquivo já
disponível no ambiente de nuvem.

_→ Adicione aqui: screenshot do upload do arquivo no Volume, e link para o script no GitHub._

## Modelagem e Catálogo de Dados (Etapa 4.3)

Modelo escolhido: **Esquema Estrela**, com fato central `fact_sleep_behavior` (granularidade: 1
linha por usuário) e duas dimensões: `dim_user` (perfil demográfico) e `dim_app` (app usado antes
de dormir, com uma chave substituta `app_key`). Script: `03_gold_modeling.py`.

| Tabela | Campo | Tipo | Descrição | Domínio |
|---|---|---|---|---|
| dim_user | user_id | string | Identificador do usuário | Chave primária, ex.: USR-00001 |
| dim_user | age_group | string | Faixa etária derivada | 18-25, 26-35, 36-45, 46-55, 56+ |
| dim_app | app_key | int | Chave substituta do app | Gerada na camada Gold |
| dim_app | app_category | string | Categoria do app | Rede Social, Vídeo sob demanda, Streaming, Notícias/Leitura, Mensagens |
| fact_sleep_behavior | total_sleep_hours | double | Total de horas dormidas | 0 a 24 |
| fact_sleep_behavior | sleep_debt_category | string | Categoria final de débito de sono | Optimal Recovery, Mild Deficit, Moderate Debt, Severe Sleep Debt |

*(complete as demais linhas com o `DESCRIBE TABLE` gerado pelo notebook 03)*

## Pipeline de Dados (Etapa 4.4)

Pipeline organizado em notebooks separados, seguindo a Arquitetura Medalhão:

1. `00_config.py` — parâmetros, criação do catálogo/schemas/volume
2. `01_bronze_ingestion.py` — leitura do CSV bruto (Bronze)
3. `02_silver_transformation.py` — tipagem, deduplicação, validação de domínio (Silver)
4. `03_gold_modeling.py` — modelagem em Esquema Estrela (Gold)
5. `04_data_quality.py` — verificação de qualidade
6. `05_analysis.py` — análise final e resposta às perguntas

Principais transformações Silver→Gold: tipagem de todas as colunas (a Bronze mantém tudo como
texto), remoção de duplicatas por `user_id`, validação de domínio de categorias (gênero,
ocupação, cronotipo, app, categoria de débito de sono — valores fora do domínio viram nulo em vez
de descartar a linha inteira), validação de intervalo para idade e percentuais, e duas colunas
derivadas (`age_group`, `app_category`).

_→ Adicione aqui: screenshots confirmando que as tabelas Bronze/Silver/Gold foram persistidas no
Unity Catalog, e o link para os notebooks no GitHub (Databricks Repos)._

## Qualidade de Dados (Etapa 4.5)

_→ Cole aqui o resultado do `dq_report` gerado por `04_data_quality.py`, com discussão de cada
problema encontrado (ex.: % de nulos, outliers em minutos de celular) e como foi tratado._

## Análise de Dados (Etapa 4.5)

_→ Cole aqui os resultados de `05_analysis.py`, com discussão de cada pergunta de negócio (o que
o número/gráfico significa no contexto do problema)._

## Autoavaliação

_→ Discuta: quais perguntas você conseguiu responder e quais não, dificuldades encontradas (ex.:
dataset sem série temporal impede análise de tendência ao longo do tempo para um mesmo usuário),
e trabalhos futuros (ex.: cruzar com dados de qualidade do sono medidos por wearables, ou
acompanhar os mesmos usuários ao longo de várias semanas)._
