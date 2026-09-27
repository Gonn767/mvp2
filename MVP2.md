# MVP: Pipeline de Dados na Nuvem — Débito de Sono e Uso de Celular à Noite

## Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

### Problema

Hábitos de uso de celular antes de dormir (rolar redes sociais, brilho de tela, exposição à luz
azul) são frequentemente apontados como causa de pior qualidade de sono e maior fadiga no dia
seguinte. Este MVP busca **entender quais fatores comportamentais (uso de celular à noite,
cafeína, atividade física, cronotipo, ocupação) mais se associam ao débito de sono e à fadiga
relatada no dia seguinte**, usando um dataset público do Kaggle com 8.500 registros de usuários.

**Nota sobre escopo:** o dataset é uma foto única por usuário (não há série temporal — cada
pessoa aparece uma única vez, com valores que representam seu padrão típico). Por isso, as
perguntas abaixo comparam grupos de usuários entre si, e não a evolução de uma mesma pessoa ao
longo do tempo. Todas as associações discutidas são correlacionais, não causais.

### Perguntas de negócio

1. Mais tempo de celular antes de dormir está associado a maior débito de sono?
2. O uso de filtro de luz azul reduz a fadiga relatada no dia seguinte?
3. O cronotipo (coruja, cotovia ou intermediário) influencia o total de horas dormidas e o
   débito de sono?
4. Consumo de cafeína após as 17h está associado a maior latência para pegar no sono?
5. Qual app usado antes de dormir está mais associado a débito severo de sono?
6. A atividade física ao longo do dia reduz o efeito do uso noturno de celular sobre a fadiga?
7. Certas ocupações (ex.: profissionais de saúde em plantão) apresentam maior prevalência de
   débito severo de sono?

### Por que estas perguntas fazem sentido para Engenharia de Dados

Essas perguntas definiram decisões técnicas concretas do pipeline: padronização das categorias
de app e ocupação em um domínio fechado e validado (perguntas 5 e 7 exigiam agrupamentos
confiáveis); granularidade do fato por usuário, já que não há chave temporal; validação de
intervalo (0–100) para os campos percentuais antes de qualquer análise; e sinalização (não
remoção) de outliers em minutos de celular à noite, relevante para a pergunta 1.

### Fonte de dados e licença

Kaggle — ["Sleep Debt & Screen Time: Late Night Phone Habits"](https://www.kaggle.com/datasets/samartalwar/sleep-debt-and-screen-time-late-night-phone-habits),
por samartalwar. 8.500 registros relacionando uso de app antes de dormir, luz azul e fadiga no
dia seguinte. **[Preencher: licença exata copiada da aba "License" da página do Kaggle]**. Uso
deste trabalho é estritamente acadêmico.

### Estrutura dos dados brutos (colunas do CSV)

| Coluna | Descrição |
|---|---|
| `user_id` | Identificador único do usuário |
| `age` | Idade |
| `gender` | Gênero (Female / Male / Non-Binary) |
| `occupation_type` | Ocupação |
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
| `sleep_debt_category` | Categoria final de débito de sono |

## Carga dos Dados (Etapa 4.2)

Coleta pelo **caso simples** do enunciado: o CSV foi baixado manualmente do Kaggle e enviado por
upload para um Volume do Unity Catalog no Databricks (script: `01_bronze_ingestion.py`). Não há
API pública para este dataset, então não há automação de coleta — apenas leitura do arquivo já
disponível no ambiente de nuvem.

O arquivo `bedtime_screentime_sleep_debt.csv` (969,47 KB) foi confirmado no volume
`/Volumes/sleep_mvp/bronze/raw_files`:

![Arquivo bruto no Volume](screenshots/arquivoraw.png)

Após a leitura pelo notebook Bronze, a tabela `bronze.raw_sleep_screentime` reflete exatamente o
CSV original (sem transformação de conteúdo), com todas as colunas mantidas como texto:

![Amostra da tabela Bronze](screenshots/infobronze.png)

Código de referência: [`01_bronze_ingestion.py`](./01_bronze_ingestion.py).

## Modelagem e Catálogo de Dados (Etapa 4.3)

Modelo escolhido: **Esquema Estrela**, com fato central `fact_sleep_behavior` (granularidade: 1
linha por usuário) e duas dimensões: `dim_user` (perfil demográfico) e `dim_app` (app usado antes
de dormir, com chave substituta `app_key`). Código de referência:
[`03_gold_modeling.py`](./03_gold_modeling.py).

As três camadas (bronze, silver, gold) foram persistidas no catálogo Unity Catalog `sleep_mvp`:

![Schemas no Catalog Explorer](screenshots/camadasleepmvp.png)

Catálogo de Dados (gerado via `DESCRIBE TABLE` em cada tabela Gold):

![Catálogo de dados das tabelas Gold](screenshots/catalogodedados.png)

| Tabela | Campo | Tipo | Descrição | Domínio / Linhagem |
|---|---|---|---|---|
| dim_user | user_id | string | Identificador do usuário | Chave primária, ex.: USR-00001 |
| dim_user | age | int | Idade | 10 a 100 (validado na Silver) |
| dim_user | age_group | string | Faixa etária derivada | 18-25, 26-35, 36-45, 46-55, 56+ |
| dim_user | gender | string | Gênero | Female, Male, Non-Binary |
| dim_user | occupation_type | string | Ocupação | Healthcare/Shift Worker, Student, Remote Tech, Corporate 9-to-5, Freelance/Creative |
| dim_user | chronotype | string | Cronotipo | Intermediate, Night Owl, Morning Lark |
| dim_app | app_key | int | Chave substituta do app | Gerada na camada Gold |
| dim_app | app_name | string | Nome do app original | Vem de `primary_bedtime_app` |
| dim_app | app_category | string | Categoria do app | Rede Social, Rede Social (vídeo curto), Vídeo sob demanda, Streaming de entretenimento, Notícias/Leitura, Mensagens |
| fact_sleep_behavior | user_id | string | Referência a dim_user | FK |
| fact_sleep_behavior | app_key | int | Referência a dim_app | FK |
| fact_sleep_behavior | bedtime_phone_minutes | int | Minutos de celular antes de dormir | ≥ 0, outliers sinalizados |
| fact_sleep_behavior | screen_brightness_pct | int | Brilho da tela | 0 a 100 |
| fact_sleep_behavior | blue_light_filter_active | boolean | Filtro de luz azul ativo | true/false |
| fact_sleep_behavior | caffeine_post_5pm_mg | int | Cafeína após 17h (mg) | ≥ 0 |
| fact_sleep_behavior | physical_activity_min | int | Atividade física (min/dia) | ≥ 0 |
| fact_sleep_behavior | sleep_latency_min | double | Minutos para pegar no sono | ≥ 0 |
| fact_sleep_behavior | total_sleep_hours | double | Total de horas dormidas | 0 a 24 |
| fact_sleep_behavior | deep_sleep_pct | double | % sono profundo | 0 a 100 |
| fact_sleep_behavior | rem_sleep_pct | double | % sono REM | 0 a 100 |
| fact_sleep_behavior | morning_alarm_snoozes | int | Sonecas no despertador | ≥ 0 |
| fact_sleep_behavior | next_day_fatigue_score | double | Fadiga no dia seguinte | escala numérica (observado 1 a 10) |
| fact_sleep_behavior | sleep_debt_category | string | Categoria de débito de sono | Optimal Recovery, Mild Deficit, Moderate Debt, Severe Sleep Debt |

`dim_app` possui apenas 6 linhas (uma por app catalogado):

![Amostra da dim_app](screenshots/modelagemdisplay.png)

## Pipeline de Dados (Etapa 4.4)

Pipeline organizado em notebooks separados, seguindo a Arquitetura Medalhão:

1. `00_config.py` — parâmetros, criação do catálogo/schemas/volume
2. `01_bronze_ingestion.py` — leitura do CSV bruto (Bronze)
3. `02_silver_transformation.py` — tipagem, deduplicação, validação de domínio (Silver)
4. `03_gold_modeling.py` — modelagem em Esquema Estrela (Gold)
5. `04_data_quality.py` — verificação de qualidade
6. `05_analysis.py` — análise final e resposta às perguntas

Principais transformações Silver→Gold: tipagem de todas as colunas (a Bronze mantém tudo como
texto), remoção de duplicatas por `user_id`, validação de domínio de categorias (valores fora do
domínio viram nulo em vez de descartar a linha inteira), validação de intervalo para idade e
percentuais, e duas colunas derivadas (`age_group`, `app_category`). O print da seção anterior
(schemas no Catalog Explorer) já evidencia que as três camadas foram persistidas com sucesso.

## Qualidade de Dados (Etapa 4.5)

![Relatório de qualidade de dados](screenshots/qualidade_dos_dados.png)

**Discussão:**
- **Completude:** 0,0% de nulos em todas as colunas verificadas (idade, gênero, ocupação,
  cronotipo, app, horas de sono, fadiga, categoria de débito) — sobre 8.500 registros. O dataset
  chegou extremamente completo, sem necessidade de imputação.
- **Unicidade:** 0 duplicatas de `user_id`, tanto em `dim_user` quanto em `fact_sleep_behavior` —
  confirma 8.500 usuários únicos, consistente com o volume anunciado pelo Kaggle.
- **Consistência:** nenhum valor de `sleep_debt_category` fora do domínio esperado (Optimal
  Recovery, Mild Deficit, Moderate Debt, Severe Sleep Debt).
- **Acurácia:** 0 linhas com idade fora da faixa 10–100, 0 linhas com `total_sleep_hours` fora de
  0–24, e 0 linhas em que `deep_sleep_pct + rem_sleep_pct` ultrapassa 100% — o dataset é
  internamente consistente nessas regras de negócio.
- **Outliers:** 123 linhas (≈1,4% de 8.500) foram sinalizadas com z-score > 3 em
  `bedtime_phone_minutes`. Essas linhas foram **mantidas** (não removidas), pois representam
  usuários com uso de celular à noite genuinamente extremo (até ~180 minutos observados no
  dataset), não erros de digitação — e são justamente parte do que explica os casos de "Severe
  Sleep Debt" na Pergunta 1.

## Análise de Dados (Etapa 4.5)

### Pergunta 1 — Tempo de celular à noite x categoria de débito de sono

![Pergunta 1](screenshots/perg1.png)

Há um padrão de dose-resposta muito claro: a média de minutos de celular antes de dormir sobe de
forma monotônica com a gravidade do débito de sono — de 33,5 min (Optimal Recovery) para 45,5 min
(Mild Deficit), 64,1 min (Moderate Debt) e 123,6 min (Severe Sleep Debt, quase o dobro do próximo
grupo). Isso é a evidência mais forte do dataset: usuários no grupo de débito severo passam, em
média, quase 4x mais tempo no celular antes de dormir do que os do grupo de recuperação ótima.

### Pergunta 2 — Filtro de luz azul x fadiga no dia seguinte

![Pergunta 2](screenshots/perg2.png)

Quem usa filtro de luz azul relata fadiga ligeiramente menor no dia seguinte (3,58) do que quem
não usa (3,99). A direção do efeito é a esperada (menos luz azul, menos supressão de melatonina,
menos fadiga), mas o tamanho do efeito é modesto (~0,4 ponto). Isso sugere que o filtro de luz
azul ajuda, mas sozinho não é suficiente para neutralizar o impacto do uso de celular à noite —
possivelmente porque não reduz o *tempo* de tela, só sua composição espectral.

### Pergunta 3 — Cronotipo x horas de sono e débito de sono

![Pergunta 3](screenshots/perg3.png)

O cronotipo é um fator forte: corujas ("Night Owl") dormem em média só 5,01h e relatam fadiga de
5,4 — o pior resultado entre os três grupos. Cotovias ("Morning Lark") dormem 7,38h e têm fadiga
de apenas 2,53. Intermediários ficam no meio (6,44h, fadiga 3,48). Isso é coerente com o conceito
de "jetlag social": pessoas com cronotipo noturno tendem a ter horários de sono desalinhados com
compromissos sociais/profissionais do dia seguinte, pagando um preço maior em fadiga.

### Pergunta 4 — Cafeína após as 17h x latência do sono

![Pergunta 4](screenshots/perg4.png)

A correlação encontrada foi de **r ≈ 0,199** — positiva (na direção esperada: mais cafeína à
noite, mais tempo para pegar no sono), mas fraca. Isso indica que a cafeína pós-17h não é o
principal fator explicando quanto tempo alguém demora para dormir neste dataset; outros fatores
(como uso de celular e cronotipo) parecem ter peso maior.

### Pergunta 5 — App usado antes de dormir x débito severo de sono

![Pergunta 5](screenshots/perg5.png)

TikTok/Reels lidera com 10,8% dos seus usuários em débito severo de sono — bem acima de
Instagram/Reddit (7,7%) e YouTube (6,8%). Na outra ponta, News/Reading tem a menor taxa (4,0%).
O padrão sugere que conteúdo de vídeo curto e rolagem infinita (TikTok/Reels) está associado ao
maior risco de débito severo, possivelmente por ser mais difícil de "desligar" no meio, diferente
de conteúdo mais finito como notícias.

### Pergunta 6 — Atividade física x fadiga, controlando por tempo de celular à noite

![Pergunta 6](screenshots/perg6.png)

O fator decisivo aqui é o tempo de celular, não a atividade física: entre quem usa celular menos
de 60min antes de dormir, a fadiga já é baixa independente do nível de atividade (2,43 ativos vs.
2,55 pouco ativos — diferença pequena). Já entre quem usa celular 60min ou mais, a fadiga sobe
muito (5,6 e 5,83) e a atividade física reduz esse valor em apenas ~0,23 ponto. Ou seja, atividade
física ajuda um pouco, mas **não compensa** o efeito do uso prolongado de celular à noite.

### Pergunta 7 — Ocupação x prevalência de débito severo de sono

![Pergunta 7](screenshots/perg7.png)

Profissionais de saúde em plantão (Healthcare / Shift Worker) têm de longe a maior taxa de débito
severo: 18,3% — mais de 2,5x a taxa do segundo colocado, Freelance/Creative (6,9%). Isso é
consistente com a literatura sobre trabalho em turnos: horários de trabalho irregulares
desalinham o ritmo circadiano, o que provavelmente se soma ao uso de celular como forma de
"descompressão" após plantões noturnos.

## Autoavaliação

Todas as 7 perguntas de negócio definidas na Etapa de Objetivo foram respondidas com dados reais
extraídos do ambiente Databricks. O achado mais forte do trabalho foi a relação entre tempo de
celular antes de dormir e a gravidade do débito de sono (Pergunta 1) e entre ocupação/cronotipo e
fadiga (Perguntas 3 e 7) — todos com diferenças grandes e claras entre grupos. Já a relação entre
cafeína e latência do sono (Pergunta 4) mostrou uma associação bem mais fraca do que o esperado
pela literatura, o que é um resultado legítimo e vale ser discutido como limitação dos dados (o
campo não captura horário exato do consumo, só a dose total após as 17h).

**Dificuldades encontradas:** o dataset não possui série temporal (uma linha por usuário), o que
impediu qualquer análise de tendência ao longo do tempo para uma mesma pessoa — todas as
conclusões são comparações entre grupos de usuários diferentes, não antes/depois.

**Trabalhos futuros:** (1) cruzar este dataset com dados de wearables para validar as métricas de
sono auto-relatadas; (2) acompanhar os mesmos usuários por várias semanas para estudar causalidade,
não só associação; (3) investigar se o efeito do TikTok/Reels (Pergunta 5) se mantém controlando
por idade, já que apps de vídeo curto tendem a ser mais usados por usuários mais jovens.
