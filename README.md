# ZelaAí · Pipeline de Dados

## Ficha Técnica do MVP

| Campo | Descrição |
| --- | --- |
| Produto | ZelaAí, plataforma de zeladoria urbana acessível |
| Cidadão e território | Moradores de São Paulo, com foco em pessoas idosas e pessoas com deficiência |
| Problema | Falhas no espaço público, como calçadas danificadas e semáforos inoperantes, são difíceis de reportar e acompanhar. |
| Proposta de valor | O cidadão registra e acompanha a ocorrência. O gestor analisa e aprova com apoio da triagem. O RPA protocola no Portal 156. |
| Arquitetura | API Node.js no Azure App Service, PostgreSQL, triagem GenAI, OCR, UiPath e pipeline Apache Airflow. |
| Fontes | Ocorrências do ZelaAí via REST e, quando disponíveis, recortes normalizados de GeoSampa, IBGE e SP156. A demonstração é integralmente simulada. |
| IA | Triagem de categoria e urgência, com regras de contingência e decisão humana. OCR de comprovantes no backend. O ranking do pipeline usa uma fórmula determinística. |
| URLs | [API em produção](https://zelai-api.azurewebsites.net/health) · [Swagger](https://zelai-api.azurewebsites.net/docs) · [Repositório da API](https://github.com/Rodriguessz/zelai-api) · [Painel de demonstração (HTML)](docs/index.html) |
| Indicadores | Ocorrências e apoios por bairro, taxa de resolução e prioridade distrital. O backend calcula também o tempo médio até o protocolo 156. |
| ODS | ODS 9, infraestrutura resiliente (meta 9.1). ODS 10, inclusão (10.2). ODS 11, mobilidade acessível e espaços públicos inclusivos (11.2 e 11.7). |

## Envio no portal da FIAP

**Global Solution 2026.2 · Big Data Architecture & Data Integration · 4ESPR**

Envio por um integrante da equipe até **19/11/2026, às 23h55**, no portal da disciplina.

1. Informe o [link do repositório público](https://github.com/mediotti/citizentech-big-data).
2. Anexe o [PDF único da entrega](output/pdf/zelai-big-data.pdf).

| Entregável solicitado | Onde encontrar |
| --- | --- |
| 1. Pipeline orquestrado e instruções de execução | [DAG do Airflow](dags/zelai_ocorrencias.py), [código do ETL](zelai_data/) e [passo a passo](#1-pipeline-e-instruções-de-execução). |
| 2. Diagrama de arquitetura e dicionário de dados | [PDF único](output/pdf/zelai-big-data.pdf), páginas 1–4. |
| 3. Evidências do pipeline em execução | [Grafo, logs, dados carregados e captura do painel](docs/evidencias/README.md). O resumo está na página 5 do PDF. |

**Avaliação** · Pipeline funcional, 8 pontos. Arquitetura e documentação, 6 pontos.

## 1. Pipeline e instruções de execução

### Executar o lote de demonstração

Use Python 3.12 em Linux, macOS ou WSL2. O modo padrão cria 240 ocorrências fictícias em seis bairros e três páginas. A amostra usa semente 42, UUIDs determinísticos e datas fixas para testar paginação, regras e reconciliação sem depender de acesso ao Azure. Os números populacionais e de solicitações do cenário também são simulados.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="$PWD"

airflow db migrate
airflow dags test zelai_ocorrencias 2026-11-17T09:00:00+00:00
python -m unittest discover -s tests -v
python -m http.server 8000 --bind 127.0.0.1 --directory data/public
```

Abra [o painel local](http://127.0.0.1:8000/). Também é possível baixar o [HTML de demonstração](docs/index.html) e abri-lo no navegador. Para acionar pelo Airflow, rode `airflow standalone` usando o mesmo ambiente, abra [a UI local](http://127.0.0.1:8080/) e encontre `zelai_ocorrencias`. Habilite a DAG para a agenda diária às 6h de São Paulo. O ambiente standalone serve para avaliação local. Execute os workers com armazenamento compartilhado se a orquestração sair de uma única máquina.

### Integração com o ZelaAí

Configure as variáveis no ambiente de execução do Airflow. Use o gerenciador de segredos da infraestrutura para as credenciais.

| Variável | Uso |
| --- | --- |
| `ZELAI_SOURCE` | `demo` (padrão) ou `api`. |
| `ZELAI_API_URL` | Origem REST. Padrão `https://zelai-api.azurewebsites.net`. Loopback HTTP é aceito para testes locais. |
| `ZELAI_EMAIL` e `ZELAI_SENHA` | Conta autorizada do ZelaAí. A DAG chama `POST /auth/login` e usa o JWT em `GET /ocorrencias`. |
| `ZELAI_TOKEN` | JWT opcional para uma execução manual. Tem precedência sobre e-mail e senha. |
| `ZELAI_ENRICHMENT_DIR` | Diretório opcional com os três CSV de contexto territorial. |
| `ZELAI_POSTGRES_DSN` | Destino opcional, apenas no modo `api`. Aplique a migração `002` do backend e instale `pip install -r requirements-postgres.txt`. Use uma conta com acesso somente à tabela gold (SELECT, INSERT e DELETE) e `sslmode=verify-full` para destinos remotos. |
| `ZELAI_DATA_DIR` | Diretório dos lotes, padrão `data/`. Cada lote contém entradas minimizadas e seu SQLite. |
| `ZELAI_PANEL_PATH` | HTML gerado, padrão `data/public/index.html`. O painel da API deve ser servido em ambiente privado. O HTML versionado em `docs/` contém apenas dados de demonstração. |

A integração PostgreSQL é uma substituição transacional do snapshot em `prioridade_por_bairro`, lido pelo backend em `GET /indicadores` no campo `prioridadeBairros`. Lotes aprovados geram o painel e as tabelas locais. Em produção, conecte a DAG ao PostgreSQL do MVP para ativar a integração. O ranking só considera São Paulo por causa do contexto municipal e da chave de bairro definida na migração.

#### Contexto territorial

Para usar bases públicas, prepare os três CSV em UTF-8 com vírgula e sem colunas extras. Use o mesmo geocódigo distrital nos três arquivos. Preserve no ambiente de dados o período, a URL e a versão de cada extrato.

| Arquivo e cabeçalho | Preparação |
| --- | --- |
| `geosampa.csv` · `bairro,distrito_id` | Valide a correspondência bairro–distrito com a camada de Distritos do [GeoSampa](https://novogeosampa.prefeitura.sp.gov.br/Version). Registre apenas associações inequívocas dos nomes usados pelo ZelaAí. |
| `ibge.csv` · `distrito_id,populacao_idosa` | No [Censo Demográfico 2022](https://www.ibge.gov.br/estatisticas/sociais/populacao/22827-censo-demografico-2022.html?edicao=41852&t=downloads), use um recorte distrital com distribuição etária ou agregue os setores pela chave distrital. Some pessoas de 60 anos ou mais. |
| `sp156.csv` · `distrito_id,solicitacoes` | Conte as solicitações do recorte de zeladoria em uma janela comum de 30 dias. Use o [catálogo SP156](https://apilib.prefeitura.sp.gov.br/store/apis/info?name=SP156&provider=admin&tag=Dados+Abertos&version=v1) e harmonize o distrito antes da agregação. |

A DAG lê os recortes tabulares preparados. Sem correspondência, deixa a população nula e recalcula o score com os componentes disponíveis. A população distrital é contexto do bairro e pode se repetir entre bairros do mesmo distrito.

#### Qualidade e ranking

A ingestão valida o total, o tamanho e a sequência das páginas. A transformação aplica os domínios da API, normaliza bairros e converte o dia para São Paulo. IDs repetidos, registros inválidos e falhas de reconciliação bloqueiam a publicação. A paginação por offset depende de uma lista estável durante a extração. Logs registram contagens e motivos sem armazenar respostas integrais, descrições, CEPs, coordenadas, identificadores de cidadãos ou credenciais.

O score de 0 a 100 combina quatro indicadores. Cada contagem é dividida pelo teto e limitada a 1. A soma ponderada é normalizada pela soma dos pesos disponíveis.

| Componente | Peso | Teto |
| --- | ---: | ---: |
| Ocorrências no ZelaAí | 45 | 50 |
| Apoios no ZelaAí | 25 | 300 |
| Pessoas de 60+ no distrito | 20 | 25.000 |
| Solicitações de zeladoria no SP156 em 30 dias | 10 | 1.000 |

Os pesos e tetos são parâmetros acadêmicos para triagem humana. O score indica prioridade relativa de análise e pode refletir diferenças de acesso aos canais de registro. A taxa de resolução do painel é uma razão de contagens do lote, sem inferência de impacto causal.

### Inspecionar a carga

```sh
find data/lotes -name zelai.sqlite
sqlite3 "./data/lotes/ID_DO_LOTE/zelai.sqlite"
```

```sql
SELECT * FROM controle_lote;
SELECT COUNT(*) FROM fato_ocorrencia;
SELECT SUM(total), SUM(apoios) FROM mart_ocorrencias;
SELECT * FROM prioridade_por_bairro ORDER BY score_prioridade DESC;
```

## 2. Arquitetura e dicionário de dados

O [PDF único da entrega](output/pdf/zelai-big-data.pdf) reúne a documentação exigida.

| Páginas | Conteúdo |
| --- | --- |
| 1 e 2 | Arquitetura em C4. A vista de contexto mostra usuários e sistemas externos. A vista de contêineres mostra o limite do pipeline e os contratos de integração. |
| 3 e 4 | Dicionário de `fato_ocorrencia`, `prioridade_por_bairro`, `mart_ocorrencias` e `controle_lote`, com tipos, chaves e descrições. |
| 5 | Evidências da demonstração no Airflow e da integração local com o MVP. |

## 3. Evidências de execução

| Requisito | Evidência verificável |
| --- | --- |
| DAG no Airflow | [Grafo exportado da execução](docs/evidencias/airflow-dag.svg) com as cinco tarefas concluídas. |
| Logs do pipeline | [Execução de demonstração](docs/evidencias/airflow-demo.txt) com 240 registros em três páginas e [teste de integração local](docs/evidencias/airflow-api.txt) com 106 registros em duas páginas. |
| Dados carregados | [Contagens consultadas no SQLite](docs/evidencias/carga-demo.json) e [ranking retornado por GET /indicadores](docs/evidencias/indicadores-local.json) após a gravação no PostgreSQL local. |
| Resultado para o avaliador | [Captura do painel gerado](docs/evidencias/painel-demo.png) e [demonstração em HTML](docs/index.html). |

Os dois lotes foram executados com dados fictícios. A integração com PostgreSQL foi validada no backend ZelaAí em ambiente local. Consulte o [índice de evidências](docs/evidencias/README.md) para o contexto de cada execução.
