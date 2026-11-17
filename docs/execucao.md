# Executar o pipeline ZelaAí

[Visão geral e entregáveis](../README.md) · [Evidências de execução](evidencias/README.md)

Execute os comandos abaixo a partir da raiz do repositório.

## Executar o lote de demonstração

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

Após o comando do servidor HTTP, abra o endereço `http://127.0.0.1:8000/` no navegador da máquina onde o pipeline rodou. Também é possível abrir diretamente o arquivo gerado em `data/public/index.html`. O repositório contém um snapshot de exemplo em `docs/index.html`.

Para executar pela interface do Airflow, rode `airflow standalone` com as mesmas variáveis de ambiente. Na mesma máquina, abra `http://127.0.0.1:8080/` e localize `zelai_ocorrencias`. Habilite a DAG para a agenda diária às 6h de São Paulo. O ambiente standalone serve para avaliação local. Execute os workers com armazenamento compartilhado se a orquestração sair de uma única máquina.

## Integração com o ZelaAí

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

### Contexto territorial

Para usar bases públicas, prepare os três CSV em UTF-8 com vírgula e sem colunas extras. Use o mesmo geocódigo distrital nos três arquivos. Preserve no ambiente de dados o período, a URL e a versão de cada extrato.

| Arquivo e cabeçalho | Preparação |
| --- | --- |
| `geosampa.csv` · `bairro,distrito_id` | Valide a correspondência bairro–distrito com a camada de Distritos do [GeoSampa](https://novogeosampa.prefeitura.sp.gov.br/Version). Registre apenas associações inequívocas dos nomes usados pelo ZelaAí. |
| `ibge.csv` · `distrito_id,populacao_idosa` | No [Censo Demográfico 2022](https://www.ibge.gov.br/estatisticas/sociais/populacao/22827-censo-demografico-2022.html?edicao=41852&t=downloads), use um recorte distrital com distribuição etária ou agregue os setores pela chave distrital. Some pessoas de 60 anos ou mais. |
| `sp156.csv` · `distrito_id,solicitacoes` | Conte as solicitações do recorte de zeladoria em uma janela comum de 30 dias. Use o [catálogo SP156](https://apilib.prefeitura.sp.gov.br/store/apis/info?name=SP156&provider=admin&tag=Dados+Abertos&version=v1) e harmonize o distrito antes da agregação. |

A DAG lê os recortes tabulares preparados. Sem correspondência, deixa a população nula e recalcula o score com os componentes disponíveis. A população distrital é contexto do bairro e pode se repetir entre bairros do mesmo distrito.

### Qualidade e ranking

A ingestão valida o total, o tamanho e a sequência das páginas. A transformação aplica os domínios da API, normaliza bairros e converte o dia para São Paulo. IDs repetidos, registros inválidos e falhas de reconciliação bloqueiam a publicação. A paginação por offset depende de uma lista estável durante a extração. Logs registram contagens e motivos sem armazenar respostas integrais, descrições, CEPs, coordenadas, identificadores de cidadãos ou credenciais.

O score de 0 a 100 combina quatro indicadores. Cada contagem é dividida pelo teto e limitada a 1. A soma ponderada é normalizada pela soma dos pesos disponíveis.

| Componente | Peso | Teto |
| --- | ---: | ---: |
| Ocorrências no ZelaAí | 45 | 50 |
| Apoios no ZelaAí | 25 | 300 |
| Pessoas de 60+ no distrito | 20 | 25.000 |
| Solicitações de zeladoria no SP156 em 30 dias | 10 | 1.000 |

Os pesos e tetos são parâmetros acadêmicos para triagem humana. O score indica prioridade relativa de análise e pode refletir diferenças de acesso aos canais de registro. A taxa de resolução do painel é uma razão de contagens do lote, sem inferência de impacto causal.

## Inspecionar a carga

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
