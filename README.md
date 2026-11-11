# Observability & Resilient Pipeline

Pipeline de processamento distribuído orientado a eventos (**EDA**) construído com **Apache Kafka**, instrumentado sob o padrão **RED** (Rate, Errors, Duration) com **Prometheus**, visualização em tempo real via **Grafana**, gestão de incidentes no **Alertmanager** e suíte de benchmarks/testes em **Python**.

> **Status (11/11/2026):** documentação revisada para a defesa de 13/11/2026. A suíte unitária tem 52 testes aprovados; o provisionamento do Grafana e os dois alertas foram validados manualmente em Docker. Os scripts de cenário ainda não foram executados de ponta a ponta no host da apresentação. Bytes trafegados são coletados via JMX Exporter ([auditoria](docs/12-kafka-exporter-audit.md)).

---

## 1. Arquitetura da Solução

```text
Simulador / Benchmark (Python)
            │
            ▼ HTTP POST /api/v1/events (202 Accepted)
     [Producer API] ──── /metrics ────┐
            │                         │
            ▼ Publicação assíncrona   │
     [Apache Kafka]                   ├────► [Prometheus] ────► [Grafana: 3000]
      (raw-events)                    │         │
            │                         │         ▼
            ▼                         ├────► [Alertmanager: 9093]
    [Consumer Worker] ── /metrics ────┤
      (events-processor)              │
            │ (poison pills / errors) │
            ▼                         │
     [raw-events-dlq]                 │
            ▲                         │
    [Kafka Exporter] ─── /metrics ────┘
```

---

## 2. Inicialização Rápida (Windows)

### Pré-requisitos
* Docker Desktop com Docker Compose.
* Python 3.10+ apenas para executar o gerador de carga no host. Os testes
  unitários podem ser executados em container, sem Python local.

### Método principal: script versionado
A partir da raiz do repositório, na branch `Development`:

```powershell
.\start-pipeline.ps1 -OpenBrowser
```

O `start-pipeline.ps1` é o script de inicialização versionado do projeto. Ele
verifica Docker/Compose, cria o `.env` a partir de `.env.example` quando
necessário, solicita as senhas com entrada oculta, valida a configuração, sobe
os serviços sem apagar volumes e confere endpoints, targets e o encerramento do
provisionador do `viewer`. Após alterar código, use `.\start-pipeline.ps1 -Build`.

### Alternativa manual
```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
# edite o .env e preencha GF_ADMIN_USER, GF_ADMIN_PASSWORD e GF_VIEWER_PASSWORD
docker compose up -d
```

O Compose não possui fallback de credenciais. Se alguma variável obrigatória
estiver ausente ou vazia, `docker compose up -d` falha com mensagem clara antes
de iniciar o Grafana. O serviço one-shot `grafana-viewer-provisioner` aguarda o
Grafana ficar saudável, provisiona o usuário `viewer` e encerra com código `0`.

### Credenciais do Grafana
* **Administrador:** `GF_ADMIN_USER` / `GF_ADMIN_PASSWORD`. Cada instalação pode
  definir sua própria senha administrativa; não há senha padrão no repositório.
* **Viewer:** usuário `viewer`, criado automaticamente na organização 1 com o
  papel `Viewer` (somente leitura de dashboards; não edita dashboards nem acessa
  a administração). A senha é uma **credencial compartilhada de demonstração**,
  definida localmente em `GF_VIEWER_PASSWORD`; não é uma senha individual por
  integrante e não deve ser reutilizada fora do ambiente local.
* Acesso anônimo e cadastro público permanecem desabilitados.

### Persistência de usuários
Os usuários do Grafana ficam no volume nomeado `grafana_data`
(`/var/lib/grafana`). Em uma instalação nova (volume vazio), o administrador é
criado com `GF_ADMIN_USER`/`GF_ADMIN_PASSWORD` e o `viewer` pelo provisionador
com `GF_VIEWER_PASSWORD`. Em reinicializações com volume existente, as senhas já
gravadas são preservadas: alterar o `.env` **não** redefine a senha do
administrador nem a de um `viewer` já existente. `docker compose down` preserva
os volumes; `docker compose down -v` apaga os dados e está documentado como
procedimento destrutivo separado no guia.

O passo a passo completo, incluindo comandos de verificação, está em
[`docs/guia-execucao-observability.md`](docs/guia-execucao-observability.md).

### URLs de Acesso
| Serviço | URL | Descrição / Credenciais |
|---|---|---|
| **Producer API** | `http://localhost:8000` | Ingestão HTTP (`/api/v1/events`, `/health`, `/metrics`) |
| **Prometheus** | `http://localhost:9090` | Coleta de métricas e avaliação de regras (intervalo 5s) |
| **Grafana** | `http://localhost:3000` | Dashboard auto-provisionado; login obrigatório (admin local ou `viewer`) |
| **Alertmanager** | `http://localhost:9093` | Painel de alertas ativos e silenciamentos |

---

## 3. Conformidade das Métricas (Padrão RED)

### Producer API (`http://localhost:8000/metrics`)
* `http_requests_total{status_code, method}` (Counter): Taxa de requisições recebidas.
* `kafka_producer_published_events_total` (Counter): Mensagens entregues com sucesso ao Kafka.
* `kafka_producer_errors_total` (Counter): Falhas de publicação ou buffer cheio.
* `http_request_duration_seconds` (Histogram): Latência HTTP para percentis p50, p95 e p99.

### Consumer Worker
* `kafka_consumer_processed_events_total{status="success|error"}` (Counter): Eventos processados.
* `kafka_consumer_processing_duration_seconds` (Histogram): Duração do processamento.
* `kafka_consumer_lag_records{partition}` (Gauge): Distância entre o high watermark e o offset atual.
* `kafka_consumer_dlq_events_total{reason}` (Counter): Mensagens não-processáveis isoladas na Dead Letter Queue (incrementado somente após o ACK do broker).

### Cluster Kafka (Kafka Exporter)
* `kafka_topic_partitions{topic="raw-events"}`: quantidade de partições.
* `kafka_topic_partition_current_offset` / `_oldest_offset`: offsets por partição.
* `kafka_consumergroup_lag{consumergroup="events-processor"}`: lag do consumer group.

### Cluster Kafka (JMX Exporter)
* `kafka_server_brokertopicmetrics_bytesin_total`: bytes recebidos pelo broker (contador; use `rate()` para bytes/s).
* `kafka_server_brokertopicmetrics_bytesout_total`: bytes enviados pelo broker (contador; use `rate()` para bytes/s).
* As séries por tópico incluem `topic="raw-events"` / `raw-events-dlq`; o agregado do broker não tem esse label. Detalhes em [docs/12-kafka-exporter-audit.md](docs/12-kafka-exporter-audit.md).

### Controles de Segurança e Resiliência Implementados
1. **Mitigação de payloads grandes (CWE-400):** middleware no Producer intercepta requisições com `Content-Length > 64 KiB` antes de alocar memória ou fazer parse do JSON, retornando `413 Payload Too Large`.
2. **Dead Letter Queue (DLQ):** tópico `raw-events-dlq` armazena mensagens com falha irreversível (poison pills) com metadados de diagnóstico (`x-error-reason`, `x-original-offset`, `x-failed-at`).
3. **Segmentação de redes Docker (isolamento parcial, não Zero Trust):** divisão entre `messaging-network` (dados de mensageria) e `observability-network` (Prometheus/Grafana), reduzindo a exposição entre os planos. Não há autenticação mútua, autorização por identidade nem criptografia entre containers; portanto, isso não caracteriza uma arquitetura Zero Trust.
4. **Hardening básico de containers (não é certificação CIS):** diretivas `security_opt: [no-new-privileges:true]`, descarte de capabilities com `cap_drop: [ALL]` e sistema de arquivos raiz somente-leitura (`read_only: true`) com `tmpfs: /tmp` nas aplicações.
5. **Contenção de armazenamento no Kafka:** limite de 1 GB por partição (`KAFKA_LOG_RETENTION_BYTES`), reduzindo o risco de exaustão de disco no host.
6. **CORS restrito e headers HTTP defensivos:** `CORSMiddleware` limitado às origens locais do Grafana e headers `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY` e `X-XSS-Protection`. A API não possui autenticação de usuários; CORS não substitui controle de acesso.
7. **Rate limiting defensivo:** algoritmo Token Bucket em memória, configurável via `PRODUCER_RATE_LIMIT_RPS` (desabilitado por padrão).
8. **Gestão de credenciais do Grafana:** sem senhas padrão no repositório; credenciais obrigatórias vêm do `.env` local e o Compose falha fechado se estiverem ausentes. O `viewer` é provisionado via Admin API, sem acesso anônimo ou cadastro público.
9. **Confirmação de entrega e backpressure no Producer:** confirmação de persistência no Kafka via `asyncio.Future` antes do `202 Accepted`, com semáforo de concorrência (`PRODUCER_MAX_CONCURRENT_PRODUCES`) e timeout de 2 s. Isso reduz o risco de perda silenciosa e de exaustão de conexões, mas não é uma garantia absoluta de entrega sob qualquer falha.

### Regras de Alerta (`infra/prometheus/rules/pipeline.yml`)
1. **`HighConsumerLag` (WARNING):** Lag acumulado do grupo `events-processor` > 500 mensagens por mais de 30 segundos.
2. **`HighErrorRate` (CRITICAL):** Taxa de erros de processamento do Consumer > 5% em janela móvel de 2 minutos.

---

## 4. Roteiro para Defesa Prática (15% da Nota)

Os scripts de cenário são Bash/POSIX e não são PowerShell nativos. Eles devem
ser executados em um ambiente com `bash`, `python3`, `docker` e `curl`; no
Windows, use WSL/Git Bash ou execute os passos manualmente, como descrito no
[guia de execução](docs/guia-execucao-observability.md).

### Cenário A: Acúmulo de Lag & Rebalanceamento Dinâmico
```bash
./benchmarks/scenarios/simulate_lag.sh
```
* **O que o script faz:** planeja 60 req/s por 35 s, ou seja, **2.100 submissões
  planejadas** com 1 consumer. O gerador pode descartar requisições
  (`skipped_busy`/`skipped_late`); em uma execução anterior foram submetidas
  **680 de 2.100**. Portanto, 2.100 não representa a quantidade de eventos
  efetivamente enviados nem publicados. O script verifica o lag e o alerta
  `HighConsumerLag`, escala para 3 consumidores (`--scale consumer=3`) e
  acompanha a drenagem. O `FIRING` depende da carga realmente submetida e do
  ambiente; se não ocorrer, o script emite aviso.

### Cenário B: Injeção de Erros & Diagnóstico de Causa Raiz
```bash
./benchmarks/scenarios/simulate_errors.sh
```
* **O que o script faz:** configura 20% de erros no processamento via
  `${PROCESSING_ERROR_RATE}` (`--scale consumer=1 --force-recreate` e validação
  por `printenv`), planeja 600 submissões em 60 s e espera que a taxa supere o
  limiar de 5% na janela de 2 min. O número efetivamente submetido pode ser
  menor. Em seguida, restaura a taxa para zero e envia tráfego de recuperação
  por mais de 2 minutos para limpar a janela móvel e resolver o alerta.

> **Estado de validação:** os dois alertas e o provisionamento do Grafana foram
> verificados **manualmente** em Docker. Os scripts `.sh` ainda **não foram
> executados de ponta a ponta** no host da apresentação. Registre as evidências
> na máquina da defesa. Ver [relatório de correções](docs/13-delivery-fixes-report.md).

---

## 5. Testes Automatizados e Benchmarks

### Executar testes unitários (52 testes)
Método reproduzido nesta revisão, sem depender do Python do host:

```powershell
docker run --rm -v "${PWD}:/work" -w /work python:3.13-slim python -m unittest discover -s tests -p "test_*.py" -v
```

Se houver Python 3.10+ instalado no host, o comando equivalente é
`python -m unittest discover -s tests -p "test_*.py" -v`.

**Resultado desta revisão:** 52 testes executados, todos aprovados. A suíte é
unitária: valida módulos, configuração e documentação, mas **não** sobe a pilha
Docker nem executa os cenários de integração de ponta a ponta.

### Executar teste de carga customizado
O gerador exige Python 3.10+ no host e usa apenas a biblioteca padrão.

```bash
# Teste de carga estável (steady)
python benchmarks/load_test.py --scenario load --rate 20 --duration 30

# Teste de estresse progressivo (steps de carga)
python benchmarks/load_test.py --scenario stress --rate 10 --duration 30

# Teste de surto repentino (spike)
python benchmarks/load_test.py --scenario spike --rate 10 --duration 30
```

O relatório distingue `scheduled` (planejadas), `submitted` (efetivamente
submetidas), `skipped_busy`/`skipped_late` (descartadas pelo gerador) e
`confirmed` (respostas confirmadas). Não trate `scheduled` como eventos
publicados.

---

## 6. Documentação Detalhada
Consulte a pasta [`docs/`](docs/) para especificações técnicas complementares:
* [Guia rápido de execução (Windows)](docs/guia-execucao-observability.md)
* [Requisitos e Critérios de Aceite](docs/03-requirements.md)
* [Contrato de Eventos](docs/04-event-contract.md)
* [Plano de Observabilidade](docs/05-observability.md)
* [Estratégia de Resiliência](docs/06-resilience.md)
* [Checklist de Entrega](docs/10-delivery-checklist.md)
* [Auditoria do Kafka Exporter](docs/12-kafka-exporter-audit.md)
* [Provisionamento do usuário Viewer no Grafana](docs/15-grafana-viewer-provisioning-report.md)



