# Observability & Resilient Pipeline

Pipeline de processamento distribuído orientado a eventos (**EDA**) construído com **Apache Kafka**, instrumentado sob o padrão **RED** (Rate, Errors, Duration) com **Prometheus**, visualização em tempo real via **Grafana**, gestão de incidentes no **Alertmanager** e suíte de benchmarks/testes em **Python**.

> **Status:** Concluído para entrega / defesa, com bytes trafegados coletados via JMX Exporter ([auditoria](docs/12-kafka-exporter-audit.md)).

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

## 2. Inicialização Rápida

### Pré-requisitos
* Docker e Docker Compose instalados.
* Python 3.10+ (opcional, para rodar benchmarks e testes no host).

### Subir todo o ambiente
```bash
docker compose up -d
```

### URLs de Acesso
| Serviço | URL | Descrição / Credenciais |
|---|---|---|
| **Producer API** | `http://localhost:8000` | Ingestão HTTP (`/api/v1/events`, `/health`, `/metrics`) |
| **Prometheus** | `http://localhost:9090` | Coleta de métricas e avaliação de regras (intervalo 5s) |
| **Grafana** | `http://localhost:3000` | Dashboard auto-provisionado (login desabilitado) |
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

### Segurança e Resiliência Implementadas (Defense in Depth)
1. **Proteção contra DoS / OOM (CWE-400):** Middleware no Producer intercepta requisições com `Content-Length > 64 KiB` antes de alocar memória ou fazer parse do JSON, retornando `413 Payload Too Large`.
2. **Dead Letter Queue (DLQ):** Tópico `raw-events-dlq` armazena mensagens com falha irreversível (poison pills) com metadados de diagnóstico (`x-error-reason`, `x-original-offset`, `x-failed-at`).
3. **Segmentação de Redes Docker (Zero Trust):** Divisão isolada entre `messaging-network` (dados de mensageria) e `observability-network` (Prometheus/Grafana), prevenindo movimentação lateral.
4. **Hardening de Containers (CIS Docker Benchmark):** Diretivas `security_opt: [no-new-privileges:true]`, descarte de capabilities com `cap_drop: [ALL]`, e sistema de arquivos raiz somente-leitura (`read_only: true`) com `tmpfs: /tmp` nas aplicações.
5. **Contenção de Armazenamento no Kafka:** Limite estrito de 1 GB por partição (`KAFKA_LOG_RETENTION_BYTES`) prevenindo exaustão de disco no host.
6. **CORS & HTTP Security Headers:** Restrição estrita de origens confiáveis (`CORSMiddleware`) e headers defensivos (`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `X-XSS-Protection`).
7. **Rate Limiting Defensivo:** Algoritmo Token Bucket in-memory configurável via `PRODUCER_RATE_LIMIT_RPS`.
8. **Hardening de Credenciais:** Grafana configurado via variáveis de ambiente com arquivo [.env.example](.env.example) para controle de acesso seguro.
9. **Garantia de Entrega e Anti-Slowloris no Producer:** Confirmação de persistência no Kafka via `asyncio.Future` antes do `202 Accepted`, com semáforo de concorrência (`PRODUCER_MAX_CONCURRENT_PRODUCES`) e timeout estrito de 2s, blindando a API contra perda em memória e contra exaustão de conexões.

### Regras de Alerta (`infra/prometheus/rules/pipeline.yml`)
1. **`HighConsumerLag` (WARNING):** Lag acumulado do grupo `events-processor` > 500 mensagens por mais de 30 segundos.
2. **`HighErrorRate` (CRITICAL):** Taxa de erros de processamento do Consumer > 5% em janela móvel de 2 minutos.

---

## 4. Roteiro para Defesa Prática (15% da Nota)

Foram criados scripts automatizados de um comando para reproduzir cada cenário de falha durante a apresentação:

### Cenário A: Acúmulo de Lag & Rebalanceamento Dinâmico
```bash
./benchmarks/scenarios/simulate_lag.sh
```
* **O que demonstra:** Injeta 60 req/s por 35 s (~2100 eventos) com 1 consumer, mantendo o lag acima de 500 por mais de 30 s mesmo no limite otimista de consumo (20 msg/s); o alerta `HighConsumerLag` transiciona para `FIRING`; o script escala para 3 consumidores (`--scale consumer=3`), as partições são rebalanceadas, o lag drena rapidamente e o alerta se resolve.

### Cenário B: Injeção de Erros & Diagnóstico de Causa Raiz
```bash
./benchmarks/scenarios/simulate_errors.sh
```
* **O que demonstra:** Configura injeção de 20% de erros no processamento via `${PROCESSING_ERROR_RATE}` (`--scale consumer=1 --force-recreate` + validação por `printenv`), envia 60 s de tráfego (~600 eventos) e a taxa supera o limiar de 5%; o alerta `HighErrorRate` entra em estado `FIRING`; restaura a taxa para zero e envia tráfego de recuperação por mais de 2 minutos para limpar a janela móvel e resolver o alerta.

---

## 5. Testes Automatizados e Benchmarks

### Executar Testes Unitários
```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

### Executar Teste de Carga Customizado
```bash
# Teste de carga estável (steady)
python3 benchmarks/load_test.py --scenario load --rate 20 --duration 30

# Teste de estresse progressivo (steps de carga)
python3 benchmarks/load_test.py --scenario stress --rate 10 --duration 30

# Teste de surto repentino (spike)
python3 benchmarks/load_test.py --scenario spike --rate 10 --duration 30
```

---

## 6. Documentação Detalhada
Consulte a pasta [`docs/`](docs/) para especificações técnicas complementares:
* [Requisitos e Critérios de Aceite](docs/03-requirements.md)
* [Contrato de Eventos](docs/04-event-contract.md)
* [Plano de Observabilidade](docs/05-observability.md)
* [Estratégia de Resiliência](docs/06-resilience.md)
* [Checklist de Entrega](docs/10-delivery-checklist.md)
* [Auditoria do Kafka Exporter](docs/12-kafka-exporter-audit.md)



