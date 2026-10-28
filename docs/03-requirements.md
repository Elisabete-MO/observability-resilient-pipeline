# Matriz de requisitos e critérios de aceite

| ID | Requisito | Critério de aceite |
|---|---|---|
| PROD-01 | POST `/api/v1/events` | Requisição válida recebida e tratada |
| PROD-02 | Publicar em `raw-events` assincronamente | Evento observável no tópico; falha contabilizada |
| PROD-03 | `/metrics` | Endpoint no formato Prometheus |
| PROD-04 | `http_requests_total{status_code,method}` | Counter incrementa por resposta |
| PROD-05 | `kafka_producer_published_events_total` | Counter aumenta após confirmação de publicação |
| PROD-06 | `kafka_producer_errors_total` | Counter aumenta em falha de publicação |
| PROD-07 | `http_request_duration_seconds` | Histogram com buckets para percentis |
| CONS-01 | Consumer Group | Consumo de `raw-events` por grupo configurado |
| CONS-02 | Processamento variável | Duração simulada parametrizável |
| CONS-03 | `/metrics` | Endpoint acessível na porta configurada |
| CONS-04 | `kafka_consumer_processed_events_total{status}` | Counter com `success` e `error` |
| CONS-05 | `kafka_consumer_processing_duration_seconds` | Histogram por mensagem |
| CONS-06 | `kafka_consumer_lag_records` | Gauge com definição de offset documentada e validada |
| KAFKA-01 | Kafka Exporter | Partições, consumer group lag e tráfego coletados |
| PROM-01 | Scrape dos serviços | Todos os targets `UP`; intervalo 2–5 s |
| GRAF-01 | Dashboard provisionado | Ingestão, p50/p95/p99, lag por partição, consumo/erros e saúde |
| ALERT-01 | HighConsumerLag | Warning se lag > 500 por mais de 30 s |
| ALERT-02 | HighErrorRate | Critical se erros > 5% em janela de 2 min |
| TEST-01 | Teste de carga | Crescimento de lag e recuperação observáveis |
| TEST-02 | Diagnóstico | Causa de falha simulada identificada com evidências |

> A condição de erro em janela de 2 min precisa de tráfego suficiente para produzir uma taxa significativa; registrar comportamento em ausência de eventos.
