# Registro de Decisões Arquiteturais (ADRs)

| ID | Decisão | Estado | Resolução Técnica Adotada |
|---|---|---|---|
| ADR-001 | Kafka KRaft single-node e versão | **Resolvido** | Imagem oficial `apache/kafka:4.2.2` em modo KRaft sem dependência de Zookeeper. |
| ADR-002 | Número de partições de `raw-events` | **Resolvido** | 3 partições criadas automaticamente pelo container `kafka-init`. Tópico `raw-events-dlq` também com 3 partições. |
| ADR-003 | Modelo de publicação HTTP no Producer | **Resolvido** | Retorno imediato `202 Accepted` de forma não-bloqueante; callbacks de entrega atualizam métricas em background. |
| ADR-004 | Estratégia de commits e reprocessamento | **Resolvido** | Commit explícito após sucesso ou após isolamento na Dead Letter Queue (`raw-events-dlq`). |
| ADR-005 | Medição de lag por partição e agregado | **Resolvido** | Coleta pelo `kafka-exporter` (`kafka_consumergroup_lag`) e pelo Gauge nativo `kafka_consumer_lag_records` no Consumer. |
| ADR-006 | Métricas do cluster e broker | **Resolvido** | `kafka-exporter:v1.8.0` integrado ao Prometheus, filtrando `raw-events` e grupo `events-processor`. |
| ADR-007 | Simulação determinística de erros | **Resolvido** | Variável `PROCESSING_ERROR_RATE` parametrizável e script `benchmarks/scenarios/simulate_errors.sh`. |
| ADR-008 | Volume mínimo para alerta de taxa de erros | **Resolvido** | Adicionada cláusula de taxa mínima (`sum(rate(...)) > 0.1`) prevenindo falso alarme em períodos ociosos. |
| ADR-009 | Versões Python, bibliotecas e imagens | **Resolvido** | Imagens `python:3.13-slim`, FastAPI 1.0, confluent-kafka 2.16.1, Prometheus v3.5.0, Grafana 12.1.0 e Alertmanager v0.28.1. |
