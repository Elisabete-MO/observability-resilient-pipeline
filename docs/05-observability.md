# Plano de observabilidade

## RED
- Rate: `rate(http_requests_total[1m])` e `rate(kafka_consumer_processed_events_total[1m])`.
- Errors: taxa de erros por resultado, com denominador consistente.
- Duration: `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))`; repetir para p50/p99.

## Métricas obrigatórias
Producer: `http_requests_total`, `kafka_producer_published_events_total`, `kafka_producer_errors_total`, `http_request_duration_seconds`.
Consumer: `kafka_consumer_processed_events_total{status}`, `kafka_consumer_processing_duration_seconds`, `kafka_consumer_lag_records`.
Cluster: métricas do kafka-exporter de partições, consumer lag e tráfego, conforme suporte efetivo do exporter selecionado.

## Dashboard
Ingestão (req/s), latência p50/p95/p99, lag por partição, processamento vs erros e disponibilidade (`up`).

## Alertas
- `HighConsumerLag`: lag acumulado >500 durante 30 s (WARNING).
- `HighErrorRate`: percentual de erro do Consumer >5% numa janela de 2 min (CRITICAL). Definir janela e comportamento com baixo volume de eventos.

## Atenções técnicas
- Evitar labels por event_id, order_id ou payload.
- Percentis devem vir de Histogram, não de média.
- O lag do Consumer e o lag do exporter podem diferir por definição/tempo de coleta; documentar fonte de verdade.
- Verificar suporte a métricas de bytes trafegados: kafka-exporter sozinho pode não expor todos os bytes exigidos, podendo requerer exporter/JMX adicional.
