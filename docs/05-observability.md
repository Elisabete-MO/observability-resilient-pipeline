# Plano de observabilidade

## RED
- Rate: `rate(http_requests_total[1m])` e `rate(kafka_consumer_processed_events_total[1m])`.
- Errors: taxa de erros por resultado, com denominador consistente.
- Duration: `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))`; repetir para p50/p99.

## Métricas obrigatórias
Producer: `http_requests_total`, `kafka_producer_published_events_total`, `kafka_producer_errors_total`, `http_request_duration_seconds`.
Consumer: `kafka_consumer_processed_events_total{status}`, `kafka_consumer_processing_duration_seconds`, `kafka_consumer_lag_records`.
Cluster (Kafka Exporter): partições (`kafka_topic_partitions`), lag do grupo (`kafka_consumergroup_lag`) e offsets por partição (`kafka_topic_partition_current_offset`).

Cluster (JMX Exporter): bytes recebidos e enviados pelo broker, expostos como contadores cumulativos:
- `kafka_server_brokertopicmetrics_bytesin_total{job="kafka-jmx"}` — bytes recebidos.
- `kafka_server_brokertopicmetrics_bytesout_total{job="kafka-jmx"}` — bytes enviados.

Quando o broker disponibiliza a dimensão de tópico, as séries incluem `topic="raw-events"` (e `raw-events-dlq`); o agregado do broker não tem esse label. O tráfego em bytes/s é obtido com `rate(...[$__rate_interval])`. Detalhes da integração em [12-kafka-exporter-audit.md](12-kafka-exporter-audit.md).

## Dashboard
Ingestão (req/s), latência p50/p95/p99, lag por partição, processamento vs erros, tráfego Kafka em bytes/s (recebidos/enviados) e disponibilidade (`up`).

## Alertas
- `HighConsumerLag`: lag acumulado >500 durante 30 s (WARNING).
- `HighErrorRate`: percentual de erro do Consumer >5% numa janela de 2 min (CRITICAL). Definir janela e comportamento com baixo volume de eventos.

## Atenções técnicas
- Evitar labels por event_id, order_id ou payload.
- Percentis devem vir de Histogram, não de média.
- O lag do Consumer e o lag do exporter podem diferir por definição/tempo de coleta; documentar fonte de verdade.
- Bytes trafegados: o Kafka Exporter permanece inalterado para partições/lag. A coleta de bytes usa um sidecar JMX Exporter conectado ao JMX remoto do broker (`kafka:9999`), com scrape de 5 s. O JMX Exporter é a única ferramenta adicionada e não substitui o Kafka Exporter.
