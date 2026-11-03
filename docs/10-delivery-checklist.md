# Checklist de entrega

- [x] Docker Compose sobe todos os serviços sem falhas.
- [x] Producer recebe eventos e confirma publicações via `/api/v1/events` e `/events`.
- [x] Consumer processa eventos no grupo configurado (`events-processor`).
- [x] Endpoints `/metrics` expõem métricas obrigatórias (`http_requests_total`, `kafka_consumer_lag_records`, etc.).
- [x] Kafka Exporter fornece métricas verificadas.
- [x] Prometheus coleta targets em intervalo de 5 s com suporte a escala de consumers via `dns_sd_configs`.
- [x] Grafana provisiona dashboard automaticamente (`pipeline.json`).
- [x] Dashboard apresenta ingestão, percentis (p50/p95/p99), lag por partição, consumo/erros e saúde (`up`).
- [x] HighConsumerLag dispara e resolve em cenário controlado (`simulate_lag.sh`).
- [x] HighErrorRate dispara e resolve em cenário controlado (`simulate_errors.sh`).
- [x] Benchmark produz resultados reproduzíveis com percentis calculados.
- [x] Equipe demonstra diagnóstico de causa raiz com roteiro pronto.
- [x] README documenta instalação, execução, métricas e defesa.
- [x] Repositório público não contém segredos nem credenciais sensíveis.
