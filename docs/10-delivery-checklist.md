# Checklist de entrega

- [ ] Docker Compose sobe todos os serviços sem falhas.
- [ ] Producer recebe eventos e confirma publicações.
- [ ] Consumer processa eventos no grupo configurado.
- [ ] Endpoints `/metrics` expõem métricas obrigatórias.
- [ ] Kafka Exporter fornece métricas verificadas.
- [ ] Prometheus coleta targets em intervalo de 2–5 s.
- [ ] Grafana provisiona dashboard automaticamente.
- [ ] Dashboard apresenta ingestão, percentis, lag, consumo/erros e saúde.
- [ ] HighConsumerLag dispara e resolve em cenário controlado.
- [ ] HighErrorRate dispara e resolve em cenário controlado.
- [ ] Benchmark produz resultados reproduzíveis.
- [ ] Equipe demonstra diagnóstico de causa raiz.
- [ ] README documenta instalação e execução.
- [ ] Repositório público não contém segredos.
