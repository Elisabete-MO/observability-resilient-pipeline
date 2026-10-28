# Plano de testes

1. Unitários: validação de payload, métricas, regras de processamento.
2. Integração: API → Kafka → Consumer, offsets e falhas.
3. Smoke: containers saudáveis, `/metrics`, targets Prometheus `up`.
4. Dashboards: provisionamento e séries com dados reais.
5. Alertas: disparo e resolução de HighConsumerLag/HighErrorRate.
6. Benchmark: baseline, stress, spike e recuperação.
7. Reprodutibilidade: subir do zero em ambiente limpo.

Guardar comandos e evidências por cenário na fase de implementação.
