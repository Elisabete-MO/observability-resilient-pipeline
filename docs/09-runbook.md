# Runbook de Operação e Diagnóstico

Este runbook serve como guia prático de solução de problemas e roteiro técnico durante a defesa do projeto.

---

## 1. Verificação Rápida de Saúde dos Serviços

```bash
# 1. Conferir status de todos os containers
docker compose ps

# 2. Testar readiness da API Producer
curl -i http://localhost:8000/ready

# 3. Conferir targets UP no Prometheus
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"'

# 4. Verificar se o tópico Kafka e partições estão saudáveis
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-events
```

---

## 2. Diagnóstico: Se o Consumer Lag Aumentar (`HighConsumerLag`)

1. **Identificar o gargalo no Grafana (`http://localhost:3000`):**
   - Comparar o painel **Taxa de Ingestão** com a **Taxa de Consumo**. Se a ingestão for significativamente maior que o consumo, o consumidor não está dando conta da taxa de chegada.
   - Analisar o painel **Tempo de Processamento do Consumer**: se a latência p99 estiver próxima do teto (ex.: 200ms), cada worker processa no máximo ~5 a 10 msg/s.
2. **Consultar o lag via linha de comando:**
   ```bash
   curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag{consumergroup=%22events-processor%22}%3E=0)" | grep -o '"value":\[[^]]*\]'
   ```
3. **Ação corretiva (Escala Horizontal):**
   ```bash
   # Escalar para 3 consumidores (aproveitando as 3 partições do tópico)
   docker compose up -d --scale consumer=3 consumer
   ```
4. **Verificar rebalanceamento no Kafka:**
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group events-processor
   ```
   *Resultado esperado:* Cada partição (0, 1 e 2) atribuída a um `client_id` distinto, e o lag drenando para 0.

---

## 3. Diagnóstico: Se a Taxa de Erros Aumentar (`HighErrorRate`)

1. **Acessar painel de erros no Grafana:**
   - Conferir o painel **Erros de Processamento (%)**. O limiar de alerta crítico é > 5% em janela de 2 minutos.
2. **Inspecionar os logs do Consumer para identificar a causa raiz:**
   ```bash
   docker compose logs consumer --tail=50 | grep -E "WARNING|ERROR"
   ```
   - Se os logs indicarem `Erro simulado tratado`: verificar a variável `PROCESSING_ERROR_RATE`.
   - Se indicarem `Evento invalido tratado e encaminhado para DLQ`: a causa é violação de contrato (poison pill).
3. **Auditar mensagens isoladas na Dead Letter Queue (`raw-events-dlq`):**
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic raw-events-dlq \
     --from-beginning \
     --max-messages 5
   ```
4. **Normalizar o ambiente:**
   ```bash
   PROCESSING_ERROR_RATE=0 docker compose up -d consumer
   ```

---

## 4. Diagnóstico: Se o Dashboard do Grafana Estiver Vazio

1. **Verificar se a fonte de dados Prometheus está conectada:**
   - Acessar `http://localhost:3000/datasources` e clicar em **Test**.
2. **Verificar coleta dos endpoints de métricas:**
   - Producer: `curl -s http://localhost:8000/metrics | head -n 10`
   - Prometheus Targets: `http://localhost:9090/targets`
3. **Verificar janela temporal no Grafana:**
   - Garantir que o seletor no canto superior direito esteja em `Last 15 minutes` ou `Last 30 minutes` e o auto-refresh em `5s`.
