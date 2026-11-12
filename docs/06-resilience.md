# Resiliência e Tolerância a Falhas

## Mecanismos Implementados
1. **Confirmação Assíncrona de Entrega e Idempotência no Producer:**
   - Habilitado `enable.idempotence: true` e `acks: all` no Producer, garantindo semântica *at-least-once* sem duplicação de mensagens no broker em falhas de rede transitórias.
   - **Confirmação via `asyncio.Future`:** A API aguarda a persistência confirmada do broker Kafka antes de responder `202 Accepted`, eliminando o risco de perda silenciosa em caso de crash do container (*data loss risk*).
   - **Proteção contra DoS / Slowloris:** Semáforo de concorrência (`PRODUCER_MAX_CONCURRENT_PRODUCES`) limitando requisições pendentes com *fail-fast* (`503 Service Unavailable`) e timeout rígido de 2s (`PRODUCER_ACK_TIMEOUT_SECONDS`) retornando `504 Gateway Timeout`.
   - Modo de alta vazão desativável via `PRODUCER_WAIT_FOR_ACK=false` para cenários de benchmark comparativo.
2. **Dead Letter Queue (DLQ):**
   - Tópico `raw-events-dlq` particionado para isolar mensagens envenenadas (*poison pills* ou corrupção de schema).
   - Metadados de diagnóstico gravados em headers (`x-error-reason`, `x-original-partition`, `x-original-offset`, `x-failed-at`).
   - A publicação usa `acks=all` e aguarda o callback de confirmação do broker (`on_delivery`) por até `DLQ_PUBLISH_TIMEOUT_SECONDS` (padrão 5 s). A métrica `kafka_consumer_dlq_events_total` só é incrementada após o ACK.
3. **Commit Manual Síncrono no Consumer:**
   - Desabilitado auto-commit (`enable.auto.commit: false`). O offset só é confirmado após processamento bem-sucedido ou após **confirmação efetiva de entrega na DLQ**. Se a publicação na DLQ falhar ou expirar, o offset não é confirmado e o Consumer encerra o laço sem commit, permitindo reprocessamento em uma nova sessão.
4. **Rebalanceamento Automático e Auto-Scaling:**
   - Suporte a escala dinâmica de consumidores (`docker compose up -d --scale consumer=3`). O Kafka rebalanceia as 3 partições automaticamente entre as instâncias disponíveis no mesmo grupo.
5. **Mitigação de DoS e Limite de Payload:**
   - Interceptação de requisições com `Content-Length > 64 KiB` antes de alocação de memória ou parsing JSON, respondendo `413 Payload Too Large`.
   - Limitador Token Bucket defensivo in-memory configurável via `PRODUCER_RATE_LIMIT_RPS`.

## Cenários de Teste de Falha (Roteiro da Defesa)
- **Lag sob Carga:** Simulado via `benchmarks/scenarios/simulate_lag.sh` (produção acelerada vs consumo lento → alerta WARNING → escala para 3 consumidores → drenagem de lag).
- **Taxa de Erro:** Simulado via `benchmarks/scenarios/simulate_errors.sh` (injeção de 20% de falha via `PROCESSING_ERROR_RATE`, 60 s de tráfego e 1 réplica → alerta CRITICAL → restauração e normalização com 130 s de tráfego limpo para limpar a janela de 2 min).
