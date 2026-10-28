# Plano de benchmark

## Ferramentas
Python + Locust como opção principal; script Python simples também é válido. Nenhum teste está implementado na Fase 0.

## Cenários propostos (ajustar após baseline)
| Cenário | Carga inicial sugerida | Objetivo |
|---|---|---|
| Baseline | 10 req/s por 60 s | Verificar fluxo e métricas |
| Load | 100 req/s por 60 s | Medir throughput e latência |
| Stress | 500 → 1000 req/s | Identificar degradação |
| Spike | pico curto controlado | Observar fila e recuperação |
| Fault injection | erros no processamento | Validar HighErrorRate |
| Consumer lento | capacidade menor que ingestão | Validar HighConsumerLag |

## Medidas
Taxa oferecida vs taxa efetiva, total de respostas, erros HTTP, latência p50/p95/p99, eventos confirmados no Kafka, processamento por segundo, consumer lag e tempo de recuperação.

## Cuidados
- Calibrar o gerador para não confundir saturação do cliente com limite do servidor.
- Registrar CPU, memória, número de workers, partições e configuração.
- Repetir testes e preservar condições para comparações.
- Não usar números de exemplo como resultados medidos.
- Não bombardear endpoints de terceiros; apenas ambiente local/autorizado.
