# Auditoria do Kafka Exporter

## Escopo

Verificação da configuração existente do serviço `kafka-exporter`
(`danielqsj/kafka-exporter:v1.8.0`) no `compose.yaml` e das métricas
efetivamente disponíveis para atender ao requisito **KAFKA-01**
(partições, lag do consumer group e tráfego).

Nenhuma ferramenta nova foi adicionada nesta auditoria.

## Configuração atual

```yaml
command:
  - "--kafka.server=kafka:29092"
  - "--topic.filter=^raw-events$$"
  - "--group.filter=^events-processor$$"
```

O `$$` é o escape do Docker Compose para `$`; em tempo de execução os filtros
viram `^raw-events$` e `^events-processor$`. Os filtros estão corretos para o
tópico e o grupo obrigatórios.

## Métricas verificadas

| Requisito | Métrica do exporter | Situação |
|---|---|---|
| Partições (quantidade) | `kafka_topic_partitions{topic="raw-events"}` | Disponível |
| Partições (detalhe por partição) | `kafka_topic_partition_current_offset{partition,topic}`, `kafka_topic_partition_oldest_offset`, `kafka_topic_partition_leader`, `kafka_topic_partition_replicas`, `kafka_topic_partition_in_sync_replica`, `kafka_topic_partition_under_replicated_partition` | Disponível |
| Lag do consumer group | `kafka_consumergroup_lag{consumergroup="events-processor",topic="raw-events",partition}` | Disponível |
| Offset do consumer group | `kafka_consumergroup_current_offset{consumergroup,topic,partition}` | Disponível |
| Membros do consumer group | `kafka_consumergroup_members{consumergroup}` | Disponível |
| Bytes trafegados | — | **Lacuna** |
| Bytes por broker/tópico | — | **Lacuna** |

### Lacuna de bytes trafegados

A versão `v1.8.0` do `kafka-exporter` não expõe bytes de entrada/saída por
broker ou por tópico. Ela também não expõe métricas de tamanho médio de
mensagem. Portanto, o requisito de "tráfego" não é integralmente atendido pelo
exporter atual.

Alternativas possíveis (não implementadas por restrição de escopo):

- JMX Exporter no broker, coletando `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec`
  e `BytesOutPerSec` (por broker e, com `topic=` quando disponível, por tópico).
- Outro exporter que exponha métricas de bytes do broker/tópico.

É possível derivar **throughput em mensagens** pela diferença de
`kafka_topic_partition_current_offset` ao longo do tempo, mas isso não fornece
bytes trafegados.

## Observações adicionais

- `--topic.filter=^raw-events$` exclui `raw-events-dlq`. Isso é intencional
  para o requisito atual, mas significa que o tráfego da DLQ não aparece nas
  métricas de tópico do exporter.
- `kafka_consumergroup_lag` é aproximado e calculado a partir dos offsets
  confirmados; pode divergir do Gauge nativo do Consumer
  (`kafka_consumer_lag_records`) por definição e por instante de coleta.
- O Consumer continua sendo a fonte de verdade para o lag por partição
  observado em tempo real; o exporter é a fonte para o lag do grupo no
  cluster.

## Como validar

```bash
# Métricas brutas do exporter
curl -s http://localhost:9308/metrics | grep -E 'kafka_(topic_partitions|consumergroup_lag|topic_partition_current_offset)'

# Consulta no Prometheus
curl -s "http://localhost:9090/api/v1/query?query=kafka_topic_partitions%7Btopic%3D%22raw-events%22%7D"
curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag%7Bconsumergroup%3D%22events-processor%22%7D)"
```

## Conclusão

Partições e lag estão cobertos pelo exporter atual. Bytes trafegados não estão
disponíveis e permanecem como lacuna documentada, sem adição automática de
novos serviços.
