# Auditoria do Kafka Exporter

## Escopo

Verificação da configuração existente do serviço `kafka-exporter`
(`danielqsj/kafka-exporter:v1.8.0`) no `compose.yaml` e das métricas
efetivamente disponíveis para atender ao requisito **KAFKA-01**
(partições, lag do consumer group e tráfego).

A auditoria inicial confirmou a lacuna de bytes. A solução implementada usa um
sidecar **JMX Exporter** (`bitnami/jmx-exporter`) conectado ao JMX remoto do
broker; o Kafka Exporter permanece inalterado para partições e lag.

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
| Bytes trafegados | `kafka_server_brokertopicmetrics_bytesin_total`, `kafka_server_brokertopicmetrics_bytesout_total` (via JMX Exporter) | Disponível |
| Bytes por broker/tópico | Mesmas métricas com `topic="raw-events"` / `topic="raw-events-dlq"`; agregado sem label `topic` | Disponível |

### Solução implementada para bytes trafegados

A versão `v1.8.0` do `kafka-exporter` não expõe bytes de entrada/saída. Para
atender ao requisito sem substituir o exporter existente, foi adicionado um
sidecar JMX Exporter:

- O broker Kafka habilita JMX remoto em `kafka:9999` (`KAFKA_JMX_PORT` /
  `KAFKA_JMX_HOSTNAME`).
- O serviço `jmx-exporter` usa `bitnami/jmx-exporter` (imagem fixada por
  digest), lê `infra/jmx-exporter/kafka-bytes.yml` e expõe `/metrics` na porta
  `5556`.
- O Prometheus coleta `jmx-exporter:5556` no job `kafka-jmx` a cada 5 s.
- As métricas reais são:
  - `kafka_server_brokertopicmetrics_bytesin_total`
  - `kafka_server_brokertopicmetrics_bytesout_total`

Ambas são contadores cumulativos derivados de
`kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` e `BytesOutPerSec`.
O tráfego em bytes/s é calculado com `rate(...)` no Prometheus/Grafana.

O Kafka Exporter continua sendo a fonte de partições, offsets e lag do grupo;
o JMX Exporter responde exclusivamente pelos bytes.

## Observações adicionais

- `--topic.filter=^raw-events$` exclui `raw-events-dlq` das métricas do Kafka
  Exporter. O JMX Exporter não aplica esse filtro; quando o broker expõe a
  dimensão `topic`, o tráfego da DLQ aparece nas métricas de bytes.
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

# Métricas brutas do JMX Exporter (porta interna, sem publish no host)
docker compose exec -T prometheus wget -qO- http://jmx-exporter:5556/metrics \
  | grep -E 'kafka_server_brokertopicmetrics_bytes(in|out)_total'

# Tráfego em bytes/s no Prometheus
curl -s "http://localhost:9090/api/v1/query?query=sum(rate(kafka_server_brokertopicmetrics_bytesin_total%7Bjob%3D%22kafka-jmx%22%7D%5B1m%5D))"
curl -s "http://localhost:9090/api/v1/query?query=sum(rate(kafka_server_brokertopicmetrics_bytesout_total%7Bjob%3D%22kafka-jmx%22%7D%5B1m%5D))"
```

## Conclusão

Partições e lag são cobertos pelo Kafka Exporter existente. Bytes trafegados
passaram a ser coletados pelo JMX Exporter, sem substituir o Kafka Exporter e
sem alterar as regras de alerta.
