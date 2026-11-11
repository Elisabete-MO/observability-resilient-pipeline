# Plano de Testes Automatizados
 
A suíte é composta por 52 testes automatizados em `tests/`, executáveis diretamente sem dependências externas adicionais:
 
1. **Validação de Contrato (`tests/test_consumer_validation.py` - 7 testes):**
   - Payload válido, regex de tipo de evento, timezone UTC obrigatório, campos extras proibidos (`extra="forbid"`), formato UUIDv4, limite de 64 KiB e tratamento de JSON corrompido.
2. **Durabilidade e Resiliência do Producer (`tests/test_producer_durability.py` - 5 testes):**
   - Confirmação de persistência assíncrona (`asyncio.Future`), erro de broker (503), timeout de broker (504), semáforo de concorrência com fail-fast (503) e fallback de alta vazão sem espera.
3. **Métricas e Benchmarks (`tests/test_benchmark_metrics.py` - 3 testes):**
   - Precisão de percentis (p50, p95, p99), proteção contra lista vazia e validação de argumentos CLI.
4. **Segurança e DLQ (`tests/test_security.py` - 7 testes):**
   - Rate limiter Token Bucket (concessão e bloqueio), roteamento para a DLQ com headers forenses e garantia de que falha, timeout ou ausência do producer da DLQ impedem o commit do offset.
5. **Configuração do Compose (`tests/test_compose_config.py` - 5 testes):**
   - Interpolação de `PROCESSING_ERROR_RATE`/`PROCESSING_MIN_MS`/`PROCESSING_MAX_MS`, recriação e verificação do valor aplicado nos cenários, janela de recuperação de 2 minutos e filtros do Kafka Exporter.
6. **Cenário de Lag (`tests/test_lag_scenario.py` - 5 testes):**
   - Modelo matemático conservador que confirma carga acima da capacidade do consumer, lag > 500 por mais de 30 s e permanência acima do limiar no instante da verificação do alerta; valida também que o cenário inicia com 1 réplica e sem injeção de erros e que aguarda os estados FIRING/resolução.
7. **Métricas de Bytes do Kafka (`tests/test_kafka_bytes_metrics.py` - 7 testes):**
   - JMX remoto habilitado no broker, sidecar JMX Exporter fixado por digest, mapeamento real de `BytesInPerSec`/`BytesOutPerSec`, scrape de 5 s, preservação do Kafka Exporter, painel de tráfego em `Bps` no Grafana (mantendo os 10 painéis anteriores) e documentação dos nomes reais das métricas.
8. **Provisionamento do Viewer (`tests/test_grafana_viewer_provisioning.py` - 13 testes):**
   - Ausência de credenciais conhecidas no Compose/exemplo, interpolação obrigatória sem fallback, placeholders vazios, bloqueio de acesso anônimo e cadastro público, healthcheck do Grafana, provisionador one-shot endurecido, uso da Admin API, preservação da senha existente, verificação de administrador do servidor, ausência de log de senhas, validação de variáveis obrigatórias, função de redação e documentação do procedimento local.

### Comando de Execução
```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

