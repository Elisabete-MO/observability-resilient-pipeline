# Plano de Testes Automatizados
 
A suíte é composta por 18 testes automatizados em `tests/`, executáveis diretamente sem dependências externas adicionais:
 
1. **Validação de Contrato (`tests/test_consumer_validation.py` - 7 testes):**
   - Payload válido, regex de tipo de evento, timezone UTC obrigatório, campos extras proibidos (`extra="forbid"`), formato UUIDv4, limite de 64 KiB e tratamento de JSON corrompido.
2. **Durabilidade e Resiliência do Producer (`tests/test_producer_durability.py` - 5 testes):**
   - Confirmação de persistência assíncrona (`asyncio.Future`), erro de broker (503), timeout de broker (504), semáforo de concorrência com fail-fast (503) e fallback de alta vazão sem espera.
3. **Métricas e Benchmarks (`tests/test_benchmark_metrics.py` - 3 testes):**
   - Precisão de percentis (p50, p95, p99), proteção contra lista vazia e validação de argumentos CLI.
4. **Segurança e DLQ (`tests/test_security.py` - 3 testes):**
   - Rate limiter Token Bucket (concessão e bloqueio) e roteamento para a DLQ com headers forenses.

### Comando de Execução
```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

