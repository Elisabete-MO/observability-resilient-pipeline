# Resiliência — plano

## Falhas a simular
1. Consumer lento: lag cresce.
2. Consumer indisponível: consumo para, mensagens acumulam.
3. Kafka temporariamente indisponível: Producer registra falhas e retorna resposta coerente.
4. Eventos inválidos / erro simulado: taxa de erros aumenta.
5. Reinício de Consumer: validar continuidade e política de offsets.

## Questões de implementação
- Política de retry e backoff com limites.
- Commit de offset após sucesso ou após tratamento explícito de erro.
- Possível reprocessamento; avaliar idempotência.
- Poison messages e eventual DLQ: extensão opcional, não requisito do enunciado.
- Readiness/healthchecks sem confundir processo ativo com pipeline saudável.

Documentar evidências antes e depois da falha.
