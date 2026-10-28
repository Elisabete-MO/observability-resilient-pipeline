# Runbook de diagnóstico (esqueleto)

## Se o lag aumentar
1. Verificar taxa de ingestão vs processamento.
2. Verificar número de partições, consumidores ativos e rebalanceamento.
3. Examinar erros, duração de processamento e recursos.
4. Identificar se gargalo está no Producer, broker ou Consumer.

## Se o dashboard ficar vazio
1. Checar `/metrics` no container e conectividade interna.
2. Checar targets `up` no Prometheus.
3. Checar nomes, labels e janelas das consultas PromQL.
4. Verificar relógios e intervalo de scrape.

## Se alerta não disparar
1. Confirmar condição e duração de `for`.
2. Confirmar avaliação de regras no Prometheus.
3. Verificar rota Prometheus → Alertmanager.
4. Checar diferença entre alerta PENDING, FIRING e notificação.

Completar com comandos reais e screenshots após implementação.
