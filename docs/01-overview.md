# Visão geral

## Problema
Implementar pipeline assíncrono de eventos e observar comportamento sob carga e falhas.

## Dentro do escopo
Producer REST, tópico Kafka `raw-events`, Consumer Group, processamento simulado, `/metrics`, kafka-exporter, Prometheus, Grafana provisionado, alertas, carga/benchmark e diagnóstico.

## Fora do escopo inicial
Frontend próprio, autenticação, banco real, armazenamento de eventos em Prometheus, múltiplos microsserviços de negócio, deploy em nuvem.

## Definição de sucesso
Subida reproduzível, evento ponta a ponta, métricas verificadas, painéis provisionados, dois alertas comprovados e diagnóstico demonstrável.

## Critérios de avaliação do enunciado
Infraestrutura 20%; instrumentação 30%; dashboards 20%; alertas 15%; diagnóstico na defesa 15%.
