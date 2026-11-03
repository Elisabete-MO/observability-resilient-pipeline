#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "=========================================================="
echo " [CENÁRIO 1] Demonstração de HighConsumerLag & Auto-Scaling"
echo "=========================================================="
echo ""
echo "Objetivo: Acumular > 500 mensagens de lag, disparar o alerta"
echo "WARNING no Alertmanager e demonstrar recuperação com 3 consumers."
echo ""

cd "${ROOT_DIR}"

echo "1. Garantindo apenas 1 réplica do consumer ativa..."
docker compose up -d --scale consumer=1 consumer

echo "2. Aguardando serviços estabilizarem (5s)..."
sleep 5

echo "3. Injetando carga de 50 req/s por 16 segundos (~800 eventos)..."
echo "   (Como 1 consumer processa ~8 msg/s, o lag acumulado ultrapassará 600)"
python3 benchmarks/load_test.py --rate 50 --duration 16 --workers 30

echo ""
echo "4. Carga concluída. Verificando lag atual no Prometheus..."
sleep 5
curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag{consumergroup=%22events-processor%22}%3E=0)" | grep -o '"value":\[[^]]*\]' || true

echo ""
echo "5. Aguardando o alerta HighConsumerLag completar a janela de 30s..."
echo "   Verifique no Grafana: http://localhost:3000/d/resilient-pipeline"
echo "   Verifique no Alertmanager: http://localhost:9093"
sleep 30

echo ""
echo "6. Consultando status dos alertas no Prometheus..."
curl -s "http://localhost:9090/api/v1/alerts" | grep -o '"alertname":"HighConsumerLag"[^}]*' || true

echo ""
echo "7. Escalando para 3 consumers para consumir as 3 partições em paralelo..."
docker compose up -d --scale consumer=3 consumer

echo ""
echo "8. Acompanhando a drenagem do lag..."
for i in {1..10}; do
    echo -n "   Lag atual: "
    curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag{consumergroup=%22events-processor%22}%3E=0)" | grep -o '"value":\[[^]]*\]' || echo "N/A"
    sleep 3
done

echo ""
echo "=========================================================="
echo " Cenário de Lag e Rebalanceamento finalizado com sucesso!"
echo "=========================================================="
