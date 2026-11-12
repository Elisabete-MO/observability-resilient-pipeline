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

echo "1. Garantindo apenas 1 réplica do consumer ativa e sem erros simulados..."
PROCESSING_ERROR_RATE="0" docker compose up -d --no-deps --scale consumer=1 --force-recreate consumer

echo "2. Aguardando serviços estabilizarem (5s)..."
sleep 5

echo "3. Injetando carga de 60 req/s por 35 segundos (~2100 eventos)..."
echo "   (Como 1 consumer processa no máximo ~20 msg/s, o lag acumulado"
echo "    ultrapassa 500 e permanece acima do limiar por mais de 30s)"
if ! python3 benchmarks/load_test.py --rate 60 --duration 35 --workers 40; then
    echo "AVISO: load_test retornou código não zero; o lag será verificado mesmo assim."
fi

echo ""
echo "4. Carga concluída. Verificando lag atual no Prometheus..."
sleep 5
curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag{consumergroup=%22events-processor%22}%3E=0)" | grep -o '"value":\[[^]]*\]' || true

echo ""
echo "5. Aguardando o alerta HighConsumerLag completar a janela de 30s..."
echo "   O lag foi projetado para ficar acima de 500 por >30s mesmo com"
echo "   o consumer no limite otimista de 20 msg/s."
echo "   Verifique no Grafana: http://localhost:3000/d/resilient-pipeline"
echo "   Verifique no Alertmanager: http://localhost:9093"
sleep 35

echo ""
echo "6. Aguardando o alerta HighConsumerLag entrar em FIRING (até 60s)..."
FIRING=0
for _ in {1..12}; do
    ALERTS="$(curl -s "http://localhost:9090/api/v1/alerts" || true)"
    if printf '%s' "${ALERTS}" | grep -q '"alertname":"HighConsumerLag"' && \
       printf '%s' "${ALERTS}" | grep -q '"state":"firing"'; then
        echo "   HighConsumerLag em FIRING."
        FIRING=1
        break
    fi
    sleep 5
done
if [ "${FIRING}" != "1" ]; then
    echo "AVISO: HighConsumerLag não entrou em FIRING em 60s; verifique o lag."
fi

echo ""
echo "7. Escalando para 3 consumers para consumir as 3 partições em paralelo..."
docker compose up -d --no-deps --scale consumer=3 consumer

echo ""
echo "8. Acompanhando a drenagem do lag..."
for i in {1..10}; do
    echo -n "   Lag atual: "
    curl -s "http://localhost:9090/api/v1/query?query=sum(kafka_consumergroup_lag{consumergroup=%22events-processor%22}%3E=0)" | grep -o '"value":\[[^]]*\]' || echo "N/A"
    sleep 3
done

echo ""
echo "9. Aguardando a resolução do alerta HighConsumerLag (até 120s)..."
RESOLVED=0
for _ in {1..24}; do
    ALERTS="$(curl -s "http://localhost:9090/api/v1/alerts" || true)"
    if ! printf '%s' "${ALERTS}" | grep -q '"alertname":"HighConsumerLag"'; then
        echo "   HighConsumerLag resolvido (nenhum alerta ativo)."
        RESOLVED=1
        break
    fi
    sleep 5
done
if [ "${RESOLVED}" != "1" ]; then
    echo "AVISO: HighConsumerLag ainda ativo; verifique a drenagem do lag."
fi

echo ""
echo "=========================================================="
echo " Cenário de Lag e Rebalanceamento finalizado com sucesso!"
echo "=========================================================="
