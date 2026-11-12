#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "=========================================================="
echo " [CENÁRIO 2] Demonstração de HighErrorRate (Falha Simulada)"
echo "=========================================================="
echo ""
echo "Objetivo: Injetar 20% de erros no processamento do consumer,"
echo "disparar o alerta CRITICAL no Alertmanager e restabelecer a normalidade."
echo ""

cd "${ROOT_DIR}"

echo "1. Configurando Consumer com PROCESSING_ERROR_RATE=0.20 (20% de erro)..."
# O compose interpola a variavel do host; --force-recreate garante a recriacao
# do container mesmo se o hash de configuracao nao for recalculado.
# --scale consumer=1 torna o cenário determinístico mesmo após o cenário de lag.
PROCESSING_ERROR_RATE="0.20" docker compose up -d --no-deps --scale consumer=1 --force-recreate consumer

echo "2. Aguardando reinício do consumer (5s)..."
sleep 5

APPLIED_RATE="$(docker compose exec -T consumer printenv PROCESSING_ERROR_RATE | tr -d '\r')"
if [ "${APPLIED_RATE}" != "0.20" ]; then
    echo "ERRO: PROCESSING_ERROR_RATE esperado=0.20 aplicado=${APPLIED_RATE}"
    exit 1
fi
echo "   Confirmado no container: PROCESSING_ERROR_RATE=${APPLIED_RATE}"

echo "3. Enviando tráfego de 10 req/s por 60 segundos (600 eventos)..."
echo "   (A duração cobre a janela móvel de 2 min mesmo após o cenário de lag.)"
if ! python3 benchmarks/load_test.py --rate 10 --duration 60 --workers 10; then
    echo "AVISO: load_test retornou código não zero; a taxa de erros será verificada mesmo assim."
fi

echo ""
echo "4. Verificando taxa de erros no Prometheus..."
sleep 5
curl -s "http://localhost:9090/api/v1/query?query=sum(rate(kafka_consumer_processed_events_total{status=%22error%22}[2m]))/sum(rate(kafka_consumer_processed_events_total[2m]))" | grep -o '"value":\[[^]]*\]' || true

echo ""
echo "5. Aguardando o alerta HighErrorRate entrar em FIRING (até 120s)..."
FIRING=0
for _ in {1..24}; do
    ALERTS="$(curl -s "http://localhost:9090/api/v1/alerts" || true)"
    if printf '%s' "${ALERTS}" | grep -q '"alertname":"HighErrorRate"' && \
       printf '%s' "${ALERTS}" | grep -q '"state":"firing"'; then
        echo "   HighErrorRate em FIRING."
        FIRING=1
        break
    fi
    sleep 5
done
if [ "${FIRING}" != "1" ]; then
    echo "AVISO: HighErrorRate não entrou em FIRING em 120s; verifique a janela de 2 min."
fi
echo "   Verifique no Alertmanager: http://localhost:9093"
echo "   Verifique no Grafana: http://localhost:3000/d/resilient-pipeline"

echo ""
echo "6. Diagnosticando causa raiz e restaurando PROCESSING_ERROR_RATE=0..."
PROCESSING_ERROR_RATE="0" docker compose up -d --no-deps --scale consumer=1 --force-recreate consumer
sleep 5

APPLIED_RATE="$(docker compose exec -T consumer printenv PROCESSING_ERROR_RATE | tr -d '\r')"
if [ "${APPLIED_RATE}" != "0" ]; then
    echo "ERRO: PROCESSING_ERROR_RATE esperado=0 aplicado=${APPLIED_RATE}"
    exit 1
fi
echo "   Confirmado no container: PROCESSING_ERROR_RATE=${APPLIED_RATE}"

echo "7. Enviando lote de recuperação (>2 min) para limpar a janela do alerta..."
if ! python3 benchmarks/load_test.py --rate 10 --duration 130 --workers 10; then
    echo "AVISO: load_test retornou código não zero; a resolução será verificada mesmo assim."
fi

echo ""
echo "8. Aguardando a resolução do alerta HighErrorRate (até 60s)..."
RESOLVED=0
for _ in {1..12}; do
    ALERTS="$(curl -s "http://localhost:9090/api/v1/alerts" || true)"
    if ! printf '%s' "${ALERTS}" | grep -q '"alertname":"HighErrorRate"'; then
        echo "   HighErrorRate resolvido (nenhum alerta ativo)."
        RESOLVED=1
        break
    fi
    sleep 5
done
if [ "${RESOLVED}" != "1" ]; then
    echo "AVISO: HighErrorRate ainda ativo; verifique a janela de 2 minutos."
fi

echo ""
echo "=========================================================="
echo " Cenário de Injeção de Falhas finalizado com sucesso!"
echo "=========================================================="
