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
PROCESSING_ERROR_RATE="0.20" docker compose up -d consumer

echo "2. Aguardando reinício do consumer (5s)..."
sleep 5

echo "3. Enviando tráfego de 10 req/s por 25 segundos (250 eventos)..."
python3 benchmarks/load_test.py --rate 10 --duration 25 --workers 10

echo ""
echo "4. Verificando taxa de erros no Prometheus..."
sleep 5
curl -s "http://localhost:9090/api/v1/query?query=sum(rate(kafka_consumer_processed_events_total{status=%22error%22}[2m]))/sum(rate(kafka_consumer_processed_events_total[2m]))" | grep -o '"value":\[[^]]*\]' || true

echo ""
echo "5. Consultando status do alerta HighErrorRate..."
curl -s "http://localhost:9090/api/v1/alerts" | grep -o '"alertname":"HighErrorRate"[^}]*' || true
echo "   Verifique no Alertmanager: http://localhost:9093"
echo "   Verifique no Grafana: http://localhost:3000/d/resilient-pipeline"

echo ""
echo "6. Diagnosticando causa raiz e restaurando PROCESSING_ERROR_RATE=0..."
PROCESSING_ERROR_RATE="0" docker compose up -d consumer

echo "7. Enviando lote de recuperação para normalizar a janela de 2 minutos..."
python3 benchmarks/load_test.py --rate 10 --duration 20 --workers 10

echo ""
echo "8. Verificando resolução do alerta..."
sleep 5
curl -s "http://localhost:9090/api/v1/alerts" | grep -o '"alertname":"HighErrorRate"[^}]*' || echo "Nenhum alerta ativo (resolvido com sucesso)."

echo ""
echo "=========================================================="
echo " Cenário de Injeção de Falhas finalizado com sucesso!"
echo "=========================================================="
