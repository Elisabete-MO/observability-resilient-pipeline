# Observability Resilient Pipeline — Guia rápido de execução

> Ambiente: Windows (PowerShell) com Docker Desktop. Executar os comandos a partir da pasta raiz do repositório `observability-resilient-pipeline`, na branch `Development`.

## 1. Preparar o projeto

```powershell
git switch Development
git pull origin Development
docker compose config
```

Confirme que o Docker Desktop está aberto e com o engine em execução.

## 2. Subir a aplicação

```powershell
docker compose up -d --build
docker compose ps
```

A primeira inicialização pode demorar mais, especialmente enquanto o Kafka é iniciado e os tópicos são criados. Os serviços `kafka-permissions` e `kafka-init` são tarefas de inicialização: podem aparecer como **Exited (0)** após a conclusão, o que é esperado.

## 3. Endereços para acesso

| Serviço | Endereço | Finalidade |
|---|---|---|
| Producer API | http://localhost:8000 | API de ingestão |
| Producer — documentação interativa | http://localhost:8000/docs | Testar `POST /api/v1/events` |
| Producer — health | http://localhost:8000/health | Verificação básica HTTP |
| Producer — readiness | http://localhost:8000/ready | Verifica prontidão e acesso ao tópico Kafka |
| Producer — métricas | http://localhost:8000/metrics | Métricas em formato Prometheus |
| Prometheus | http://localhost:9090 | Consultas PromQL |
| Prometheus — targets | http://localhost:9090/targets | Verificar coleta (`UP`) |
| Prometheus — alertas | http://localhost:9090/alerts | Ver estados `INACTIVE`, `PENDING` e `FIRING` |
| Grafana | http://localhost:3000 | Dashboards |
| Dashboard do pipeline | http://localhost:3000/d/resilient-pipeline | Painéis de ingestão, processamento, lag e estado do alerta |
| Alertmanager | http://localhost:9093 | Alertas ativos e silenciamentos |

**Credenciais padrão do Grafana:** usuário `admin`; senha `admin_resilient_2026`, **somente se** não houver valores personalizados em `GF_ADMIN_USER` / `GF_ADMIN_PASSWORD` e se o volume do Grafana não tiver guardado outra senha. Não publique credenciais reais no repositório.

**Observações:** o Consumer expõe `/metrics` na porta interna `8001`, acessível ao Prometheus pela rede Docker; não há publicação dessa porta no host. O Kafka está acessível no host em `localhost:9092` (além da rede interna Docker).

## 4. Verificações rápidas

```powershell
# Estado dos containers
docker compose ps

# Verificar readiness da API
Invoke-RestMethod http://localhost:8000/ready

# Ver logs recentes
docker compose logs producer --tail=50
docker compose logs consumer --tail=50

# Consultar todos os serviços (logs em tempo real; Ctrl+C para sair)
docker compose logs -f
```

No Prometheus, confirme que os alvos essenciais (`producer`, `consumer`, `kafka-exporter`, `prometheus`) estão em estado `UP` antes dos testes.

## 5. Teste de carga pelo PowerShell

O gerador de carga usa a biblioteca padrão do Python, sem instalação de dependências adicionais.

```powershell
python --version
python benchmarks/load_test.py --scenario load --rate 10 --duration 30 --workers 40
```

Atenção ao relatório: `scheduled` = requisições planejadas, `submitted` = efetivamente submetidas, `skipped_busy` = descartadas pelo gerador por falta de workers e `confirmed` = respostas confirmadas. Não trate `scheduled` como eventos efetivamente publicados.

### Consultar lag acumulado no Prometheus

```powershell
$query = 'sum(kafka_consumergroup_lag{consumergroup="events-processor",topic="raw-events"} >= 0)'
$uri = "http://localhost:9090/api/v1/query?query=$([uri]::EscapeDataString($query))"
(Invoke-RestMethod $uri).data.result | ForEach-Object { $_.value }
```

### Consultar alertas ativos

```powershell
(Invoke-RestMethod http://localhost:9090/api/v1/alerts).data.alerts |
    Select-Object state, labels, activeAt
```

## 6. Simulação de lag e recuperação

O script oficial de lag é `benchmarks/scenarios/simulate_lag.sh` e exige **Bash**, além de `python3`, `docker` e `curl` disponíveis nesse ambiente. **Não é um script PowerShell nativo.** Para uso pelo PowerShell, é possível realizar manualmente um cenário simples:

```powershell
# 1. Interromper o processamento temporariamente
docker compose stop consumer

# 2. Gerar carga (ajuste conforme a capacidade do computador)
python benchmarks/load_test.py --scenario load --rate 20 --duration 60 --workers 80

# 3. Acompanhar o lag no Grafana / Prometheus e o alerta HighConsumerLag
# 4. Reiniciar e escalar para 3 consumers
docker compose up -d --scale consumer=3 consumer
```

A condição obrigatória para `HighConsumerLag` é **lag total acima de 500 por mais de 30 segundos**. O teste manual acima não garante que esse limiar seja atingido; confira as métricas e os alertas efetivos. Registre capturas de `FIRING`, da recuperação e do lag drenado até zero.

## 7. Encerrar, reiniciar e atualizar

```powershell
# Parar containers sem apagar volumes
docker compose down

# Subir novamente
docker compose up -d

# Reconstruir aplicações depois de alterações no código
docker compose up -d --build

# Reiniciar apenas o Grafana (ex.: após alterar o JSON provisionado)
docker compose restart grafana
```

**Evite `docker compose down -v` durante os testes e antes da apresentação:** essa opção remove volumes persistentes, incluindo dados do Kafka, Prometheus e Grafana.

## 8. Arquivos importantes

| Arquivo | Conteúdo |
|---|---|
| `compose.yaml` | Serviços, redes, portas e variáveis de ambiente |
| `.env.example` | Exemplo de configuração local |
| `infra/prometheus/prometheus.yml` | Targets e intervalo de coleta |
| `infra/prometheus/rules/pipeline.yml` | Regras `HighConsumerLag` e `HighErrorRate` |
| `infra/grafana/provisioning/dashboards/json/pipeline.json` | Dashboard provisionado automaticamente |
| `docs/09-runbook.md` | Diagnóstico e resposta a incidentes |
| `benchmarks/scenarios/simulate_lag.sh` | Cenário automatizado de lag (Bash) |
| `benchmarks/scenarios/simulate_errors.sh` | Cenário automatizado de erros (Bash) |

> Este guia contém comandos de execução e acesso, não representa uma certificação de que todos os cenários de integração foram aprovados no ambiente atual. Para a apresentação, valide e registre as evidências na máquina que será usada na defesa.
