# Observability Resilient Pipeline — Guia rápido de execução

> Ambiente: Windows (PowerShell) com Docker Desktop. Executar os comandos a partir da pasta raiz do repositório `observability-resilient-pipeline`, na branch `Development`.

## 1. Preparar o projeto

```powershell
git switch Development
git pull origin Development
```

Confirme que o Docker Desktop está aberto e com o engine em execução.

### Método principal no Windows: `start-pipeline.ps1`

O script de inicialização realmente versionado no repositório é
`start-pipeline.ps1`. Execute-o na raiz do projeto:

```powershell
.\start-pipeline.ps1 -OpenBrowser
```

O script:

* verifica Docker, Docker Compose e o engine;
* cria o `.env` a partir de `.env.example` se o arquivo não existir;
* solicita o usuário administrador, se necessário, e as senhas
  `GF_ADMIN_PASSWORD`/`GF_VIEWER_PASSWORD` com `Read-Host -AsSecureString`
  (entrada oculta e confirmação), sem exibi-las nem gravá-las no histórico de
  comandos;
* valida a configuração com `docker compose config --quiet`;
* sobe os serviços sem apagar volumes;
* confere endpoints, targets do Prometheus e o encerramento do provisionador do
  `viewer`.

Após alterar código, use `.\start-pipeline.ps1 -Build` para reconstruir as
imagens.

### Alternativa manual

Se preferir executar manualmente:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
# edite o .env e preencha GF_ADMIN_USER, GF_ADMIN_PASSWORD e GF_VIEWER_PASSWORD
docker compose config
docker compose up -d
```

Edite o `.env` local (ignorado pelo Git) e preencha obrigatoriamente:

* `GF_ADMIN_USER` e `GF_ADMIN_PASSWORD` — conta administrativa do Grafana. Cada
  instalação pode usar sua própria senha administrativa.
* `GF_VIEWER_PASSWORD` — senha do usuário `viewer`, criado automaticamente. É
  uma credencial compartilhada de demonstração, configurada localmente; não é
  uma senha individual por integrante.

Não existem credenciais padrão no repositório. Se alguma variável obrigatória
estiver vazia, `docker compose config` e `docker compose up -d` falham com uma
mensagem clara.

## 2. Subir a aplicação

```powershell
docker compose up -d
docker compose ps
```

Use `docker compose up -d --build` quando precisar reconstruir as imagens após
alteração de código. A primeira inicialização pode demorar mais, especialmente
enquanto o Kafka é iniciado e os tópicos são criados. Os serviços
`kafka-permissions`, `kafka-init` e `grafana-viewer-provisioner` são tarefas de
inicialização: podem aparecer como **Exited (0)** após a conclusão, o que é
esperado. O provisionador aguarda o Grafana ficar saudável, cria o usuário
`viewer` na organização 1 com papel `Viewer` (se ainda não existir) e encerra.

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

**Credenciais do Grafana:**

* **Administrador:** use `GF_ADMIN_USER` / `GF_ADMIN_PASSWORD` definidos no `.env`
  local. Cada instalação pode ter sua própria senha administrativa; não há senha
  padrão no repositório.
* **Viewer:** o usuário `viewer` é criado automaticamente e possui papel
  `Viewer` na organização 1: consegue visualizar dashboards, mas não editar nem
  acessar configurações administrativas. A senha vem de
  `GF_VIEWER_PASSWORD` e é uma **credencial compartilhada de demonstração**,
  configurada localmente; não é individual por integrante e não deve ser
  reutilizada fora do ambiente local.
* Acesso anônimo e cadastro público estão desabilitados. Credenciais reais nunca
  devem ir para o Git.

**Persistência de usuários:** os usuários do Grafana ficam no volume nomeado
`grafana_data` (`/var/lib/grafana`). Em instalação nova, o administrador é criado
com `GF_ADMIN_USER`/`GF_ADMIN_PASSWORD` e o `viewer` pelo provisionador com
`GF_VIEWER_PASSWORD`. Em reinicializações com volume existente, as senhas já
gravadas são preservadas; alterar o `.env` não redefine a senha do administrador
nem a de um `viewer` já existente. `docker compose down` preserva os volumes. A
remoção de volumes é um procedimento destrutivo separado na seção 8.

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

No Prometheus, confirme que os alvos essenciais (`producer`, `consumer`, `kafka-exporter`, `kafka-jmx`, `prometheus`) estão em estado `UP` antes dos testes.

## 5. Teste de carga pelo PowerShell

O gerador de carga usa a biblioteca padrão do Python, sem instalação de
dependências adicionais, mas requer Python 3.10+ disponível no host.

```powershell
python --version
python benchmarks/load_test.py --scenario load --rate 10 --duration 30 --workers 40
```

Atenção ao relatório: `scheduled` = requisições planejadas, `submitted` =
efetivamente submetidas, `skipped_busy`/`skipped_late` = descartadas pelo gerador
e `confirmed` = respostas confirmadas. Não trate `scheduled` como eventos
efetivamente publicados. Em uma execução anterior do cenário de lag, foram
planejadas 2.100 submissões e apenas 680 foram efetivamente submetidas.

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
# 4. Reiniciar e escalar para 3 consumers, sem recriar dependências
docker compose up -d --no-deps --scale consumer=3 consumer
```

A carga manual planeja 1.200 submissões (20 req/s por 60 s), mas o gerador pode
descartar parte delas. A condição obrigatória para `HighConsumerLag` é **lag
total acima de 500 por mais de 30 segundos**. O teste manual acima não garante
que esse limiar seja atingido; confira as métricas e os alertas efetivos.
Registre capturas de `FIRING`, da recuperação e do lag drenado até zero.

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

Esses comandos preservam os volumes persistentes. A remoção de volumes é tratada
separadamente na próxima seção.

## 8. Limpeza destrutiva (opcional)

> **Atenção: perda de dados.** O comando abaixo remove os volumes
> `kafka_data`, `prometheus_data`, `grafana_data` e `alertmanager_data`. Isso
> apaga usuários e configurações persistidas no Grafana, histórico do
> Prometheus e dados do Kafka. Dashboards e datasources são reprovisionados a
> partir dos arquivos, mas usuários e dados históricos não são recuperados.
> Execute apenas se quiser reiniciar o ambiente do zero, nunca durante os testes
> ou antes da apresentação.

```powershell
docker compose down -v
```

## 9. Arquivos importantes

| Arquivo | Conteúdo |
|---|---|
| `start-pipeline.ps1` | Inicialização principal no Windows (Docker, `.env`, subida e verificações) |
| `compose.yaml` | Serviços, redes, portas e variáveis de ambiente |
| `.env.example` | Exemplo de configuração local |
| `infra/prometheus/prometheus.yml` | Targets e intervalo de coleta |
| `infra/prometheus/rules/pipeline.yml` | Regras `HighConsumerLag` e `HighErrorRate` |
| `infra/grafana/provisioning/dashboards/json/pipeline.json` | Dashboard provisionado automaticamente |
| `infra/grafana/provisioning/viewer_provision.py` | Provisionamento idempotente do usuário `viewer` |
| `docs/09-runbook.md` | Diagnóstico e resposta a incidentes |
| `docs/15-grafana-viewer-provisioning-report.md` | Provisionamento do Grafana, credenciais e persistência |
| `benchmarks/scenarios/simulate_lag.sh` | Cenário automatizado de lag (Bash) |
| `benchmarks/scenarios/simulate_errors.sh` | Cenário automatizado de erros (Bash) |

> **Estado de validação:** a suíte unitária tem 52 testes aprovados; o
> provisionamento do Grafana e os dois alertas foram validados manualmente em
> Docker. Os scripts `.sh` de cenário ainda não foram executados de ponta a
> ponta no host da apresentação. Este guia não certifica que todos os cenários
> de integração foram aprovados no ambiente atual; valide e registre as
> evidências na máquina que será usada na defesa.
