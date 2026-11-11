# Observability Resilient Pipeline — Guia técnico da equipe

> **Público:** integrantes da equipe do projeto. Para uma visão rápida de uso,
> consulte o [README](../README.md).
>
> **Ambiente:** Windows com PowerShell 5.1+ e Docker Desktop. Execute os comandos
> na raiz do repositório `observability-resilient-pipeline`, na branch
> `Development`.

## 1. Inicialização oficial

### Pré-requisitos

* Windows com PowerShell 5.1+.
* Docker Desktop aberto, com o engine em execução e Docker Compose v2.
* Repositório na raiz e na branch `Development`.
* Python 3.10+ somente para os testes de carga; não é necessário para subir a
  pilha.

### Comando

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start-pipeline.ps1 -OpenBrowser
```

Esse é o único fluxo oficial de inicialização e também é usado para reiniciar o
ambiente. A invocação não carrega perfis do PowerShell e aplica `Bypass` somente
a esse processo, sem alterar a política de execução do sistema.

### O que o `start-pipeline.ps1` executa

O comportamento abaixo foi confirmado pela leitura do script versionado:

1. Verifica `docker --version`, `docker compose version` e o acesso ao Docker
   Engine com `docker info`. Se o Docker Desktop não estiver em execução, encerra
   com erro.
2. Garante o `.env`:
   * se o arquivo não existir, copia `.env.example`;
   * se `GF_ADMIN_USER`, `GF_ADMIN_PASSWORD` ou `GF_VIEWER_PASSWORD` estiverem
     vazias ou com valores de exemplo, solicita os valores;
   * o usuário administrador é lido com `Read-Host` (padrão `admin`);
   * as senhas são lidas com `Read-Host -AsSecureString`, com confirmação, e não
     são exibidas nem gravadas no histórico de comandos;
   * o arquivo é gravado em UTF-8 sem BOM. Se as três variáveis já estiverem
     preenchidas, nada é alterado.
3. Valida a configuração com `docker compose config --quiet`.
4. Sobe os serviços com `docker compose up -d`, sem remover volumes.
5. Aguarda até 180 segundos, por padrão, pelos quatro endpoints de saúde. O
   limite pode ser ajustado com `-TimeoutSeconds` (15 a 600).
6. Consulta o Prometheus e confere os seis targets obrigatórios em estado `UP`.
7. Confere o serviço one-shot `grafana-viewer-provisioner` com `docker inspect`.
   Ele é considerado concluído quando `State.Running` é `false` e
   `State.ExitCode` é `0`.
8. Gera a Central de Acesso em
   `%TEMP%\observability-pipeline-launchpad.html` e, com `-OpenBrowser`, abre a
   página no navegador.

Flags úteis:

* `-OpenBrowser` — abre a Central de Acesso ao final.
* `-OpenAllPages` — abre também os painéis principais no navegador.
* `-ShowDetails` — imprime `docker compose ps` ao final.
* `-TimeoutSeconds <15-600>` — altera o tempo máximo de espera.

Códigos de saída:

* `0` — quatro endpoints OK, seis targets `UP` e provisionador concluído.
* `2` — ambiente iniciado, mas há verificações pendentes.
* `1` — erro de preparação, como Docker indisponível, `.env` incompleto sem
  `.env.example` ou falha em `docker compose config`/`up`.

### Significado das verificações

**Endpoints de saúde**

| Verificação | Endpoint | O que indica |
|---|---|---|
| Producer | `http://localhost:8000/ready` | API pronta e com acesso ao tópico Kafka |
| Prometheus | `http://localhost:9090/-/ready` | Servidor pronto para consultas e coleta |
| Grafana | `http://localhost:3000/api/health` | Grafana saudável e banco acessível |
| Alertmanager | `http://localhost:9093/-/ready` | Servidor pronto para receber alertas |

**Targets do Prometheus**

| Target | O que cobre |
|---|---|
| `producer` | Métricas HTTP e de publicação da API |
| `consumer` | Métricas de processamento, lag por partição e DLQ |
| `kafka-exporter` | Offsets, partições e lag do consumer group |
| `kafka-jmx` | Métricas do broker, incluindo bytes de entrada e saída |
| `prometheus` | Auto-coleta do próprio Prometheus |
| `alertmanager` | Estado do Alertmanager |

O script só considera o ambiente pronto quando os quatro endpoints respondem e
os seis targets estão `UP`. Caso contrário, retorna `2` e a Central de Acesso
mostra os itens pendentes.

## 2. Central de Acesso

A Central é gerada a cada execução do script em:

```text
%TEMP%\observability-pipeline-launchpad.html
```

Ela é um arquivo HTML estático, não um servidor. O próprio script monta o
conteúdo (status, targets e atalhos), grava o arquivo em UTF-8 sem BOM e, com
`-OpenBrowser`, chama `Start-Process` para abri-lo. Contém:

* o resumo da inicialização e o horário da verificação;
* o estado dos seis targets;
* atalhos para o dashboard do Grafana, targets e alertas do Prometheus,
  Alertmanager, readiness, métricas e documentação da API.

Se a abertura automática falhar, abra manualmente:

```powershell
Start-Process (Join-Path $env:TEMP 'observability-pipeline-launchpad.html')
```

A página é um retrato da inicialização e não se atualiza sozinha. Para atualizar
o resumo e os estados, execute novamente o script oficial.

## 3. Credenciais e provisionamento do `viewer`

### Origem das credenciais

O `.env` local é a única fonte das credenciais. O `.env.example` contém as
variáveis vazias e o Compose falha com mensagem clara se alguma estiver ausente:

* `GF_ADMIN_USER` e `GF_ADMIN_PASSWORD` — conta administrativa do Grafana,
  escolhida em cada instalação;
* `GF_VIEWER_PASSWORD` — senha do usuário `viewer`.

Não há senha administrativa padrão no repositório. O `.env` é ignorado pelo Git
e não deve ser commitado.

### Como o `viewer` é criado

O usuário `viewer` não é criado pelo `start-pipeline.ps1`. O provisionamento é
feito pelo serviço one-shot `grafana-viewer-provisioner`, definido no
`compose.yaml`:

1. O serviço aguarda o Grafana ficar saudável (`condition: service_healthy`) e
   não reinicia após terminar (`restart: "no"`).
2. O script `infra/grafana/provisioning/viewer_provision.py` usa a Admin API do
   Grafana com as credenciais administrativas do `.env`.
3. Se o usuário não existir, cria `viewer` com papel `Viewer` na organização 1.
   Se já existir, garante o vínculo com a organização e o papel `Viewer`.
4. A senha de um usuário já existente é preservada; o provisionamento é
   idempotente. Se o usuário tiver privilégios de administrador do servidor, o
   provisionador aborta em vez de rebaixá-lo silenciosamente.
5. A senha do administrador não é alterada.

O `start-pipeline.ps1` apenas verifica se o serviço encerrou com código `0`. Se
o provisionamento falhar, a verificação fica pendente e o script retorna `2`.
Os logs do serviço não imprimem as senhas.

### Persistência

Usuários e configurações do Grafana ficam no volume nomeado `grafana_data`
(`/var/lib/grafana`). Em reinicializações com o volume existente, as senhas já
gravadas são preservadas: alterar o `.env` não redefine a senha do administrador
nem a de um `viewer` já existente. `docker compose stop` e `docker compose down`
preservam os volumes. A remoção de volumes é tratada na seção 8.

## 4. Diagnóstico opcional

Execute os comandos abaixo na raiz do repositório, apenas quando o script
indicar pendências ou antes de uma demonstração. Eles não fazem parte do fluxo
normal de inicialização.

```powershell
# Inclui containers já encerrados
docker compose ps -a

# Logs do provisionamento do viewer
docker compose logs grafana-viewer-provisioner --tail=100

# Logs das aplicações
docker compose logs producer --tail=50
docker compose logs consumer --tail=50

# Readiness da API
Invoke-RestMethod http://localhost:8000/ready
```

Os serviços `kafka-permissions`, `kafka-init` e `grafana-viewer-provisioner` são
tarefas de inicialização. Podem aparecer como **Exited (0)** após a conclusão, o
que é esperado.

Consulta dos targets ativos:

```powershell
(Invoke-RestMethod 'http://localhost:9090/api/v1/targets?state=active').data.activeTargets |
    Select-Object @{n='job';e={$_.labels.job}}, health
```

Consulta do lag acumulado:

```powershell
$query = 'sum(kafka_consumergroup_lag{consumergroup="events-processor",topic="raw-events"} >= 0)'
$uri = "http://localhost:9090/api/v1/query?query=$([uri]::EscapeDataString($query))"
(Invoke-RestMethod $uri).data.result | ForEach-Object { $_.value }
```

Consulta dos alertas ativos:

```powershell
(Invoke-RestMethod http://localhost:9090/api/v1/alerts).data.alerts |
    Select-Object state, labels, activeAt
```

## 5. Testes automatizados

A suíte é unitária: valida módulos, configuração, documentação e scripts, mas
não sobe a pilha Docker nem executa os cenários de integração de ponta a ponta.

Método reproduzido nesta revisão, sem depender do Python do host:

```powershell
docker run --rm -v "${PWD}:/work" -w /work python:3.13-slim python -m unittest discover -s tests -p "test_*.py" -v
```

Se houver Python 3.10+ no host, o comando equivalente é:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

**Resultado desta revisão:** 52 testes executados, todos aprovados.

## 6. Testes de carga

O gerador usa apenas a biblioteca padrão do Python e exige Python 3.10+ no host.
Execute na raiz, com a API em execução e o ambiente estável.

```powershell
# Carga estável
python benchmarks/load_test.py --scenario load --rate 10 --duration 30 --workers 40

# Estresse progressivo (três degraus)
python benchmarks/load_test.py --scenario stress --rate 10 --duration 30

# Surto repentino (baseline, pico e recuperação)
python benchmarks/load_test.py --scenario spike --rate 10 --duration 30
```

Leitura do relatório:

* `phases[].scheduled` — requisições planejadas para cada fase;
* `phases[].submitted` — requisições efetivamente submetidas;
* `phases[].skipped_busy` e `phases[].skipped_late` — descartadas pelo gerador;
* `attempted` e `confirmed` — totais no nível superior do relatório;
* o processo retorna `0` somente quando todas as tentativas foram confirmadas;
  caso contrário, retorna `1`.

Não trate `scheduled` como eventos publicados. Em uma execução anterior do
cenário de lag, foram planejadas 2.100 submissões e apenas 680 foram efetivamente
submetidas.

## 7. Demonstrações

Os scripts oficiais de cenário são Bash/POSIX e exigem `bash`, `python3`,
`docker` e `curl`. No Windows, use WSL ou Git Bash. Eles não são scripts
PowerShell nativos.

### 7.1 Lag e rebalanceamento

Script oficial:

```bash
./benchmarks/scenarios/simulate_lag.sh
```

O cenário:

1. garante 1 consumer e zera a taxa de erros;
2. planeja 60 req/s por 35 s (2.100 submissões planejadas);
3. consulta o lag no Prometheus;
4. aguarda a janela de 30 s do alerta `HighConsumerLag` e procura o estado
   `FIRING` por até 60 s;
5. escala para 3 consumers e acompanha a drenagem;
6. procura a resolução do alerta por até 120 s.

**Resultado esperado quando a carga submetida é suficiente:** lag total acima de
500 por mais de 30 s, alerta `HighConsumerLag` em `PENDING`/`FIRING`, seguido de
drenagem e resolução após a escala. O `FIRING` depende da carga realmente
submetida e do ambiente; o script emite aviso se ele não ocorrer.

**Limitação:** o script não retorna o consumer para 1 réplica ao final. Para
restaurar o estado padrão após a demonstração:

```powershell
docker compose up -d --no-deps --scale consumer=1 consumer
```

Alternativa manual pelo PowerShell, quando não houver Bash disponível:

```powershell
# 1. Interromper o processamento
docker compose stop consumer

# 2. Gerar carga (1.200 submissões planejadas; parte pode ser descartada)
python benchmarks/load_test.py --scenario load --rate 20 --duration 60 --workers 80

# 3. Observar o lag e o alerta no Grafana/Prometheus
# 4. Escalar para 3 consumers
docker compose up -d --no-deps --scale consumer=3 consumer
```

O teste manual não garante o limiar de 500 mensagens por 30 s. Registre as
evidências reais de `PENDING`, `FIRING`, drenagem e resolução.

### 7.2 Injeção de erros

Script oficial:

```bash
./benchmarks/scenarios/simulate_errors.sh
```

O cenário:

1. recria o consumer com `PROCESSING_ERROR_RATE=0.20` e confirma o valor com
   `printenv`;
2. planeja 10 req/s por 60 s (600 submissões planejadas);
3. aguarda o alerta `HighErrorRate` em `FIRING` por até 120 s;
4. restaura `PROCESSING_ERROR_RATE=0` e confirma o valor;
5. envia 130 s de tráfego de recuperação para limpar a janela móvel;
6. aguarda a resolução do alerta por até 60 s.

**Resultado esperado quando a carga submetida é suficiente:** taxa de erros
acima de 5% na janela de 2 minutos, `HighErrorRate` em `PENDING`/`FIRING` e
posterior resolução após a restauração. A regra exige a janela de 2 minutos e um
`for` de 10 s; a carga efetivamente submetida pode ser menor que a planejada, e
o `FIRING` não é garantido.

Alternativa manual pelo PowerShell:

```powershell
$env:PROCESSING_ERROR_RATE = '0.20'
docker compose up -d --no-deps --scale consumer=1 --force-recreate consumer
docker compose exec -T consumer printenv PROCESSING_ERROR_RATE

python benchmarks/load_test.py --scenario load --rate 10 --duration 60 --workers 10

# Observe o alerta e depois restaure a taxa normal
$env:PROCESSING_ERROR_RATE = '0'
docker compose up -d --no-deps --scale consumer=1 --force-recreate consumer
docker compose exec -T consumer printenv PROCESSING_ERROR_RATE

python benchmarks/load_test.py --scenario load --rate 10 --duration 130 --workers 10
```

### 7.3 Alertas e recuperação

O Prometheus avalia as regras e envia os alertas ao Alertmanager. Acompanhe os
estados em `http://localhost:9090/alerts` e os alertas recebidos em
`http://localhost:9093`.

* `HighConsumerLag`: lag total acima de 500 por mais de 30 s.
* `HighErrorRate`: mais de 5% de erros na janela móvel de 2 minutos, com
  sustentação de 10 s.

A recuperação depende da condição ficar falsa pelo tempo necessário. Em caso de
erros, aguarde a janela de 2 minutos antes de concluir que o alerta não resolveu.
Se o estado não mudar, confira os targets, as métricas e os logs do consumer.

## 8. Encerrar, restaurar e limpar

```powershell
# Para os containers e preserva os volumes
docker compose stop

# Remove os containers e as redes, preservando os volumes
docker compose down
```

Para iniciar novamente, use o comando oficial da seção 1, que também gera e abre
a Central de Acesso.

Após a demonstração de lag, restaure o consumer para 1 réplica. Após a
demonstração de erros, confirme `PROCESSING_ERROR_RATE=0`; o script oficial já
faz essa restauração.

### Limpeza destrutiva (opcional)

> **Atenção: perda de dados.** O comando abaixo remove os volumes
> `kafka_data`, `prometheus_data`, `grafana_data` e `alertmanager_data`. Isso
> apaga usuários e configurações persistidas no Grafana, histórico do
> Prometheus e dados do Kafka. Dashboards e datasources são reprovisionados a
> partir dos arquivos, mas usuários e dados históricos não são recuperados.
> Execute apenas para reiniciar o ambiente do zero, nunca durante os testes ou
> antes da apresentação.

```powershell
docker compose down -v
```

## 9. Resultados, limitações e estado de validação

**Comprovado nesta revisão**

* A suíte unitária tem 52 testes aprovados.
* O provisionamento do Grafana e os dois alertas foram validados manualmente em
  Docker em etapas anteriores do projeto.

**Ainda depende de validação no host da apresentação**

* Os scripts `simulate_lag.sh` e `simulate_errors.sh` não foram executados de
  ponta a ponta no host da defesa.
* O `FIRING` dos alertas depende da carga efetivamente submetida, das janelas
  móveis e do ambiente; não há garantia de que ocorra em toda execução.
* A Central de Acesso é um retrato estático da inicialização e precisa ser
  regenerada pelo script para refletir o estado atual.
* O gerador de carga pode descartar requisições; 2.100 planejadas não equivalem
  a 2.100 eventos publicados. Na execução anterior do cenário de lag, 680 de
  2.100 foram submetidas.

Registre as evidências na máquina usada na defesa e não trate cenários pendentes
como aprovados.

## 10. Referências

* [README](../README.md) — visão rápida para usuários e avaliadores.
* [Runbook de incidentes](09-runbook.md) — diagnóstico e resposta.
* [Relatório de provisionamento do Grafana](15-grafana-viewer-provisioning-report.md)
  — credenciais, `viewer` e persistência.
* [Relatório de correções de entrega](13-delivery-fixes-report.md) — correções e
  validações.
* [Auditoria do Kafka Exporter](12-kafka-exporter-audit.md) — métricas do broker.
* [Plano de benchmarks](07-benchmark-plan.md) e [plano de testes](08-test-plan.md)
  — critérios e escopo.
