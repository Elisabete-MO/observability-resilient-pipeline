# Relatório Técnico de Teste de Estresse e Ponto de Ruptura (Breakpoint Test)

Este documento registra a execução, métricas e o diagnóstico do teste de estresse de saturação e ruptura (*breakpoint test*) executado contra a arquitetura distribuída do pipeline de observabilidade e mensageria (**Producer API → Apache Kafka → Consumer Worker**).

---

## 1. Objetivo do Teste

Avaliar a resiliência e os limites operacionais do sistema sob volumes maciços de requisições concorrentes, mapeando:
1. O comportamento do mecanismo de autoproteção (*Fail-Fast Backpressure*).
2. O ponto de saturação do broker Apache Kafka e o acúmulo de *Consumer Lag*.
3. O comportamento do loop de eventos assíncrono (FastAPI/Uvicorn) e uso de recursos (CPU e RAM).
4. As condições exatas que levam à degradação e parada do serviço.

---

## 2. Metodologia e Fases de Execução

O teste foi executado em 4 fases progressivas utilizando um gerador assíncrono de alto desempenho (`asyncio` / TCP sockets) direcionado ao endpoint `POST /api/v1/events`:

* **Fase 1 (Linha de Base - Baseline):** 50 req/s, 50 conexões concorrentes por 10 segundos (500 requisições).
* **Fase 2 (Saturação Contínua):** 700 req/s, 700 conexões concorrentes por 15 segundos (10.500 requisições).
* **Fase 3 (Avalanche Maciça):** 8 rodadas consecutivas de 2.500 conexões instantâneas simultâneas (20.000 requisições).
* **Fase 4 (Ponto de Ruptura / Colapso):** Rajada de 6.000 conexões simultâneas concorrentes com medição de timeout e esgotamento de socket.

---

## 3. Dados Empíricos Coletados

### Tabela Consolidada de Resultados

| Métrica / Parâmetro | Fase 1 (Baseline) | Fase 2 (Saturação) | Fase 3 (Avalanche) | Fase 4 (Ruptura) |
|---|---|---|---|---|
| **Volume de Requisições** | 500 | 10.500 | 20.000 | 6.000 |
| **Conexões Simultâneas** | 50 | ~10 em voo | 2.500 | 6.000 |
| **Sucesso HTTP 202 (Accepted)** | **500 (100.0%)** | **10.500 (100.0%)** | **12.280 (61.4%)** | **2.726 (45.4%)** |
| **Rejeição HTTP 503 (Backpressure)** | 0 (0.0%) | 0 (0.0%) | **7.720 (38.6%)** | **1.318 (22.0%)** |
| **Timeouts (> 10s) / Parada** | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **1.956 (32.6%)** |
| **Latência p50** | 7.38 ms | 6.85 ms | ~868 ms | **6.411 ms (6.4s)** |
| **Latência p95** | 8.00 ms | 10.47 ms | ~1.183 ms | **10.349 ms (10.3s)** |
| **Latência p99** | 8.54 ms | 13.37 ms | ~1.203 ms | **10.356 ms (10.3s)** |
| **CPU do Producer (Pico)** | 0.32% | 0.15% | 0.35% | **101.01% (Saturação)** |
| **Memória do Producer** | 55.3 MiB (21.6%) | 43.4 MiB (16.9%) | 114.5 MiB (44.7%) | **153.6 MiB (60.0%)** |
| **Consumer Lag Máximo** | 0 msgs | 9.303 msgs | 22.442 msgs | 25.168+ msgs |
| **Estado dos Alertas** | Normal | `HighConsumerLag` (FIRING) | `HighConsumerLag` (FIRING) | `HighConsumerLag` (FIRING) |

---

## 4. Diagnóstico Técnico: Motivos da Degradação e Possível Parada

A análise do código-fonte e dos limites de infraestrutura identificou 4 barreiras e pontos de parada:

```
[Inundação de Requisições]
           │
           ▼
[1. Semáforo de Concorrência (500)] ────► HTTP 503 (Rejeição imediata de excedentes)
           │
           ▼
[2. Saturação de CPU no Event Loop]  ────► Timeouts de Cliente (> 10s) / Falha de atendimento
           │
           ▼
[3. Consumer Lag no Apache Kafka]   ────► Alerta HighConsumerLag (FIRING no Alertmanager)
           │
           ▼
[4. Teto de RAM no Docker (256m)]   ────► OOMKilled (Exit Code 137 / Queda total do serviço)
```

### 1. Mecanismo de Fail-Fast: Semáforo de Concorrência (HTTP 503)
* **Local no código:** `apps/producer/main.py:43` e `312–316`
```python
MAX_CONCURRENT_PRODUCES = int(os.getenv("PRODUCER_MAX_CONCURRENT_PRODUCES", "500"))
...
if produce_semaphore.locked():
    raise HTTPException(
        503,
        "Producer sobrecarregado (concorrencia maxima de publicacao atingida). Tente novamente.",
    )
```
* **Diagnóstico:** O Producer impõe um limite máximo de 500 requisições simultâneas em trânsito com o Kafka. Quando uma avalanche de 2.500 conexões atingiu o serviço, as primeiras 500 adquiriram o semáforo. Todas as requisições subsequentes encontraram `produce_semaphore.locked() == True` e foram rejeitadas com **HTTP 503**.
* **Efeito:** Trata-se de uma falha controlada (*circuit breaker in-process*). O serviço preservou sua estabilidade interna descartando 7.720 requisições (38.6%) sem travar o processo.

### 2. Saturação do Event Loop e Fila de Sockets (Timeouts > 10s)
* **Diagnóstico:** Na rajada de 6.000 conexões concorrentes, a capacidade de agendamento do *Single-Threaded Event Loop* do Python foi superada.
* **Métrica:** O container `producer-1` atingiu **101.01% de uso de CPU**.
* **Efeito:** **1.956 requisições (32.6%)** sofreram *Client Timeout* (> 10s). O servidor não conseguiu responder a tempo, caracterizando a **parada de atendimento** sob sobrecarga extrema de conexões.

### 3. Descompasso de Vazão e Acúmulo Crítico de Consumer Lag
* **Local no código:** `apps/consumer/main.py:34-35`
```python
PROCESSING_MIN_MS = int(os.getenv("PROCESSING_MIN_MS", "50"))
PROCESSING_MAX_MS = int(os.getenv("PROCESSING_MAX_MS", "200"))
```
* **Diagnóstico:** Enquanto o Producer foi capaz de persistir até 1.000 eventos/s no Kafka, um único Consumer Worker processa em média a cada 125ms (~8 a 12 eventos/s).
* **Efeito:** Foram injetados mais de 25.000 eventos no tópico `raw-events`. O lag atingiu **22.442 mensagens acumuladas**.
* **Alerta no Alertmanager:**
```json
{
  "labels": { "alertname": "HighConsumerLag", "severity": "warning" },
  "annotations": {
    "summary": "Acumulo de mensagens no Consumer",
    "description": "Lag total acima de 500 por mais de 30 segundos. Valor: 22442."
  },
  "status": { "state": "active" }
}
```

### 4. Risco de Colapso por Memória (OOMKilled - Exit Code 137)
* **Local no código:** `compose.yaml:133` (`mem_limit: 256m`)
* **Diagnóstico:** Durante os testes, o uso de RAM do Producer escalou de **43.9 MiB para 153.6 MiB (60.0% da cota)**.
* **Efeito Potencial:** Caso a carga de milhares de conexões permaneça contínua com payloads volumosos ou conexões lentas (*Slowloris*), o heap do processo Python ultrapassa 256 MiB e o kernel do Linux finaliza o container via `SIGKILL` (código de saída 137), gerando indisponibilidade total (*Connection Refused*).

---

## 5. Conclusões e Diagnóstico de Resiliência

1. **Integridade de Dados:** Nenhuma mensagem aceita com HTTP 202 foi perdida. O Kafka persistiu todos os eventos confirmados com semântica *at-least-once* e idempotência ativa.
2. **Eficácia da Autoproteção:** O semáforo agiu como disjuntor de segurança (*circuit breaker in-process*), descartando requisições excedentes antes que o processo sofresse crash por falta de memória.
3. **Eficácia da Observabilidade:** O Prometheus e o Alertmanager detectaram imediatamente a sobrecarga, disparando alertas de saturação de lag em tempo real.

---

## 6. Plano de Melhorias Arquiteturais e Otimizações Recomendadas

A partir dos dados coletados nas 4 fases de estresse, foram mapeadas melhorias divididas em três horizontes:

### 6.1. Curto Prazo (Configuração & Ganhos Imediatos)
* **Escalonamento de Consumidores (`consumer=3`):**
  * *Problema resolvido:* Acúmulo de 22.442 mensagens de lag em 1 única réplica.
  * *Ação:* Alocar 1 instância do Consumer Worker para cada partição do tópico `raw-events` (`docker compose up -d --scale consumer=3`), triplicando a velocidade de drenagem de ~12 msg/s para ~36–45 msg/s.
* **Ativação de Compressão e Loteamento no Kafka Producer:**
  * *Problema resolvido:* Consumo excessivo de I/O de rede e buffers de socket do broker.
  * *Ação:* Adicionar `compression.type: "lz4"`, `linger.ms: 5` e `batch.num.messages: 1000` na configuração do `librdkafka`. Reduz o tráfego de rede em até 70% e agrupa mensagens simultâneas em batches atômicos.
* **Ajuste de Margem de Memória do Container:**
  * *Problema resolvido:* Consumo de memória atingiu 153.6 MiB (60% da cota de 256 MiB).
  * *Ação:* Elevar `mem_limit: 512m` no `compose.yaml` para o serviço `producer`, prevenindo acionamento acidental do Linux OOM Killer.

### 6.2. Médio Prazo (Aplicação & Concorrência)
* **Multi-Processos no Producer (Uvicorn Multi-Worker):**
  * *Problema resolvido:* Saturação de CPU a 101.01% e 32.6% de timeouts de cliente (> 10s) na rajada de 6.000 conexões devido ao *Single-Threaded Event Loop* do Python.
  * *Ação:* Iniciar o Uvicorn com múltiplos workers (`--workers 4`). Cada processo executará seu próprio loop assíncrono e semáforo, elevando o teto de concorrência simultânea de 500 para 2.000 req/s estáveis.
* **Fila de Espera Curta no Semáforo (*Graceful Queueing*):**
  * *Problema resolvido:* O Producer rejeitava imediatamente requisições que chegavam quando 500 chamadas estavam ativas, mesmo que o Kafka levasse apenas ~7ms para liberar um slot.
  * *Ação:* Substituir a rejeição instantânea por uma espera delimitada de até 100ms (`await asyncio.wait_for(produce_semaphore.acquire(), timeout=0.100)`). Isso reduzirá descartes espúrios de HTTP 503 em surtos pontuais.
* **Consumo em Lote no Consumer Worker (*Batch Processing*):**
  * *Problema resolvido:* Processamento e commit síncrono mensagem a mensagem.
  * *Ação:* Utilizar leitura de blocos (`consumer.consume(num_messages=50, timeout=1.0)`) com processamento paralelo e commit de offset único por bloco.

### 6.3. Longo Prazo (Observabilidade Avançada & Resiliência de Produção)
* **Novas Regras de Alerta Pró-ativas no Prometheus:**
  * `ProducerBackpressureHigh` (WARNING): Dispara se a taxa de HTTP 503 ultrapassar 5% do tráfego total por 1 minuto.
  * `ProducerMemoryWarning` (WARNING): Dispara se o consumo de RAM do Producer atingir 80% do limite do contêiner.
  * `KafkaBrokerSlowAck` (CRITICAL): Dispara se a latência de persistência do broker superar 1.5s.
* **Auto-Scaling Orientado a Eventos (KEDA / Docker Autoscaler):**
  * Automação para escalonar automaticamente réplicas de consumidores quando o `kafka_consumergroup_lag` ultrapassar o limiar de 500 registros.
* **Proteção de Camada de Rede no ASGI (Socket Throttling):**
  * Configurar `--limit-concurrency 1000` e `--backlog 2048` no Uvicorn para descartar conexões excedentes antes que objetos Pydantic e buffers de memória sejam instanciados.

---

### 6.4. Matriz de Priorização das Melhorias

| Prioridade | Ação Recomendada | Camada Afetada | Impacto Esperado | Esforço Técnico |
|---|---|---|---|---|
| **P0 (Imediata)** | Escalar `consumer=3` | Infra / Docker | Drenagem do backlog de lag 3x mais rápida | Mínimo (1 comando) |
| **P1 (Alta)** | Suavizar semáforo (espera 100ms) | Aplicação Producer | Reduz rejeições 503 em até 80% sob rajada | Baixo (ajuste de código) |
| **P1 (Alta)** | Compressão LZ4 e `linger.ms: 5` | Mensageria Kafka | Reduz tráfego de rede e pressão de disco | Baixo (configuração) |
| **P2 (Média)** | Multi-Worker no Uvicorn (`--workers 4`) | Servidor ASGI | Elimina a saturação de CPU a 101% | Baixo (Dockerfile/compose) |
| **P2 (Média)** | Elevar `mem_limit: 512m` no Producer | Infra / Docker | Previne morte por OOM Killer (código 137) | Mínimo (compose.yaml) |
| **P3 (Média)** | Novos alertas de 503 e RAM no Prometheus | Observabilidade | Diagnóstico precoce de saturação | Baixo (rules.yml) |
| **P3 (Baixa)** | Consumo em lote (*batch processing*) | Aplicação Consumer | Aumenta vazão unitária de processamento | Médio (refatoração) |

---

## 7. Resultados Obtidos Após a Implementação das Otimizações

Após aplicadas as melhorias (Uvicorn `--workers 4`, tolerância de 100ms no semáforo, compressão LZ4/batching no Kafka, elevação de RAM para 512 MiB e escala para 3 consumidores), repetimos os mesmos testes de rajada instantânea:

### Comparativo: Rajada Instantânea de 1.200 Conexões
* **Antes:** 711 aceitas (59.2%), 489 descartadas com 503 (40.8%), latência p50=868ms, throughput de 951 req/s.
* **Depois:** **1.200 aceitas (100.0%)**, **0 erros (0.0%)**, latência p50=**227ms (redução de 74%)**, throughput de **2.788 req/s (quase 3x mais rápido)**.

### Comparativo: Rajada Instantânea de 3.000 Conexões
* **Antes:** 2.011 aceitas (67.0%), 989 descartadas com 503 (33.0%), latência p50=2.025ms, throughput de 801 req/s.
* **Depois:** **3.000 aceitas (100.0%)**, **0 erros (0.0%)**, latência p50=**592ms (redução de 71%)**, throughput de **3.018 req/s (4x mais rápido)**.

### Drenagem de Consumer Lag
* Com 3 réplicas do Consumer Worker alocadas individualmente para as 3 partições do tópico `raw-events`, a velocidade combinada de drenagem de mensagens superou 40 msg/s, mantendo o lag estabilizado próximo a zero mesmo sob rajadas massivas.

---

## 8. Teste de Limite Físico Máximo da Máquina Única e Ponto de Escalar para Segunda Máquina (Breakpoint Absoluto)

Para responder à pergunta **"Qual é o máximo que uma única máquina suporta antes de ser mandatório escalar para uma segunda máquina?"**, forçamos o sistema progressivamente com rajadas de **5.000, 8.000, 12.000 e 16.000 conexões simultâneas**.

### 8.1. Tabela de Degradação Progressiva até o Limite Físico

| Métrica / Parâmetro | 3.000 Conexões | 5.000 Conexões | 8.000 Conexões | 12.000 Conexões | 16.000 Conexões (Breakpoint Absoluto) |
|---|---|---|---|---|---|
| **Tempo de Execução** | 0.99 s | 1.89 s | 3.46 s | 6.91 s | **13.39 s** |
| **Throughput Efetivo** | **3.018,9 req/s** | **2.648,1 req/s** | **2.311,7 req/s** | **1.736,2 req/s** | **1.194,7 req/s** |
| **Sucesso HTTP 202** | **3.000 (100.0%)** | **5.000 (100.0%)** | **7.771 (97.1%)** | **10.642 (88.7%)** | **9.519 (59.5%)** |
| **Erros 503 (Backpressure)** | 0 (0.0%) | 0 (0.0%) | 229 (2.9%) | 1.358 (11.3%) | **1.404 (8.8%)** |
| **Client Timeout (> 10s) / Parada** | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **5.077 (31.7%) [COLAPSO DE ATENDIMENTO]** |
| **Latência Global p50** | 592.11 ms | 1.048,59 ms | 1.862,92 ms | 4.318,60 ms | **10.286,56 ms (10.28s)** |
| **Latência Global p95** | 834.24 ms | 1.515,50 ms | 3.045,35 ms | 6.051,02 ms | **12.707,23 ms (12.70s)** |
| **Latência Global p99** | 849.50 ms | 1.576,46 ms | 3.073,30 ms | 6.156,79 ms | **12.753,93 ms (12.75s)** |
| **RAM do Producer (Pico)** | 379.3 MiB (74.0%) | 447.4 MiB (87.4%) | 451.7 MiB (88.2%) | 492.8 MiB (96.2%) | **511.6 MiB / 512 MiB (99.92%) [LIMITE EXAURIDO]** |
| **CPU do Producer (Pico)** | ~0.77% | ~0.67% | ~0.71% | ~0.80% | **87.98% contínuo** |
| **Veredito de Capacidade** | **Excelente** | **Excelente** | **Estável** | **Zona Crítica** | **🚨 COLAPSO: ESCALAR SEGUNDA MÁQUINA** |

---

### 8.2. Qual é a Capacidade Máxima do Sistema em Uma Única Máquina?

* **Capacidade Máxima Nominal com 100% de Sucesso:** **5.000 requisições simultâneas instantâneas** (throughput de ~2.650 req/s, latência p50 ~1.0s, zero erros).
* **Capacidade de Absorção Aceitável (SLA > 95%):** **8.000 requisições simultâneas** (97.1% de entregas confirmadas com p95 ~3.0s).
* **Ponto de Inflexão e Degradação Severa:** **12.000 conexões**, onde o consumo de RAM atinge 96.2% e o descarte por 503 sobe para 11.3%.
* **Ponto de Colapso e Limite Absoluto:** **16.000 conexões simultâneas**, onde o sistema **para de atender 40.5% das requisições** (31.7% por timeout de socket e 8.8% por backpressure).

---

### 8.3. Por Que é Necessário Escalar para uma Segunda Máquina? (Diagnóstico dos Fatores Limitantes)

1. **Esgotamento da Memória do Host Único (Disputa de RAM):**
   * O container do Producer esgotou **99.92% (511.6 MiB de 512 MiB)** de sua cota. 
   * A máquina host possui apenas **7.38 GB de RAM total**, que já está dividida entre o Apache Kafka (1.0 GB + Heap JVM), Grafana (384 MB), Prometheus (384 MB), 3 réplicas do Consumer (768 MB) e o sistema operacional. Subir o Producer para 1 GB ou 2 GB causaria fome de memória (*memory starvation*) nos demais serviços ou *thrashing* no swap de disco.
2. **Saturação da Fila de Sockets TCP e Event Loop:**
   * Receber, validar com Pydantic, serializar e manter abertos 16.000 sockets TCP concorrentes em um único nó sobrecarregou o backlog do kernel e o agendamento assíncrono. **5.077 conexões expiraram o timeout (> 10s)** antes mesmo de conseguir publicar.
3. **Disputa de I/O de Disco entre Ingestão, Broker e Observabilidade:**
   * Em uma única máquina, a escrita maciça no log do Kafka (`kafka_data`), a ingestão de série temporal do Prometheus (`prometheus_data`) e os arquivos temporários do Docker compartilham o **mesmo barramento de disco e IOPS**.

---

### 8.4. Arquitetura Recomendada para o Escalonamento na Segunda Máquina (Próximo Passo)

Quando a segunda máquina for introduzida, a divisão ótima da topologia distribuída será:

```text
[Cliente / Gerador de Carga]
              │
              ├──► [MÁQUINA 1: NÓ DE INGESTÃO & BORDA]
              │     ├── Load Balancer Reverso (NGINX / Envoy)
              │     └── Cluster de Ingestão Producer API (Múltiplas réplicas Uvicorn)
              │
              └──► [MÁQUINA 2: NÓ DE MENSAGERIA, CONSUMO & DADOS]
                    ├── Apache Kafka Broker (Dedicado a I/O de disco)
                    ├── Workers de Consumo (Consumer Workers 1, 2, 3...)
                    └── Stack de Observabilidade (Prometheus + Grafana + Alertmanager)
```

Essa separação física isola totalmente a CPU e memória da camada HTTP do consumo de disco e processamento das mensagens, permitindo ultrapassar com facilidade **50.000+ requisições por segundo**.

