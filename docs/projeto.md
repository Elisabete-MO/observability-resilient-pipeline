# Análise Técnica e Planejamento do Desafio Prático
## Observabilidade e Pipelines Resilientes em Tempo Real

**Tecnologias:** Apache Kafka, Prometheus, Grafana, Alertmanager, Docker e Python (para benchmark).

**Estimativa:** 16–20 horas de desenvolvimento, com margem de até 23 horas para validação e documentação.

**Equipe:** Viável individualmente ou em dupla.

---

## 1. Avaliação geral

O desafio consiste em implementar um pipeline de processamento assíncrono de eventos, instrumentá-lo com métricas, visualizar seu comportamento e identificar problemas de desempenho e disponibilidade.

O foco principal não é desenvolver uma aplicação web completa, mas demonstrar conhecimentos de:

- Arquitetura orientada a eventos (EDA).
- Comunicação assíncrona com Kafka.
- Instrumentação de aplicações.
- Monitoramento com Prometheus.
- Visualização de métricas no Grafana.
- Alertas e diagnóstico de falhas.
- Benchmark, testes de carga e estresse.

**Complexidade estimada: intermediária.**

A implementação das aplicações é relativamente simples. A maior dificuldade está na integração dos serviços, na configuração das métricas e na validação do comportamento sob carga.

## 2. Arquitetura proposta

```text
Simulador / Benchmark Python
            |
            | HTTP POST
            v
       Producer API
            |
            | Publicação assíncrona
            v
       Apache Kafka
        (raw-events)
            |
            v
       Consumer Worker
            |
            v
    Processamento simulado


Producer ------ /metrics ----+
Consumer ------ /metrics ----+--> Prometheus
Kafka Exporter - /metrics ---+        |
                                      +--> Grafana
                                      |
                                      +--> Alertmanager
```

### Backend

São necessárias duas aplicações principais:

**Producer API**
- Recebe eventos via `POST /api/v1/events`.
- Publica mensagens no tópico `raw-events`.
- Disponibiliza métricas no endpoint `/metrics`.
- Registra quantidade de requisições, publicações, erros e latência.

**Consumer Worker**
- Consome mensagens do Kafka por meio de um Consumer Group.
- Simula processamento com duração variável.
- Registra eventos processados com sucesso ou erro.
- Disponibiliza métricas de processamento, latência e consumer lag.

Não há exigência de autenticação, cadastro de usuários, CRUD ou banco de dados real.

### Frontend

**Não é necessário desenvolver um frontend tradicional** com React, Angular, Flutter ou HTML.

O Grafana será a interface visual, apresentando dashboards com métricas e indicadores do pipeline.

## 3. Infraestrutura e observabilidade

A infraestrutura deverá ser executada via Docker Compose, incluindo os serviços necessários.

**Prometheus**
- Coleta métricas do Producer, Consumer e Kafka Exporter.
- Utiliza intervalo de coleta entre 2 e 5 segundos.
- Permite consultas com PromQL.

**Grafana**

Dashboard provisionado automaticamente, contendo:
- Taxa de ingestão de eventos.
- Latências p50, p95 e p99.
- Consumer lag por partição.
- Taxa de processamento versus taxa de erros.
- Indicadores de disponibilidade.

**Alertmanager**

Dois alertas obrigatórios:

| Alerta | Condição |
|---|---|
| HighConsumerLag | Lag superior a 500 mensagens por mais de 30 segundos |
| HighErrorRate | Taxa de erros do Consumer superior a 5% em uma janela de 2 minutos |

A implementação deve permitir provocar e verificar os dois alertas.

## 4. Benchmark e testes de desempenho

Foi proposta a utilização de **scripts Python** para gerar tráfego controlado e avaliar o comportamento do pipeline.

Não é obrigatório utilizar K6 ou Locust. Python pode atender à necessidade, desde que os scripts controlem adequadamente a carga e produzam medições confiáveis.

### Modalidades de teste

| Modalidade | Objetivo |
|---|---|
| Benchmark | Medir desempenho, throughput, latência e erros |
| Load Test | Avaliar o sistema sob carga esperada |
| Stress Test | Identificar o ponto de degradação ou falha |
| Spike Test | Avaliar o comportamento diante de picos repentinos |

### Exemplo de execução

```bash
python benchmark.py --rps 10 --duration 60
python benchmark.py --rps 100 --duration 60
python benchmark.py --rps 500 --duration 60
python benchmark.py --rps 1000 --duration 60
```

Esses comandos são uma proposta de interface para o script, não uma implementação já existente.

### Funcionalidades sugeridas para o benchmark

- Configuração de requisições por segundo (RPS).
- Configuração da duração do teste.
- Controle de concorrência.
- Geração de eventos válidos e eventos destinados a provocar erros.
- Medição de requisições concluídas e falhas.
- Cálculo de throughput e latências p50, p95 e p99.
- Registro de resultados para comparação entre execuções.

As métricas de Kafka e Consumer deverão ser obtidas da infraestrutura de observabilidade. O benchmark HTTP, isoladamente, não consegue determinar o número de mensagens efetivamente processadas.

### Cenário principal de demonstração

1. Iniciar o pipeline com baixa carga.
2. Executar o benchmark com 10 eventos/s.
3. Aumentar progressivamente para 100, 500 e 1.000 eventos/s.
4. Acompanhar as taxas de ingestão e processamento no Grafana.
5. Identificar quando o Consumer deixa de acompanhar o Producer.
6. Observar o crescimento do Consumer Lag.
7. Verificar o disparo do alerta HighConsumerLag.
8. Aumentar o número de Consumers, respeitando a quantidade de partições disponíveis.
9. Observar a redistribuição do processamento e o comportamento do lag.

Também deverá existir um cenário controlado para provocar erros de processamento e verificar o HighErrorRate.

**Objetivo:** identificar gargalos, quantificar o desempenho e demonstrar a capacidade de diagnóstico do sistema.

Os valores de RPS são cargas experimentais propostas, não capacidades previamente conhecidas do ambiente.

## 5. Estimativa de desenvolvimento

| Atividade | Estimativa |
|---|---:|
| Estrutura do projeto, Docker e Kafka | 2–3 h |
| Producer API | 2 h |
| Consumer Worker | 2 h |
| Instrumentação com métricas | 2–3 h |
| Kafka Exporter e Prometheus | 1,5–2 h |
| Grafana, PromQL e dashboards | 3–4 h |
| Alertmanager e regras | 1,5–2 h |
| Benchmark e simulações de falha | 1–2 h |
| Documentação e validação final | 2–3 h |

**Planejamento recomendado: aproximadamente 20 horas.**

As estimativas consideram familiaridade básica com programação, Docker e os conceitos estudados de Kafka e Prometheus, além de apoio de IA durante o desenvolvimento.

São estimativas de planejamento, não garantias de conclusão.

## 6. Cronograma sugerido

| Dia | Carga | Entrega |
|---|---|---|
| 1 | 4–5 h | Kafka, Producer e Consumer funcionando |
| 2 | 4–5 h | Métricas e coleta pelo Prometheus |
| 3 | 4–5 h | Dashboard Grafana e alertas |
| 4 | 4–5 h | Benchmark, simulações, documentação e defesa |

**Alternativas:**
- 3 dias: aproximadamente 6 horas/dia, com menor margem para imprevistos.
- 4 dias: 4–5 horas/dia, planejamento recomendado.
- 5 dias: 3–4 horas/dia, execução mais confortável.

## 7. Necessidade de equipe e divisão de trabalho

O escopo não exige uma equipe grande nem profissionais separados para frontend e backend.

**Uma pessoa:** execução viável, especialmente com apoio de ferramentas de IA.

**Duas pessoas:** divisão recomendada para acelerar desenvolvimento, revisão e testes.

Uma possível distribuição:

| Pessoa A | Pessoa B |
|---|---|
| Producer API | Docker Compose e Kafka |
| Consumer Worker | Prometheus e Kafka Exporter |
| Instrumentação das aplicações | Grafana e Alertmanager |
| Benchmark Python | Testes e validação dos alertas |

A integração, os testes finais e a preparação para a defesa devem ser realizados conjuntamente.

Essa distribuição é apenas uma sugestão: o ideal é que ambos compreendam o fluxo completo.

## 8. Critérios de avaliação

Segundo o documento do desafio:

| Critério | Peso |
|---|---:|
| Instrumentação das métricas | 30% |
| Infraestrutura via Docker Compose | 20% |
| Dashboards Grafana | 20% |
| Alertas | 15% |
| Diagnóstico de falha durante a defesa | 15% |

**Ponto de atenção:** a avaliação não depende apenas de entregar os serviços funcionando.

Será necessário demonstrar entendimento das métricas e capacidade de identificar a causa raiz de uma falha simulada.

Portanto, é importante não delegar toda a implementação à IA sem compreender as decisões técnicas.

## 9. Riscos e dificuldades

Os principais riscos identificados são:

- Problemas de comunicação entre containers.
- Falhas de configuração do Kafka e dos Consumer Groups.
- Métricas não coletadas corretamente pelo Prometheus.
- Cálculos incorretos de latência ou consumer lag.
- Consultas PromQL que não retornam os dados esperados.
- Alertas que não disparam durante as simulações.
- Benchmark gerando carga insuficiente ou resultados pouco confiáveis.
- Ambiente funcionando parcialmente, mas não inicializando corretamente do zero.

A integração e a validação devem receber atenção especial, pois podem consumir mais tempo que a implementação do código.

## 10. Conclusão

**O desafio é tecnicamente viável para uma dupla, sem necessidade de desenvolver frontend próprio ou um backend complexo.**

A maior parte do esforço estará na configuração da infraestrutura, instrumentação, visualização e interpretação das métricas.

A implementação de um benchmark em Python é uma boa estratégia para gerar cenários reproduzíveis, demonstrar degradação de desempenho e validar os alertas.

A recomendação é trabalhar em etapas incrementais, garantindo que cada componente esteja funcional antes de adicionar o próximo.

**Estimativa final:** aproximadamente 20 horas de trabalho, distribuídas em quatro dias de 4–5 horas, com possibilidade de execução em dupla.

**Resultado esperado:** um pipeline distribuído funcional e observável, capaz de demonstrar, sob cargas controladas, seu desempenho, seus gargalos e seu comportamento diante de falhas.