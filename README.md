# Observability & Resilient Pipeline

Pipeline orientado a eventos que demonstra, de ponta a ponta, ingestão,
processamento e observabilidade. A API FastAPI recebe eventos, publica no Apache
Kafka e o consumer processa a fila. O Prometheus coleta as métricas, o Grafana
exibe os dashboards e o Alertmanager concentra os alertas.

O ponto de entrada do projeto é a **Central de Acesso**, uma página local gerada
pelo script de inicialização. É por ela que você chega ao Grafana, ao Prometheus,
ao Alertmanager e à documentação da API.

## Pré-requisitos

* Windows com PowerShell 5.1+.
* Docker Desktop aberto e com o engine em execução (Docker Compose v2).
* Repositório clonado, com os comandos executados na raiz e na branch
  `Development`.
* Python 3.10+ apenas para o teste de carga opcional. A pilha sobe sem ele.

## Iniciar o projeto

Na raiz do repositório, execute:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start-pipeline.ps1 -OpenBrowser
```

Na primeira execução, ou se alguma credencial estiver faltando, o script cria o
`.env` local e solicita:

* `GF_ADMIN_USER` — usuário administrador do Grafana;
* `GF_ADMIN_PASSWORD` — senha desse administrador;
* `GF_VIEWER_PASSWORD` — senha do usuário de demonstração `viewer`.

As senhas são digitadas com entrada oculta e confirmação, não aparecem no
histórico de comandos e são gravadas apenas no `.env` local, que é ignorado pelo
Git. Não há credenciais padrão no repositório.

Depois de subir os serviços e verificar a saúde do ambiente, o script abre
automaticamente a **Central de Acesso**. A primeira execução pode levar alguns
minutos, enquanto o Kafka e os demais serviços iniciam. Se o terminal indicar
pendências, a própria central mostra o estado dos serviços e o guia técnico
explica como diagnosticar.

## Central de Acesso e principais ferramentas

A Central é um relatório local da inicialização, com o resumo das verificações,
o estado dos seis targets e atalhos para as ferramentas. Ela não substitui o
Grafana: funciona como a porta de entrada do ambiente.

| Ferramenta | Para que serve | Endereço |
|---|---|---|
| **Central de Acesso** | Ponto de partida e estado da inicialização | Aberta pelo script |
| **Grafana** | Dashboard do pipeline, com taxa, latência, erros, lag e alertas | http://localhost:3000 |
| **Prometheus** | Consultas e verificação dos targets | http://localhost:9090 |
| **Alertmanager** | Alertas ativos e silenciamentos | http://localhost:9093 |
| **FastAPI** | Documentação interativa da API de ingestão | http://localhost:8000/docs |

## Login no Grafana

* **Administrador:** use `GF_ADMIN_USER` e `GF_ADMIN_PASSWORD`, definidos na
  primeira execução.
* **Viewer:** usuário `viewer`, com papel `Viewer` na organização 1. Ele pode
  visualizar dashboards, mas não editar nem acessar configurações
  administrativas. A senha é `GF_VIEWER_PASSWORD`, uma credencial compartilhada
  de demonstração definida localmente.

As contas ficam no volume `grafana_data`. Em uma instalação já existente,
alterar o `.env` não redefine senhas já gravadas. O guia técnico descreve a
persistência e a limpeza do ambiente.

## Como observar o pipeline

1. Abra a Central de Acesso e escolha **Grafana** para entrar no dashboard.
2. No **Prometheus**, confirme em `/targets` que os alvos estão `UP`.
3. Opcionalmente, gere carga com o teste simples abaixo.
4. Acompanhe no dashboard a taxa de requisições, a latência, os erros, o lag do
   consumer e o estado dos alertas. Use o **Alertmanager** para ver os alertas
   ativos.

## Teste simples e opcional

Com a pilha em execução e Python 3.10+ instalado no host, execute na raiz:

```powershell
python --version
python benchmarks/load_test.py --scenario load --rate 5 --duration 20 --workers 10
```

Esse teste envia eventos válidos e não provoca falhas intencionais. O relatório
distingue requisições planejadas, submetidas e confirmadas; nem toda requisição
planejada chega a ser enviada. Para cenários de lag e de erros, consulte o guia
técnico.

## Encerrar o ambiente

```powershell
docker compose stop
```

Esse comando preserva os volumes, incluindo usuários e histórico. Para iniciar
novamente, execute o mesmo comando de inicialização apresentado acima.

## Documentação da equipe

O [guia técnico da equipe](docs/guia-execucao-observability.md) detalha o
funcionamento do script, a Central de Acesso, o provisionamento do `viewer`,
diagnóstico, testes automatizados, testes de carga, cenários de lag e erros,
limitações e limpeza destrutiva.

> **Estado de validação (12/11/2026):** a suíte unitária tem 55 testes
> aprovados. O provisionamento do Grafana e os dois alertas foram validados
> manualmente em Docker. Os scripts de cenário em Bash ainda não foram
> executados de ponta a ponta no host da apresentação.
