# Contrato de eventos — proposta para validação

O enunciado não define o formato do payload. Este contrato é **proposto**, não obrigatório.

```json
{
  "event_id": "7e8cb1a7-3a4f-45dc-a17d-bbaf0a8dc8d2",
  "event_type": "order.created",
  "occurred_at": "2026-10-27T20:00:00Z",
  "payload": {"order_id": "demo-123", "amount": 149.9}
}
```

## Validação proposta
- `event_id`: UUID; usado para correlação em logs, nunca como label Prometheus.
- `event_type`: string controlada.
- `occurred_at`: timestamp UTC ISO 8601.
- `payload`: objeto JSON com limite de tamanho a definir.

## Respostas HTTP
- `202 Accepted`: Retornado após confirmação da persistência no broker Kafka (`acks=all`), contendo `status: "published"`, `event_id`, `topic`, `partition` e `offset`.
- `413 Payload Too Large`: Rejeição antecipada de requisições com `Content-Length > 64 KiB`.
- `422 Unprocessable Entity`: Falha na validação de schema Pydantic ou JSON inválido.
- `503 Service Unavailable`: Falha na comunicação com o Kafka ou semáforo de concorrência lotado (backpressure).
- `504 Gateway Timeout`: Timeout na confirmação de entrega do Kafka (> 2.0s).
