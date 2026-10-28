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
Definir antes de implementar: `202 Accepted` pode significar aceitação para publicação assíncrona, **não** confirmação de persistência no Kafka. A confirmação de publicação deve ser medida separadamente. Definir resposta para payload inválido, indisponibilidade do Kafka e timeout.
