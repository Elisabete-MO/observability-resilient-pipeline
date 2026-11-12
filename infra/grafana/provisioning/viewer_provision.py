#!/usr/bin/env python3
"""Provisiona o usuario Viewer do Grafana via Admin HTTP API (idempotente).

O script e executado uma unica vez pelo servico `grafana-viewer-provisioner`.
Ele nao registra senhas e nao altera a senha de um usuario ja existente.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_GRAFANA_URL = "http://grafana:3000"
VIEWER_ROLE = "Viewer"
HTTP_TIMEOUT_SECONDS = 10


class ProvisionError(Exception):
    """Falha controlada de provisionamento (mensagem segura para log)."""


def redact(text: object, secrets: list[str]) -> str:
    """Remove qualquer segredo conhecido de uma mensagem antes de logar."""
    message = str(text)
    for secret in secrets:
        if secret:
            message = message.replace(secret, "***")
    return message


def log(message: str) -> None:
    print(f"[viewer-provision] {message}", flush=True)


def load_config(environ: dict[str, str]) -> dict[str, object]:
    """Le e valida as variaveis obrigatorias sem usar valores padrao inseguros."""

    def required(name: str) -> str:
        value = environ.get(name, "").strip()
        if not value:
            raise ProvisionError(
                f"variavel obrigatoria ausente ou vazia: {name}. "
                "Defina-a no arquivo .env local antes de subir o ambiente."
            )
        return value

    admin_user = required("GF_ADMIN_USER")
    admin_password = required("GF_ADMIN_PASSWORD")
    viewer_password = required("GF_VIEWER_PASSWORD")

    try:
        org_id = int(environ.get("GF_VIEWER_ORG_ID", "1"))
        ready_timeout = int(environ.get("GRAFANA_READY_TIMEOUT_SECONDS", "180"))
    except ValueError as exc:
        raise ProvisionError(
            "GF_VIEWER_ORG_ID e GRAFANA_READY_TIMEOUT_SECONDS devem ser inteiros."
        ) from exc

    if org_id <= 0:
        raise ProvisionError("GF_VIEWER_ORG_ID deve ser um inteiro positivo.")
    if ready_timeout <= 0:
        raise ProvisionError(
            "GRAFANA_READY_TIMEOUT_SECONDS deve ser um inteiro positivo."
        )

    return {
        "grafana_url": environ.get("GRAFANA_URL", DEFAULT_GRAFANA_URL).rstrip("/"),
        "admin_user": admin_user,
        "admin_password": admin_password,
        "viewer_login": environ.get("GF_VIEWER_LOGIN", "viewer").strip() or "viewer",
        "viewer_name": environ.get("GF_VIEWER_NAME", "viewer").strip() or "viewer",
        "viewer_email": environ.get("GF_VIEWER_EMAIL", "viewer@localhost.local").strip()
        or "viewer@localhost.local",
        "viewer_password": viewer_password,
        "org_id": org_id,
        "ready_timeout": ready_timeout,
    }


def http_json(
    base_url: str,
    method: str,
    path: str,
    admin_user: str,
    admin_password: str,
    payload: dict[str, object] | None = None,
) -> tuple[int, dict[str, object]]:
    """Executa uma chamada HTTP e devolve (status, corpo JSON)."""
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(f"{base_url}{path}", data=data, method=method)
    token = base64.b64encode(
        f"{admin_user}:{admin_password}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    request.add_header("Accept", "application/json")
    request.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, _parse_body(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, _parse_body(body)
    except OSError as exc:
        reason = getattr(exc, "reason", exc)
        raise ProvisionError(f"nao foi possivel conectar ao Grafana: {reason}") from exc


def _parse_body(body: str) -> dict[str, object]:
    if not body.strip():
        return {}
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return {"message": body.strip()[:300]}
    return parsed if isinstance(parsed, dict) else {"data": parsed}


def wait_until_ready(config: dict[str, object], secrets: list[str]) -> None:
    """Aguarda /api/health responder ate o timeout configurado."""
    deadline = time.monotonic() + int(config["ready_timeout"])
    last_error = "sem resposta"

    while time.monotonic() < deadline:
        try:
            status, body = http_json(
                str(config["grafana_url"]),
                "GET",
                "/api/health",
                str(config["admin_user"]),
                str(config["admin_password"]),
            )
        except ProvisionError as exc:
            last_error = redact(exc, secrets)
        else:
            if status == 200:
                log("Grafana pronto para provisionamento.")
                return
            last_error = f"HTTP {status}: {redact(body.get('message', ''), secrets)}"

        time.sleep(2)

    raise ProvisionError(
        "Grafana nao ficou pronto dentro do tempo limite "
        f"({config['ready_timeout']}s). Ultimo erro: {last_error}"
    )


def lookup_global_user(
    config: dict[str, object], secrets: list[str]
) -> dict[str, object] | None:
    login = urllib.parse.quote(str(config["viewer_login"]), safe="")
    status, body = http_json(
        str(config["grafana_url"]),
        "GET",
        f"/api/users/lookup?loginOrEmail={login}",
        str(config["admin_user"]),
        str(config["admin_password"]),
    )
    if status == 200:
        return body
    if status == 404:
        return None
    if status in (401, 403):
        raise ProvisionError(
            "credenciais administrativas invalidas (HTTP "
            f"{status}). Verifique GF_ADMIN_USER/GF_ADMIN_PASSWORD no .env; "
            "a senha do administrador nao sera alterada."
        )
    raise ProvisionError(
        f"falha ao consultar usuario '{config['viewer_login']}' "
        f"(HTTP {status}): {redact(body.get('message', ''), secrets)}"
    )


def create_viewer(
    config: dict[str, object], secrets: list[str]
) -> dict[str, object]:
    log(f"Usuario '{config['viewer_login']}' nao existe; criando na organizacao {config['org_id']}.")
    status, body = http_json(
        str(config["grafana_url"]),
        "POST",
        "/api/admin/users",
        str(config["admin_user"]),
        str(config["admin_password"]),
        {
            "name": config["viewer_name"],
            "email": config["viewer_email"],
            "login": config["viewer_login"],
            "password": config["viewer_password"],
            "OrgId": config["org_id"],
        },
    )
    if status not in (200, 201):
        raise ProvisionError(
            f"falha ao criar usuario '{config['viewer_login']}' "
            f"(HTTP {status}): {redact(body.get('message', ''), secrets)}"
        )
    log("Usuario criado; a senha informada nao sera exibida.")
    return body


def get_org_user(
    config: dict[str, object], secrets: list[str], user_id: int
) -> dict[str, object] | None:
    """Retorna o vinculo do usuario com a organizacao, ou None se ausente.

    O endpoint `/api/orgs/{org}/users/lookup` pode retornar 404 para usuarios
    que existem na organizacao (observado no Grafana 12.1.0). Por isso a fonte
    de verdade e a lista de organizacoes do usuario.
    """
    org_id = int(config["org_id"])
    status, body = http_json(
        str(config["grafana_url"]),
        "GET",
        f"/api/users/{user_id}/orgs",
        str(config["admin_user"]),
        str(config["admin_password"]),
    )
    if status == 200:
        orgs = body if isinstance(body, list) else body.get("data", [])
        for org in orgs if isinstance(orgs, list) else []:
            if isinstance(org, dict) and int(org.get("orgId", 0)) == org_id:
                return org
        return None
    if status == 404:
        return None
    raise ProvisionError(
        f"falha ao consultar organizacoes do usuario {user_id} "
        f"(HTTP {status}): {redact(body.get('message', ''), secrets)}"
    )


def ensure_org_membership(
    config: dict[str, object], secrets: list[str], user_id: int
) -> dict[str, object]:
    org_user = get_org_user(config, secrets, user_id)
    if org_user is None:
        log(f"Adicionando usuario a organizacao {config['org_id']} como {VIEWER_ROLE}.")
        status, body = http_json(
            str(config["grafana_url"]),
            "POST",
            f"/api/orgs/{config['org_id']}/users",
            str(config["admin_user"]),
            str(config["admin_password"]),
            {"loginOrEmail": config["viewer_login"], "role": VIEWER_ROLE},
        )
        if status not in (200, 201):
            raise ProvisionError(
                "falha ao adicionar usuario a organizacao "
                f"{config['org_id']} (HTTP {status}): "
                f"{redact(body.get('message', ''), secrets)}"
            )
        org_user = get_org_user(config, secrets, user_id)
        if org_user is None:
            raise ProvisionError(
                "usuario nao encontrado na organizacao apos o vinculo."
            )

    return org_user


def enforce_viewer_role(
    config: dict[str, object], secrets: list[str], user_id: int
) -> dict[str, object]:
    org_user = ensure_org_membership(config, secrets, user_id)
    current_role = str(org_user.get("role", ""))

    if current_role == VIEWER_ROLE:
        log(f"Papel ja esta correto: {VIEWER_ROLE}.")
        return org_user

    log(f"Ajustando papel de '{current_role}' para {VIEWER_ROLE} sem alterar a senha.")
    status, body = http_json(
        str(config["grafana_url"]),
        "PUT",
        f"/api/orgs/{config['org_id']}/users/{user_id}",
        str(config["admin_user"]),
        str(config["admin_password"]),
        {"role": VIEWER_ROLE},
    )
    if status not in (200, 201):
        raise ProvisionError(
            f"falha ao atribuir papel {VIEWER_ROLE} (HTTP {status}): "
            f"{redact(body.get('message', ''), secrets)}"
        )

    return ensure_org_membership(config, secrets, user_id)


def run(config: dict[str, object]) -> None:
    secrets = [
        str(config["admin_password"]),
        str(config["viewer_password"]),
    ]

    wait_until_ready(config, secrets)

    user = lookup_global_user(config, secrets)
    if user is None:
        create_viewer(config, secrets)
        user = lookup_global_user(config, secrets)
        if user is None:
            raise ProvisionError(
                "usuario foi criado, mas nao foi possivel confirma-lo na API."
            )
        user_id = int(user.get("id", 0))
        if user_id <= 0:
            raise ProvisionError(
                "usuario foi criado, mas a API nao retornou um identificador valido."
            )
    else:
        log(
            f"Usuario '{config['viewer_login']}' ja existe (id={user.get('id')}); "
            "a senha existente sera preservada."
        )
        user_id = int(user.get("id", 0))
        if user_id <= 0:
            raise ProvisionError(
                "usuario existente sem identificador valido; nada foi alterado."
            )

    if bool(user.get("isGrafanaAdmin")) or bool(user.get("isAdmin")):
        raise ProvisionError(
            "o usuario Viewer existente possui privilegios de administrador do "
            "servidor. Revise-o manualmente; nenhuma senha foi alterada."
        )

    org_user = enforce_viewer_role(config, secrets, user_id)
    final_role = str(org_user.get("role", ""))
    if final_role != VIEWER_ROLE:
        raise ProvisionError(
            f"verificacao final falhou: papel atual '{final_role}', esperado {VIEWER_ROLE}."
        )

    log(
        f"Provisionamento concluido: usuario '{config['viewer_login']}' "
        f"na organizacao {config['org_id']} com papel {final_role}."
    )


def main() -> int:
    try:
        config = load_config(dict(os.environ))
        run(config)
    except ProvisionError as exc:
        secrets = [
            os.environ.get("GF_ADMIN_PASSWORD", ""),
            os.environ.get("GF_VIEWER_PASSWORD", ""),
        ]
        print(f"[viewer-provision] ERRO: {redact(exc, secrets)}", file=sys.stderr)
        return 1
    except Exception as exc:  # pragma: no cover - ultima barreira de seguranca
        secrets = [
            os.environ.get("GF_ADMIN_PASSWORD", ""),
            os.environ.get("GF_VIEWER_PASSWORD", ""),
        ]
        print(
            f"[viewer-provision] ERRO inesperado: {redact(exc, secrets)}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
