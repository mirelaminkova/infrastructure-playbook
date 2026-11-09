#!/usr/bin/env python3
"""Create the central-relay accounts of merged BYOC Pulsars.

The usegalaxy.eu relay (stargate.galaxyproject.eu, role
usegalaxy-eu/ansible-pulsar-relay) only creates its bootstrap admin; every
other account is made through the relay's admin API. For each BYOC runner
in ``job_conf.yml`` that uses the central relay (``relay_url`` is
``{{ pulsar_byoc_central_relay_url }}``), this script makes sure the relay
has an active account ``byoc_<user id>`` with permissions read+write and
the password from ``group_vars/pulsar_relay/vault.yml``. Accounts named
``byoc_<hex>`` that no longer have a runner are deactivated (not deleted,
so a re-enabled user keeps their topics).

Run it after a BYOC PR is merged (workflow byoc-relay-accounts.yml), with a
relay admin account in ``BYOC_RELAY_ADMIN_USERNAME`` /
``BYOC_RELAY_ADMIN_PASSWORD``. ``--dry-run`` only prints what it would do.
Passwords are never printed.

pulsar-relay API used (pulsar_relay/api/auth.py at 048689a): POST
/auth/login (OAuth2 password form, 5/min per IP), GET /auth/users, POST
/auth/register, PATCH /auth/users/{user_id}; all but login need "admin".
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

import byoc_sync as bs
import yaml

MANAGED_ACCOUNT_RE = re.compile(r"byoc_[0-9a-f]{16}|byoc_[0-9a-f]{32}")
PERMISSIONS = ["read", "write"]
DEFAULT_RELAY_URL = "https://" + bs.CENTRAL_RELAY_HOST


def central_slugs(job_conf: str) -> list:
    """Return the slugs whose generated runner uses the central relay."""
    marker = f'relay_url: "{{{{ {bs.CENTRAL_RELAY_VAR} }}}}"'
    return [
        slug
        for slug in bs.managed_slugs(job_conf)
        if marker in (bs.block_text(job_conf, slug) or "")
    ]


def desired_accounts(root: Path, vault: bs.AnsibleVault) -> dict:
    """Return ``{username: password}`` for every central BYOC runner."""
    slugs = central_slugs((root / bs.JOB_CONF).read_text())
    if not slugs:
        return {}
    values = (
        yaml.load(  # noqa: S506 - _UnsafeLoader only adds !unsafe
            vault.read(root / bs.RELAY_VAULT), Loader=bs._UnsafeLoader
        )
        or {}
    )
    accounts = {}
    for slug in slugs:
        username = values.get(bs.relay_username_var(slug))
        password = values.get(bs.relay_password_var(slug))
        if username != slug or not isinstance(password, str):
            raise bs.ByocError(
                f"{bs.RELAY_VAULT} has no usable credentials for {slug}"
            )
        if not bs.RELAY_PASSWORD_RE.fullmatch(password):
            raise bs.ByocError(f"relay password of {slug} breaks the rules")
        accounts[username] = password
    return accounts


class RelayAdmin:
    """Minimal client for the pulsar-relay user admin API."""

    def __init__(self, url: str, session: Any = None):
        """Talk to the relay at ``url`` (https only)."""
        if not url.startswith("https://"):
            raise bs.ByocError("the relay URL must be https")
        if session is None:
            import requests

            session = requests.Session()
        self.url = url.rstrip("/")
        self.session = session

    def _call(self, method: str, path: str, **kw: Any) -> Any:
        response = self.session.request(
            method, self.url + path, timeout=60, **kw
        )
        if response.status_code not in (200, 201, 204):
            # the relay's error detail never echoes a password
            raise bs.ByocError(f"{method} {path}: HTTP {response.status_code}")
        return response.json() if response.status_code != 204 else None

    def login(self, username: str, password: str) -> None:
        """Log in as the admin; one login per run (rate limit 5/min)."""
        token = self._call(
            "POST",
            "/auth/login",
            data={"username": username, "password": password},
        )["access_token"]
        self.session.headers["Authorization"] = f"Bearer {token}"

    def users(self) -> list:
        """Return every account on the relay, without password hashes."""
        return self._call("GET", "/auth/users")

    def register(self, username: str, password: str) -> None:
        """Create an account with read+write permissions."""
        self._call(
            "POST",
            "/auth/register",
            json={
                "username": username,
                "password": password,
                "permissions": PERMISSIONS,
            },
        )

    def update(self, user_id: str, **fields: Any) -> None:
        """Patch an account (password, permissions, is_active)."""
        self._call("PATCH", f"/auth/users/{user_id}", json=fields)


def reconcile(
    admin: RelayAdmin, accounts: dict, dry_run: bool, out: Any = sys.stdout
) -> list:
    """Bring the relay's byoc_* accounts in line with ``accounts``.

    Returns ``[(username, action)]`` with action create, update or
    deactivate. Existing accounts get their password set every run because
    the API cannot tell whether it is already right; that is idempotent.
    Accounts not named ``byoc_<hex>`` are never touched.
    """
    existing = {
        u["username"]: u
        for u in admin.users()
        if MANAGED_ACCOUNT_RE.fullmatch(str(u.get("username", "")))
    }
    actions = []
    for username, password in sorted(accounts.items()):
        user = existing.get(username)
        if user is None:
            actions.append((username, "create"))
            if not dry_run:
                admin.register(username, password)
        else:
            actions.append((username, "update"))
            if not dry_run:
                admin.update(
                    user["user_id"],
                    password=password,
                    permissions=PERMISSIONS,
                    is_active=True,
                )
    for username, user in sorted(existing.items()):
        if username not in accounts and user.get("is_active", True):
            actions.append((username, "deactivate"))
            if not dry_run:
                admin.update(user["user_id"], is_active=False)
    for username, action in actions:
        prefix = "would " if dry_run else ""
        print(f"{username}: {prefix}{action}", file=out)
    return actions


def main(argv: Optional[list] = None) -> int:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--root", default=".", help="playbook checkout")
    parser.add_argument(
        "--relay-url",
        default=os.environ.get("BYOC_RELAY_URL", DEFAULT_RELAY_URL),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    password_file = os.environ.get("ANSIBLE_VAULT_PASSWORD_FILE")
    admin_user = os.environ.get("BYOC_RELAY_ADMIN_USERNAME")
    admin_password = os.environ.get("BYOC_RELAY_ADMIN_PASSWORD")
    if not password_file:
        parser.error("ANSIBLE_VAULT_PASSWORD_FILE must be set")
    if not (admin_user and admin_password):
        parser.error("BYOC_RELAY_ADMIN_USERNAME/_PASSWORD must be set")
    try:
        accounts = desired_accounts(
            Path(args.root).resolve(), bs.AnsibleVault(password_file)
        )
        admin = RelayAdmin(args.relay_url)
        admin.login(admin_user, admin_password)
        reconcile(admin, accounts, args.dry_run)
    except bs.ByocError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
