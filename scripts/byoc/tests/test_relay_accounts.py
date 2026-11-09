"""Central relay accounts: byoc_relay_accounts.py against a fake relay."""

import io

import byoc_relay_accounts as ra
import byoc_sync as bs
import pytest
from conftest import MQ_USER, RELAY_USER

ADMIN = ("byoc-admin", "fake-admin-password")


class FakeResponse:
    """A requests-like response."""

    def __init__(self, status, payload=None):
        """Return ``payload`` with ``status``."""
        self.status_code = status
        self._payload = payload

    def json(self):
        """Return the payload."""
        return self._payload


class FakeRelay:
    """Just enough of pulsar-relay's /auth API, with its admin checks."""

    def __init__(self, users=()):
        """Start with the given users (dicts with username, password)."""
        self.headers = {}
        self.users = {}
        self.calls = []
        self.logins = 0
        for user in [
            {"username": ADMIN[0], "password": ADMIN[1], "perms": ["admin"]},
            *users,
        ]:
            self._add(user["username"], user["password"], user.get("perms"))

    def _add(self, username, password, perms=None, active=True):
        user_id = f"u{len(self.users)}"
        self.users[user_id] = {
            "user_id": user_id,
            "username": username,
            "password": password,
            "permissions": perms or ["read", "write"],
            "is_active": active,
        }

    def by_name(self, username):
        """Return the stored user called ``username``."""
        return next(
            u for u in self.users.values() if u["username"] == username
        )

    def request(self, method, url, timeout, data=None, json=None):
        """Dispatch like the relay's FastAPI routes."""
        assert url.startswith("https://stargate.galaxyproject.eu/auth/")
        path = url.split("/auth", 1)[1]
        self.calls.append((method, path))
        if path == "/login":
            self.logins += 1
            user = self.by_name(data["username"])
            if user["password"] != data["password"]:
                return FakeResponse(401)
            return FakeResponse(
                200, {"access_token": "tok-" + user["user_id"]}
            )
        if self.headers.get("Authorization") != "Bearer tok-u0":
            return FakeResponse(403)
        if method == "GET" and path == "/users":
            return FakeResponse(
                200,
                [
                    {k: v for k, v in u.items() if k != "password"}
                    for u in self.users.values()
                ],
            )
        if method == "POST" and path == "/register":
            if any(
                u["username"] == json["username"] for u in self.users.values()
            ):
                return FakeResponse(400)
            self._add(json["username"], json["password"], json["permissions"])
            return FakeResponse(201, {})
        if method == "PATCH" and path.startswith("/users/"):
            user = self.users[path.split("/")[-1]]
            if "password" in json:
                user["password"] = json["password"]
            if "permissions" in json:
                user["permissions"] = json["permissions"]
            if "is_active" in json:
                user["is_active"] = json["is_active"]
            return FakeResponse(200, {})
        return FakeResponse(404)


def admin_for(relay):
    """Return a logged-in RelayAdmin talking to ``relay``."""
    admin = ra.RelayAdmin("https://stargate.galaxyproject.eu/", session=relay)
    admin.login(*ADMIN)
    return admin


def merge(root, vault, user, prefs):
    """Apply a user's change as if the BYOC PR had been merged."""
    cfg = bs.validate(user, prefs)
    bs.apply_change(root, bs.plan_change(root, cfg.slug, cfg, vault), vault)
    return cfg


def test_only_central_runners_get_accounts(
    checkout, vault, central_prefs, relay_prefs, mq_prefs
):
    """Own-relay, MQ and NEMO runners never become central accounts."""
    cfg = merge(checkout, vault, RELAY_USER, central_prefs)
    merge(checkout, vault, MQ_USER, relay_prefs)
    merge(checkout, vault, "00000000000000aa", mq_prefs)
    assert ra.central_slugs((checkout / bs.JOB_CONF).read_text()) == [cfg.slug]
    assert ra.desired_accounts(checkout, vault) == {
        cfg.slug: central_prefs["relay_password"]
    }


def test_create_then_idempotent(checkout, vault, central_prefs):
    """A merged user gets an account; a second run changes nothing."""
    cfg = merge(checkout, vault, RELAY_USER, central_prefs)
    relay = FakeRelay()
    accounts = ra.desired_accounts(checkout, vault)
    out = io.StringIO()
    assert ra.reconcile(admin_for(relay), accounts, False, out) == [
        (cfg.slug, "create")
    ]
    user = relay.by_name(cfg.slug)
    assert user["password"] == central_prefs["relay_password"]
    assert user["permissions"] == ["read", "write"]
    snapshot = {k: dict(v) for k, v in relay.users.items()}
    ra.reconcile(admin_for(relay), accounts, False, io.StringIO())
    assert relay.users == snapshot
    assert central_prefs["relay_password"] not in out.getvalue()


def test_password_change_and_removal(checkout, vault, central_prefs):
    """A new password is set; a removed user's account is deactivated."""
    cfg = merge(checkout, vault, RELAY_USER, central_prefs)
    relay = FakeRelay(
        [{"username": cfg.slug, "password": "old-fake-password-000"}]
    )
    ra.reconcile(admin_for(relay), ra.desired_accounts(checkout, vault), False)
    assert (
        relay.by_name(cfg.slug)["password"] == central_prefs["relay_password"]
    )
    change = bs.plan_change(checkout, cfg.slug, None, vault)
    bs.apply_change(checkout, change, vault)
    assert ra.desired_accounts(checkout, vault) == {}
    actions = ra.reconcile(admin_for(relay), {}, False, io.StringIO())
    assert actions == [(cfg.slug, "deactivate")]
    assert relay.by_name(cfg.slug)["is_active"] is False
    # already inactive: nothing to do
    assert ra.reconcile(admin_for(relay), {}, False, io.StringIO()) == []


def test_other_accounts_never_touched(checkout, vault, central_prefs):
    """NEMO, admin and look-alike accounts are left alone."""
    merge(checkout, vault, RELAY_USER, central_prefs)
    others = [
        {"username": "nemo", "password": "nemo-fake-password"},
        {"username": "byoc_notahexid", "password": "x" * 16},
        {"username": "byoc_" + MQ_USER + "x", "password": "y" * 16},
    ]
    relay = FakeRelay(others)
    before = {u["username"]: dict(u) for u in relay.users.values()}
    ra.reconcile(
        admin_for(relay),
        ra.desired_accounts(checkout, vault),
        False,
        io.StringIO(),
    )
    for name in ["nemo", ADMIN[0], "byoc_notahexid", "byoc_" + MQ_USER + "x"]:
        assert relay.by_name(name) == before[name]


def test_dry_run_changes_nothing(checkout, vault, central_prefs):
    """--dry-run only lists actions and logs in once."""
    cfg = merge(checkout, vault, RELAY_USER, central_prefs)
    relay = FakeRelay([{"username": "byoc_" + MQ_USER, "password": "z" * 16}])
    out = io.StringIO()
    actions = ra.reconcile(
        admin_for(relay), ra.desired_accounts(checkout, vault), True, out
    )
    assert actions == [(cfg.slug, "create"), ("byoc_" + MQ_USER, "deactivate")]
    assert [m for m, _ in relay.calls] == ["POST", "GET"]
    assert relay.logins == 1
    assert "would create" in out.getvalue()


def test_bad_admin_credentials_abort(checkout, vault, central_prefs):
    """A failed admin login or a non-admin token is an error."""
    relay = FakeRelay()
    admin = ra.RelayAdmin("https://stargate.galaxyproject.eu", session=relay)
    with pytest.raises(bs.ByocError, match="HTTP 401"):
        admin.login(ADMIN[0], "wrong")
    with pytest.raises(bs.ByocError, match="HTTP 403"):
        admin.users()


def test_vault_mismatch_aborts(checkout, vault, central_prefs):
    """A runner without matching vault credentials is never provisioned."""
    cfg = merge(checkout, vault, RELAY_USER, central_prefs)
    plaintext = bs.set_vault_vars(
        vault.read(checkout / bs.RELAY_VAULT),
        {bs.relay_username_var(cfg.slug): "nemo"},
    )
    vault.write(checkout / bs.RELAY_VAULT, plaintext)
    with pytest.raises(bs.ByocError, match="no usable credentials"):
        ra.desired_accounts(checkout, vault)


def test_https_only():
    """The admin password never goes over plain http."""
    with pytest.raises(bs.ByocError, match="https"):
        ra.RelayAdmin("http://stargate.galaxyproject.eu", session=FakeRelay())


def test_main_requires_admin_credentials(monkeypatch, tmp_path):
    """Without relay admin credentials the script refuses to start."""
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD_FILE", str(tmp_path / "pw"))
    monkeypatch.delenv("BYOC_RELAY_ADMIN_USERNAME", raising=False)
    with pytest.raises(SystemExit):
        ra.main(["--root", str(tmp_path), "--dry-run"])
