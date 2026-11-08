"""End-to-end sync: sources, dry-run, PR per user, idempotency."""

import io
import json
import subprocess

import byoc_sync as bs
import pytest
from conftest import MQ_USER, RELAY_USER, git

KEY = b"test-fingerprint-key"
OTHER_USER = "00000000000000aa"


class FakeSource:
    """In-memory Galaxy."""

    def __init__(self, users):
        """``users``: {id: prefs or None (deleted)}."""
        self.users = users
        self.fetched = []

    def user_ids(self):
        """Return the listed (active) users."""
        return [u for u, p in self.users.items() if p is not None]

    def get(self, user_id):
        """Return a record, None for unknown users."""
        self.fetched.append(user_id)
        if user_id not in self.users:
            return None
        prefs = self.users[user_id]
        return bs.UserRecord(user_id, prefs is None, prefs or {})


class FakeGh:
    """Records gh calls and keeps open PRs; git calls run for real."""

    def __init__(self):
        """Start without PRs."""
        self.prs = {}
        self.calls = []

    def __call__(self, args, cwd=None, **kw):
        """Dispatch like byoc_sync.run."""
        if args[0] != "gh":
            return bs.run(args, cwd=cwd, **kw)
        self.calls.append(args[1:3])
        opt = {args[i]: args[i + 1] for i in range(3, len(args) - 1)}
        if args[1:3] == ["pr", "list"]:
            pr = self.prs.get(opt["--head"])
            out = json.dumps([pr] if pr else [])
        elif args[1:3] == ["pr", "create"]:
            assert opt["--repo"] == "me/infrastructure-playbook"
            assert opt["--base"] == "main"
            self.prs[opt["--head"]] = {"number": 1, "body": opt["--body"]}
            out = f"https://github.com/me/pr/{opt['--head']}"
        elif args[1:3] == ["pr", "edit"]:
            self.prs[args[3]]["body"] = opt["--body"]
            out = ""
        else:  # pragma: no cover
            raise AssertionError(args)
        return subprocess.CompletedProcess(args, 0, out, "")


@pytest.fixture
def github(git_checkout):
    """Return a GitHub using the bare remote and a fake gh."""
    root, _ = git_checkout
    return bs.GitHub(
        root, "me/infrastructure-playbook", "main", runner=FakeGh()
    )


def snapshot(root):
    """Return the bytes of every managed file."""
    return {
        p: (root / p).read_bytes() for p in bs.PLAIN_FILES + bs.VAULT_FILES
    }


def test_dry_run_writes_nothing(checkout, vault, mq_prefs, relay_prefs):
    """Dry-run prints the diffs with redacted secrets and changes nothing."""
    before = snapshot(checkout)
    out = io.StringIO()
    results = bs.sync(
        FakeSource({MQ_USER: mq_prefs, RELAY_USER: relay_prefs}),
        checkout,
        vault,
        KEY,
        dry_run=True,
        out=out,
    )
    assert snapshot(checkout) == before
    assert [r.status for r in results] == ["dry-run", "dry-run"]
    text = out.getvalue()
    assert '+        relay_url: "https://relay.example.org"' in text
    assert "(value redacted)" in text
    for secret in (mq_prefs["password"], relay_prefs["relay_password"]):
        assert secret not in text
    assert "galaxy-pulsar" not in text  # relay username is in the vault


def test_one_pr_per_user_and_idempotent(
    git_checkout, github, vault, mq_prefs, relay_prefs
):
    """Each user gets a branch + PR; a re-run does not push again."""
    root, remote = git_checkout
    source = FakeSource({MQ_USER: mq_prefs, RELAY_USER: relay_prefs})
    results = bs.sync(source, root, vault, KEY, github=github, dry_run=False)
    assert [r.status for r in results] == ["published", "published"]
    branches = git("branch", "--list", "byoc/*", cwd=remote).split()
    assert branches == ["byoc/byoc_" + MQ_USER, "byoc/byoc_" + RELAY_USER]
    # each branch holds only its own user
    show = git("show", f"byoc/byoc_{RELAY_USER}:{bs.JOB_CONF}", cwd=remote)
    assert bs.managed_slugs(show) == ["byoc_" + RELAY_USER]
    # base branch untouched, secrets encrypted on the branch
    assert "BYOC" not in git("show", f"main:{bs.JOB_CONF}", cwd=remote)
    vault_blob = git(
        "show", f"byoc/byoc_{RELAY_USER}:{bs.RELAY_VAULT}", cwd=remote
    )
    assert vault_blob.startswith("$ANSIBLE_VAULT")
    pr_body = github.run.prs["byoc/byoc_" + RELAY_USER]["body"]
    message = git(
        "log", "-1", "--format=%B", f"byoc/byoc_{RELAY_USER}", cwd=remote
    )
    assert message.startswith("BYOC: add relay Pulsar for byoc_")
    assert "BYOC-State: " in message
    assert relay_prefs["relay_password"] not in pr_body
    assert "BYOC-State: " in pr_body
    heads = git("for-each-ref", "refs/heads/byoc", cwd=remote)

    again = bs.sync(source, root, vault, KEY, github=github, dry_run=False)
    assert [r.status for r in again] == ["pending", "pending"]
    assert git("for-each-ref", "refs/heads/byoc", cwd=remote) == heads
    creates = [c for c in github.run.calls if c == ["pr", "create"]]
    assert len(creates) == 2


def test_changed_prefs_update_the_open_pr(
    git_checkout, github, vault, relay_prefs
):
    """A new value force-pushes the branch and edits the PR, no new PR."""
    root, remote = git_checkout
    source = FakeSource({RELAY_USER: relay_prefs})
    bs.sync(source, root, vault, KEY, github=github, dry_run=False)
    relay_prefs["max_accepted_cores"] = 4
    results = bs.sync(source, root, vault, KEY, github=github, dry_run=False)
    assert results[0].status == "published"
    assert results[0].detail.startswith("updated PR")
    show = git("show", f"byoc/byoc_{RELAY_USER}:{bs.DESTINATIONS}", cwd=remote)
    assert "max_accepted_cores: 4" in show
    assert [c for c in github.run.calls if c[1] == "create"] == [
        ["pr", "create"]
    ]


def test_no_change_no_pr(checkout, vault, relay_prefs, github):
    """Once merged (state in the checkout), nothing is proposed."""
    cfg = bs.validate(RELAY_USER, relay_prefs)
    bs.apply_change(
        checkout, bs.plan_change(checkout, cfg.slug, cfg, vault), vault
    )
    results = bs.sync(
        FakeSource({RELAY_USER: relay_prefs}),
        checkout,
        vault,
        KEY,
        github=github,
        dry_run=False,
    )
    assert [r.status for r in results] == ["unchanged"]
    assert github.run.calls == []


def test_users_without_byoc_are_skipped(checkout, vault):
    """Most users have no BYOC prefs: no output, no PR."""
    out = io.StringIO()
    results = bs.sync(
        FakeSource({MQ_USER: {}, RELAY_USER: {"transport": "none"}}),
        checkout,
        vault,
        KEY,
        out=out,
    )
    assert results == [] and out.getvalue() == ""


def test_disabled_or_deleted_user_gets_removal(
    checkout, vault, mq_prefs, relay_prefs
):
    """Generated config of disabled/deleted users is proposed for removal."""
    for user, prefs in ((MQ_USER, mq_prefs), (RELAY_USER, relay_prefs)):
        cfg = bs.validate(user, prefs)
        bs.apply_change(
            checkout, bs.plan_change(checkout, cfg.slug, cfg, vault), vault
        )
    source = FakeSource({MQ_USER: {"transport": "none"}, RELAY_USER: None})
    out = io.StringIO()
    results = bs.sync(source, checkout, vault, KEY, out=out)
    assert {r.slug: r.status for r in results} == {
        "byoc_" + MQ_USER: "dry-run",
        "byoc_" + RELAY_USER: "dry-run",
    }
    assert "BYOC: remove Pulsar endpoint" in out.getvalue()
    # the deleted user is not listed but was looked up directly
    assert RELAY_USER in source.fetched


def test_invalid_prefs_never_remove(checkout, vault, relay_prefs):
    """A typo in the prefs is reported and keeps the working config."""
    cfg = bs.validate(RELAY_USER, relay_prefs)
    bs.apply_change(
        checkout, bs.plan_change(checkout, cfg.slug, cfg, vault), vault
    )
    relay_prefs["relay_url"] = "http://relay.example.org"
    results = bs.sync(
        FakeSource({RELAY_USER: relay_prefs}), checkout, vault, KEY
    )
    assert [(r.status, r.detail) for r in results] == [
        ("invalid", "relay_url must start with https://")
    ]


def test_duplicate_mq_username_rejected_for_both(checkout, vault, mq_prefs):
    """Two users choosing the same RabbitMQ name: neither is onboarded."""
    results = bs.sync(
        FakeSource({MQ_USER: mq_prefs, OTHER_USER: dict(mq_prefs)}),
        checkout,
        vault,
        KEY,
        out=io.StringIO(),
    )
    assert {r.status for r in results} == {"invalid"}
    assert len(results) == 2


def test_only_and_max_prs(git_checkout, github, vault, mq_prefs, relay_prefs):
    """--user limits the scan; --max-prs caps PRs per run."""
    root, _ = git_checkout
    source = FakeSource({MQ_USER: mq_prefs, RELAY_USER: relay_prefs})
    only = bs.sync(
        source, root, vault, KEY, only={RELAY_USER}, out=io.StringIO()
    )
    assert [r.slug for r in only] == ["byoc_" + RELAY_USER]
    capped = bs.sync(
        source, root, vault, KEY, github=github, dry_run=False, max_prs=1
    )
    assert sorted(r.status for r in capped) == ["deferred", "published"]


def test_fingerprint_is_keyed(relay_prefs):
    """The fingerprint changes with the key and with any value."""
    cfg = bs.validate(RELAY_USER, relay_prefs)
    assert bs.state_fingerprint(cfg, b"a") != bs.state_fingerprint(cfg, b"b")
    relay_prefs["relay_password"] = "a-different-fake-pass"
    other = bs.validate(RELAY_USER, relay_prefs)
    assert bs.state_fingerprint(cfg, KEY) != bs.state_fingerprint(other, KEY)


class FakeResponse:
    """requests.Response stand-in."""

    def __init__(self, status, data=None):
        """Store status and JSON."""
        self.status_code, self._data = status, data

    def json(self):
        """Return the JSON body."""
        return self._data


class FakeSession:
    """requests.Session stand-in answering like Galaxy 26.1."""

    def __init__(self, users, prefs):
        """``users`` for /api/users, ``prefs`` {id: {input: value}}."""
        self.headers = {}
        self.users, self.prefs = users, prefs
        self.requests = []

    def get(self, url, params=None, timeout=None):
        """Answer a GET."""
        assert timeout and self.headers["x-api-key"] == "fake-admin-key"
        path = url.split("https://galaxy.example.org", 1)[1]
        self.requests.append((path, params))
        if path == "/api/users":
            start, size = params["offset"], params["limit"]
            return FakeResponse(200, self.users[start : start + size])
        user_id = path.split("/")[3]
        if user_id not in self.prefs:
            return FakeResponse(404)
        if path.endswith("/information/inputs"):
            fields = [
                {"name": k, "value": v} for k, v in self.prefs[user_id].items()
            ]
            return FakeResponse(
                200,
                {
                    "inputs": [
                        {"name": "other", "inputs": [{"name": "transport"}]},
                        {
                            "type": "section",
                            "name": "byoc_pulsar",
                            "inputs": fields,
                        },
                    ]
                },
            )
        return FakeResponse(200, {"id": user_id, "deleted": False})


def test_galaxy_api_source(relay_prefs):
    """Pagination and parsing of the legacy information/inputs payload."""
    users = [{"id": f"{i:016x}"} for i in range(5)]
    session = FakeSession(users, {"0000000000000003": relay_prefs})
    source = bs.GalaxyApiSource(
        "https://galaxy.example.org/", "fake-admin-key", session=session
    )
    source.page_size = 2
    assert list(source.user_ids()) == [u["id"] for u in users]
    record = source.get("0000000000000003")
    assert record.prefs == relay_prefs and not record.deleted
    assert source.get("0000000000000004") is None
    assert source.get("../../api/x") is None


@pytest.mark.parametrize("status", [401, 403, 500])
def test_galaxy_api_errors_abort(status):
    """A bad or non-admin key must abort, never look like deleted users."""

    class Denying(FakeSession):
        def get(self, url, params=None, timeout=None):
            return FakeResponse(status)

    source = bs.GalaxyApiSource(
        "https://galaxy.example.org", "fake-admin-key", Denying([], {})
    )
    with pytest.raises(bs.ByocError):
        source.get(MQ_USER)
    with pytest.raises(bs.ByocError):
        list(source.user_ids())


def test_galaxy_api_requires_https():
    """The admin key never travels over plain http."""
    with pytest.raises(bs.ByocError):
        bs.GalaxyApiSource(
            "http://galaxy.example.org", "k", FakeSession([], {})
        )


def test_main_dry_run_with_json(
    tmp_path, checkout, vault, relay_prefs, capsys, monkeypatch
):
    """The CLI end to end, from a JSON export, in dry-run mode."""
    export = tmp_path / "users.json"
    export.write_text(
        json.dumps(
            {
                "users": [
                    {"id": RELAY_USER, "preferences": relay_prefs},
                    {
                        "id": MQ_USER,
                        "preferences": {"transport": "mq", "username": "x"},
                    },
                ]
            }
        )
    )
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD_FILE", vault.password_file)
    monkeypatch.setattr(
        bs.AnsibleVault,
        "__init__",
        lambda self, pw: setattr(self, "password_file", pw)
        or setattr(self, "binary", vault.binary),
    )
    before = snapshot(checkout)
    code = bs.main(
        ["--root", str(checkout), "--prefs-json", str(export), "--dry-run"]
    )
    captured = capsys.readouterr()
    assert code == 1  # the mq user is invalid
    assert f"byoc_{MQ_USER}: invalid max_accepted_cores" in captured.err
    assert f"byoc_{RELAY_USER}: dry-run" in captured.err
    assert relay_prefs["relay_password"] not in captured.out + captured.err
    assert snapshot(checkout) == before


def test_main_requires_repo_unless_dry_run(monkeypatch, tmp_path):
    """Without --dry-run the PR target must be explicit."""
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD_FILE", str(tmp_path / "pw"))
    monkeypatch.delenv("BYOC_REPO", raising=False)
    monkeypatch.delenv("BYOC_BASE", raising=False)
    with pytest.raises(SystemExit):
        bs.main(["--prefs-json", "x.json"])
