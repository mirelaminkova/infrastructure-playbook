"""Site settings: usegalaxy.eu as the default, test.usegalaxy.be as a second site."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

import byoc_sync as bs
from conftest import FAKE_VAULT_PASSWORD, MQ_USER, ansible_vault_binary

HERE = Path(__file__).resolve().parent
SITES = HERE.parent / "sites"
BE_FIXTURE = HERE / "fixtures" / "test.usegalaxy.be"


@pytest.fixture
def be_site():
    """Return the test.usegalaxy.be settings."""
    return bs.load_site(str(SITES / "test.usegalaxy.be.yml"))


@pytest.fixture
def be_vault(tmp_path):
    """Return an AnsibleVault for the "byoc" vault id with a fake password."""
    password_file = tmp_path / "byoc_vault_password"
    password_file.write_text(FAKE_VAULT_PASSWORD + "\n")
    return bs.AnsibleVault(
        str(password_file), binary=ansible_vault_binary(), vault_id="byoc"
    )


@pytest.fixture
def be_checkout(tmp_path):
    """Copy the test.usegalaxy.be fixture into a new tree."""
    root = tmp_path / "be-playbook"
    shutil.copytree(BE_FIXTURE, root)
    return root


def snapshot(root):
    """Return {relative path: text} for every file under root."""
    return {
        str(p.relative_to(root)): p.read_text()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def be_cfg(be_site, mq_prefs, **changes):
    """Return a validated mq configuration on test.usegalaxy.be."""
    return bs.validate(MQ_USER, {**mq_prefs, **changes}, be_site)


def test_eu_settings_file_matches_the_default():
    """sites/usegalaxy.eu.yml describes exactly what the script does today."""
    assert bs.load_site(str(SITES / "usegalaxy.eu.yml")) == bs.EU_SITE


def test_unknown_or_unsafe_settings_are_refused(tmp_path):
    """A typo or a path outside the playbook stops the run."""
    data = yaml.safe_load((SITES / "test.usegalaxy.be.yml").read_text())
    bad = tmp_path / "bad.yml"
    bad.write_text(yaml.safe_dump({**data, "job_cnf": "x"}))
    with pytest.raises(bs.ByocError, match="unknown site settings"):
        bs.load_site(str(bad))
    bad.write_text(yaml.safe_dump({**data, "mq_vars": "../outside.yml"}))
    with pytest.raises(bs.ByocError, match="relative to the playbook"):
        bs.load_site(str(bad))
    bad.write_text(yaml.safe_dump({**data, "anchors": data["anchors"][:1]}))
    with pytest.raises(bs.ByocError, match="no insertion point"):
        bs.load_site(str(bad))


def test_relay_is_refused_where_not_offered(be_site, relay_prefs):
    """test.usegalaxy.be offers RabbitMQ only."""
    with pytest.raises(bs.ByocValidationError, match="only the mq transport"):
        bs.validate(MQ_USER, relay_prefs, be_site)


def test_add_on_the_second_site(be_site, be_checkout, be_vault, mq_prefs):
    """A new mq endpoint lands in all four files in test.usegalaxy.be's layout."""
    cfg = be_cfg(be_site, mq_prefs)
    change = bs.plan_change(be_checkout, cfg.slug, cfg, be_vault, be_site)
    assert change.action == "add"
    assert change.vault == {be_site.mq_vault: {bs.mq_password_var(cfg.slug): "set"}}
    assert mq_prefs["password"] not in change.diff()
    bs.apply_change(be_checkout, change, be_vault, be_site)

    job_conf = yaml.safe_load((be_checkout / be_site.job_conf).read_text())
    runner = job_conf["galaxy_job_config"]["runners"][f"pulsar_{cfg.slug}"]
    assert runner["load"] == "galaxy.jobs.runners.pulsar:PulsarMQJobRunner"
    assert runner["amqp_url"] == (
        f"pyamqp://{cfg.mq_username}:{{{{ {bs.mq_password_var(cfg.slug)} }}}}"
        f"@127.0.0.1:5671//pulsar/{cfg.mq_username}?ssl=1"
    )
    assert runner["galaxy_url"] == "https://test.usegalaxy.be"
    assert runner["manager"] == "production"
    assert runner["amqp_acknowledge"] is True
    assert list(job_conf["galaxy_job_config"]) == ["runners", "handling"]

    destinations = yaml.safe_load((be_checkout / be_site.destinations).read_text())
    destination = destinations["destinations"][f"pulsar_{cfg.slug}_tpv"]
    assert destination["inherits"] == "_pulsar"
    assert destination["scheduling"]["require"] == [cfg.tag]

    users = yaml.safe_load((be_checkout / be_site.mq_vars).read_text())
    assert users["rabbitmq_users"][-1] == {
        "user": cfg.mq_username,
        "password": f"{{{{ {bs.mq_password_var(cfg.slug)} }}}}",
        "vhost": f"/pulsar/{cfg.mq_username}",
    }

    secrets = (be_checkout / be_site.mq_vault).read_text()
    assert mq_prefs["password"] not in secrets
    assert "$ANSIBLE_VAULT;1.2;AES256;byoc" in secrets
    cipher = bs.inline_vault_values(bs.block_text(secrets, cfg.slug))
    assert be_vault.decrypt_string(cipher[bs.mq_password_var(cfg.slug)]) == (
        mq_prefs["password"]
    )


def test_rerun_gives_no_diff(be_site, be_checkout, be_vault, mq_prefs):
    """Unchanged values keep their ciphertext, so a second run is a no-op."""
    cfg = be_cfg(be_site, mq_prefs)
    bs.apply_change(
        be_checkout,
        bs.plan_change(be_checkout, cfg.slug, cfg, be_vault, be_site),
        be_vault,
        be_site,
    )
    before = snapshot(be_checkout)
    again = bs.plan_change(be_checkout, cfg.slug, cfg, be_vault, be_site)
    assert not again.changed
    assert snapshot(be_checkout) == before


def test_new_password_only_touches_the_secret(
    be_site, be_checkout, be_vault, mq_prefs
):
    """A new password re-encrypts one value and leaves the plain files alone."""
    cfg = be_cfg(be_site, mq_prefs)
    bs.apply_change(
        be_checkout,
        bs.plan_change(be_checkout, cfg.slug, cfg, be_vault, be_site),
        be_vault,
        be_site,
    )
    new_cfg = be_cfg(be_site, mq_prefs, password="AnotherPassw0rd99")
    change = bs.plan_change(be_checkout, cfg.slug, new_cfg, be_vault, be_site)
    assert change.action == "update"
    assert change.plain == {}
    assert change.vault == {be_site.mq_vault: {bs.mq_password_var(cfg.slug): "set"}}
    bs.apply_change(be_checkout, change, be_vault, be_site)
    secrets = (be_checkout / be_site.mq_vault).read_text()
    cipher = bs.inline_vault_values(bs.block_text(secrets, cfg.slug))
    assert be_vault.decrypt_string(cipher[bs.mq_password_var(cfg.slug)]) == (
        "AnotherPassw0rd99"
    )


def test_removal_restores_the_files(be_site, be_checkout, be_vault, mq_prefs):
    """Disabling BYOC removes every generated block again."""
    original = snapshot(be_checkout)
    cfg = be_cfg(be_site, mq_prefs)
    bs.apply_change(
        be_checkout,
        bs.plan_change(be_checkout, cfg.slug, cfg, be_vault, be_site),
        be_vault,
        be_site,
    )
    change = bs.plan_change(be_checkout, cfg.slug, None, be_vault, be_site)
    assert change.action == "remove"
    bs.apply_change(be_checkout, change, be_vault, be_site)
    assert snapshot(be_checkout) == original


def ansible_debug(tmp_path, extra_vars, var, vault_id=None):
    """Run ``ansible localhost -m debug`` with a vars file; return the result."""
    binary = Path(ansible_vault_binary()).with_name("ansible")
    if not binary.exists():
        pytest.skip("ansible is not installed")
    (tmp_path / "ansible.cfg").write_text("[defaults]\n")
    args = [str(binary), "localhost", "-c", "local", "-i", "localhost,"]
    args += ["-m", "debug", "-a", f"var={var}", "-e", f"@{extra_vars}"]
    if vault_id:
        args += ["--vault-id", vault_id]
    env = {"ANSIBLE_CONFIG": str(tmp_path / "ansible.cfg"), "PATH": "/usr/bin:/bin"}
    return subprocess.run(
        args, capture_output=True, text=True, env=env, cwd=tmp_path, check=False
    )


def test_inline_values_are_only_decrypted_when_used(
    tmp_path, be_site, be_checkout, be_vault, mq_prefs
):
    """Without the BYOC password other variables still load; with it the
    password decrypts. Runs that do not use BYOC secrets need no password."""
    cfg = be_cfg(be_site, mq_prefs)
    bs.apply_change(
        be_checkout,
        bs.plan_change(be_checkout, cfg.slug, cfg, be_vault, be_site),
        be_vault,
        be_site,
    )
    secrets = be_checkout / be_site.mq_vault
    plain = ansible_debug(tmp_path, secrets, "tpv_byoc_enabled")
    assert plain.returncode == 0, plain.stdout + plain.stderr
    assert '"tpv_byoc_enabled": true' in plain.stdout
    used = ansible_debug(
        tmp_path,
        secrets,
        bs.mq_password_var(cfg.slug),
        vault_id=f"byoc@{be_vault.password_file}",
    )
    assert used.returncode == 0, used.stdout + used.stderr
    assert mq_prefs["password"] in used.stdout


def test_cli_dry_run_on_the_second_site(
    tmp_path, be_checkout, be_vault, mq_prefs, capsys, monkeypatch
):
    """--site switches the whole run, from validation to the diff."""
    export = tmp_path / "users.json"
    export.write_text(
        json.dumps({"users": [{"id": MQ_USER, "preferences": mq_prefs}]})
    )
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD_FILE", be_vault.password_file)
    monkeypatch.setattr(
        bs.AnsibleVault,
        "__init__",
        lambda self, pw, vault_id=None: (
            setattr(self, "password_file", pw)
            or setattr(self, "binary", be_vault.binary)
            or setattr(self, "vault_id", vault_id)
        ),
    )
    before = snapshot(be_checkout)
    code = bs.main(
        [
            "--root",
            str(be_checkout),
            "--site",
            str(SITES / "test.usegalaxy.be.yml"),
            "--prefs-json",
            str(export),
            "--dry-run",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0, captured.err
    assert f"byoc_{MQ_USER}: dry-run" in captured.err
    assert "127.0.0.1:5671" in captured.out
    assert "inherits: _pulsar" in captured.out
    assert mq_prefs["password"] not in captured.out + captured.err
    assert snapshot(be_checkout) == before


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__]))
