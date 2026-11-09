"""Rendering into the real playbook files, for both transports."""

import byoc_sync as bs
import jinja2
import pytest
import yaml
from conftest import MQ_USER, RELAY_USER, REPO

# The relay runner params Galaxy accepts: lib/galaxy/jobs/runners/pulsar.py
# PULSAR_PARAM_SPECS (relay_url, relay_username, relay_password,
# relay_topic_prefix), passed on to pulsar.client.manager
# .build_client_manager(relay_url=..., relay_username=..., ...).
GALAXY_RELAY_PARAMS = {
    "relay_url",
    "relay_username",
    "relay_password",
    "relay_topic_prefix",
}


def vault_vars(root, vault):
    """Return all decrypted vault variables of the checkout."""
    values = {}
    for path in bs.VAULT_FILES:
        loaded = yaml.load(  # noqa: S506
            vault.read(root / path), Loader=bs._UnsafeLoader
        )
        values.update(loaded or {})
    return values


def _template(value, env, variables):
    if isinstance(value, str):
        return env.from_string(value).render(**variables)
    if isinstance(value, list):
        return [_template(v, env, variables) for v in value]
    if isinstance(value, dict):
        return {k: _template(v, env, variables) for k, v in value.items()}
    return value


def rendered_runners(root, vault, extra=None):
    """Render job_conf.yml through job_conf.yml.j2 as Ansible would.

    Only the ``runners:`` part of the template is rendered; that is where
    the runner params end up, unquoted.
    """
    env = jinja2.Environment(undefined=jinja2.ChainableUndefined)
    env.filters["dict2items"] = lambda d: [
        {"key": k, "value": v} for k, v in d.items()
    ]
    env.filters["items2dict"] = lambda items: {
        i["key"]: i["value"] for i in items
    }
    variables = vault_vars(root, vault)
    variables.update(extra or {})
    jobconf = yaml.safe_load((root / bs.JOB_CONF).read_text())
    variables["galaxy_jobconf"] = _template(
        jobconf["galaxy_jobconf"], env, variables
    )
    template = (REPO / "templates/galaxy/config/job_conf.yml.j2").read_text()
    template = template.split("# Job handler configuration")[0]
    output = env.from_string(template).render(**variables)
    return yaml.safe_load(output)["runners"]


def destinations(root):
    """Parse the generated blocks of destinations.yml.j2.

    The file needs the whole inventory to render; the blocks are checked
    in place (between ``destinations:`` and the LOCAL CONDOR header) and
    parsed as they sit under ``destinations:``.
    """
    text = (root / bs.DESTINATIONS).read_text()
    start = text.index("\ndestinations:\n")
    end = text.index("  # LOCAL CONDOR DESTINATIONS #")
    parsed = {}
    for slug in bs.managed_slugs(text):
        block = bs.block_text(text, slug)
        assert start < text.index(block) < end
        parsed.update(
            yaml.safe_load("destinations:\n" + block)["destinations"]
        )
    return parsed


def add(root, vault, user, prefs):
    """Validate, plan and apply one user's change."""
    cfg = bs.validate(user, prefs)
    change = bs.plan_change(root, cfg.slug, cfg, vault)
    bs.apply_change(root, change, vault)
    return cfg, change


def test_relay_runner_and_destination(checkout, vault, relay_prefs):
    """Relay: Galaxy gets exactly the pair credentials, TPV the limits."""
    cfg, change = add(checkout, vault, RELAY_USER, relay_prefs)
    assert change.action == "add"
    runner = rendered_runners(checkout, vault)[f"pulsar_{cfg.slug}"]
    assert runner["load"] == "galaxy.jobs.runners.pulsar:PulsarMQJobRunner"
    relay_keys = {k for k in runner if k.startswith("relay_")}
    assert relay_keys <= GALAXY_RELAY_PARAMS
    assert runner["relay_url"] == "https://relay.example.org"
    assert runner["relay_username"] == "galaxy-pulsar"
    assert runner["relay_password"] == relay_prefs["relay_password"]
    assert runner["relay_topic_prefix"] == "src_user1"
    assert "amqp_url" not in runner
    dest = destinations(checkout)[f"pulsar_{cfg.slug}_tpv"]
    assert dest["runner"] == f"pulsar_{cfg.slug}"
    assert dest["inherits"] == "pulsar_default"
    assert dest["max_accepted_cores"] == 16
    assert dest["max_accepted_gpus"] == 1
    assert dest["params"] == {"jobs_directory": "/data/pulsarstr"}
    assert dest["scheduling"] == {"require": ["byoc-" + RELAY_USER]}
    # relay users need no RabbitMQ account
    assert cfg.slug not in (checkout / bs.MQ_VARS).read_text()


def test_existing_nemo_runner_still_renders(checkout, vault, relay_prefs):
    """All hand-written runners (MQ and NEMO relay) render as before."""
    before = rendered_runners(checkout, vault)
    cfg, _ = add(checkout, vault, RELAY_USER, relay_prefs)
    after = rendered_runners(checkout, vault)
    assert set(after) == set(before) | {f"pulsar_{cfg.slug}"}
    assert {k: after[k] for k in before} == before
    assert "relay_url" in after["pulsar_eu_nemo"]


def test_mq_runner_destination_and_rabbitmq_user(checkout, vault, mq_prefs):
    """Mq: like PR #1384, with a generated name."""
    cfg, _ = add(checkout, vault, MQ_USER, mq_prefs)
    runner = rendered_runners(checkout, vault)[f"pulsar_{cfg.slug}"]
    assert runner["amqp_url"] == (
        "pyamqp://mypulsarendpoint:MyPulS4rP4ssw0rd@mq.galaxyproject.eu:5671"
        "//pulsar/mypulsarendpoint?ssl=1"
    )
    assert runner["manager"] == "production"
    assert not {k for k in runner if k.startswith("relay_")}
    mq = yaml.safe_load((checkout / bs.MQ_VARS).read_text())
    entry = [u for u in mq["rabbitmq_users"] if u["user"] == cfg.mq_username]
    assert entry == [
        {
            "user": "mypulsarendpoint",
            "password": "{{ %s }}" % bs.mq_password_var(cfg.slug),
            "vhost": "/pulsar/mypulsarendpoint",
        }
    ]
    assert mq["rabbitmq_plugins"]  # the anchor key still follows
    dest = destinations(checkout)[f"pulsar_{cfg.slug}_tpv"]
    assert "params" not in dest  # default jobs_directory


def test_no_plaintext_secret_in_git_files(checkout, vault, relay_prefs):
    """Passwords only exist inside the encrypted vault files."""
    add(checkout, vault, RELAY_USER, relay_prefs)
    for path in bs.PLAIN_FILES:
        text = (checkout / path).read_text()
        assert relay_prefs["relay_password"] not in text
        assert 'relay_username: "galaxy-pulsar"' not in text
    for path in bs.VAULT_FILES:
        raw = (checkout / path).read_text()
        assert raw.startswith("$ANSIBLE_VAULT;1.1;AES256")
        assert relay_prefs["relay_password"] not in raw


def test_vault_values_are_unsafe_and_comments_kept(checkout, vault, mq_prefs):
    """Ansible must never template a user value; existing data stays."""
    cfg, _ = add(checkout, vault, MQ_USER, mq_prefs)
    plain = vault.read(checkout / bs.MQ_VAULT)
    assert "# existing endpoints" in plain
    assert "rabbitmq_password_galaxy_eosc01: dummyEosc01pw" in plain
    assert (
        f"{bs.mq_password_var(cfg.slug)}: !unsafe 'MyPulS4rP4ssw0rd'" in plain
    )


def test_idempotent(checkout, vault, relay_prefs):
    """A second run changes nothing, and the vault is not re-encrypted."""
    cfg, _ = add(checkout, vault, RELAY_USER, relay_prefs)
    before = {
        p: (checkout / p).read_bytes() for p in bs.PLAIN_FILES + bs.VAULT_FILES
    }
    again = bs.plan_change(checkout, cfg.slug, cfg, vault)
    assert not again.changed
    bs.apply_change(checkout, again, vault)
    after = {p: (checkout / p).read_bytes() for p in before}
    assert before == after


def test_password_change_only_touches_vault(checkout, vault, relay_prefs):
    """A new credential is an update of the vault alone."""
    cfg, _ = add(checkout, vault, RELAY_USER, relay_prefs)
    relay_prefs["relay_password"] = "another-fake-password-42"
    cfg2 = bs.validate(RELAY_USER, relay_prefs)
    change = bs.plan_change(checkout, cfg.slug, cfg2, vault)
    assert change.action == "update"
    assert not change.plain
    assert change.vault == {
        bs.RELAY_VAULT: {bs.relay_password_var(cfg.slug): "set"}
    }
    assert "another-fake-password-42" not in change.diff()


def test_switch_mq_to_relay(checkout, vault, mq_prefs, relay_prefs):
    """Changing transport removes the RabbitMQ user and its password."""
    cfg, _ = add(checkout, vault, MQ_USER, mq_prefs)
    add(checkout, vault, MQ_USER, relay_prefs)
    assert cfg.slug not in (checkout / bs.MQ_VARS).read_text()
    assert bs.mq_password_var(cfg.slug) not in vault.read(
        checkout / bs.MQ_VAULT
    )
    runner = rendered_runners(checkout, vault)[f"pulsar_{cfg.slug}"]
    assert runner["relay_url"] == "https://relay.example.org"
    assert (checkout / bs.JOB_CONF).read_text().count(
        bs.begin_marker(cfg.slug)
    ) == 1


def test_remove_restores_original(checkout, vault, mq_prefs):
    """Removal leaves the plain files byte-identical to before."""
    original = {p: (checkout / p).read_text() for p in bs.PLAIN_FILES}
    cfg, _ = add(checkout, vault, MQ_USER, mq_prefs)
    change = bs.plan_change(checkout, cfg.slug, None, vault)
    assert change.action == "remove"
    bs.apply_change(checkout, change, vault)
    assert {p: (checkout / p).read_text() for p in original} == original
    assert bs.mq_password_var(cfg.slug) not in vault.read(
        checkout / bs.MQ_VAULT
    )


def test_blocks_sorted_regardless_of_order(
    checkout, vault, mq_prefs, relay_prefs, tmp_path
):
    """Generated blocks are ordered by slug, so output is deterministic."""
    add(checkout, vault, RELAY_USER, relay_prefs)
    add(checkout, vault, MQ_USER, mq_prefs)
    slugs = bs.managed_slugs((checkout / bs.JOB_CONF).read_text())
    assert slugs == sorted(slugs) == ["byoc_" + MQ_USER, "byoc_" + RELAY_USER]
    assert bs.managed_slugs(
        (checkout / bs.DESTINATIONS).read_text()
    ) == sorted(slugs)


def test_mq_username_collision(checkout, vault, mq_prefs):
    """A RabbitMQ user that already exists is never taken over."""
    mq_prefs["username"] = "existing_user"
    text = (checkout / bs.MQ_VARS).read_text()
    text = text.replace(
        "\nrabbitmq_plugins:",
        "  - user: existing_user\n    password: x\n\nrabbitmq_plugins:",
    )
    (checkout / bs.MQ_VARS).write_text(text)
    cfg = bs.validate(MQ_USER, mq_prefs)
    with pytest.raises(bs.ByocValidationError, match="already exists"):
        bs.plan_change(checkout, cfg.slug, cfg, vault)


def test_broken_markers_abort(checkout, vault, mq_prefs):
    """A hand-edited block (one marker missing) aborts instead of guessing."""
    cfg, _ = add(checkout, vault, MQ_USER, mq_prefs)
    path = checkout / bs.JOB_CONF
    path.write_text(path.read_text().replace(bs.end_marker(cfg.slug), ""))
    with pytest.raises(bs.ByocError, match="markers"):
        bs.plan_change(checkout, cfg.slug, cfg, vault)


def test_refuses_plaintext_vault(checkout, vault, mq_prefs):
    """A vault file that is not encrypted is never written to."""
    (checkout / bs.MQ_VAULT).write_text("rabbitmq_password_x: y\n")
    cfg = bs.validate(MQ_USER, mq_prefs)
    with pytest.raises(bs.ByocError, match="not vault-encrypted"):
        bs.plan_change(checkout, cfg.slug, cfg, vault)


def test_set_vault_vars_rejects_quotes():
    """Defence in depth if validation were ever bypassed."""
    with pytest.raises(bs.ByocError):
        bs.set_vault_vars("a: 1\n", {"b": "x'y"})


def test_vault_ignores_env_and_ansible_cfg(
    checkout, vault, mq_prefs, monkeypatch, tmp_path
):
    """CI sets ANSIBLE_VAULT_PASSWORD_FILE and the repo has an ansible.cfg.

    Neither may add a second vault id (ansible-vault then refuses to
    encrypt).
    """
    other = tmp_path / "other_password"
    other.write_text("x\n")
    (tmp_path / "ansible.cfg").write_text(
        f"[defaults]\nvault_password_file={other}\n"
    )
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD_FILE", vault.password_file)
    monkeypatch.setenv("ANSIBLE_CONFIG", str(tmp_path / "ansible.cfg"))
    monkeypatch.chdir(tmp_path)
    cfg, _ = add(checkout, vault, MQ_USER, mq_prefs)
    assert "MyPulS4rP4ssw0rd" in vault.read(checkout / bs.MQ_VAULT)


def sn09_relay_vars():
    """Return the central relay URL variable as Ansible resolves it."""
    sn09 = yaml.safe_load((REPO / "group_vars/sn09/sn09.yml").read_text())
    value = jinja2.Environment().from_string(sn09[bs.CENTRAL_RELAY_VAR])
    return {bs.CENTRAL_RELAY_VAR: value.render(**sn09)}


def test_central_relay_runner(checkout, vault, central_prefs):
    """Central: stargate URL via sn09, account byoc_<id>, no URL typed."""
    cfg, change = add(checkout, vault, RELAY_USER, central_prefs)
    job_conf = (checkout / bs.JOB_CONF).read_text()
    assert f'relay_url: "{{{{ {bs.CENTRAL_RELAY_VAR} }}}}"' in job_conf
    runner = rendered_runners(checkout, vault, sn09_relay_vars())[
        f"pulsar_{cfg.slug}"
    ]
    assert {k for k in runner if k.startswith("relay_")} <= GALAXY_RELAY_PARAMS
    # same relay as the production NEMO runners
    assert runner["relay_url"] == "https://stargate.galaxyproject.eu/"
    assert runner["relay_username"] == "byoc_" + RELAY_USER
    assert runner["relay_password"] == central_prefs["relay_password"]
    assert "relay_topic_prefix" not in runner
    assert set(change.vault) == {bs.RELAY_VAULT}
    assert cfg.slug not in (checkout / bs.MQ_VARS).read_text()


def test_switch_own_to_central(checkout, vault, relay_prefs, central_prefs):
    """Moving to the central relay rewrites the runner and the username."""
    add(checkout, vault, RELAY_USER, relay_prefs)
    cfg, change = add(checkout, vault, RELAY_USER, central_prefs)
    assert change.action == "update"
    assert change.vault[bs.RELAY_VAULT] == {
        bs.relay_username_var(cfg.slug): "set",
        bs.relay_password_var(cfg.slug): "set",
    }
    runner = rendered_runners(checkout, vault, sn09_relay_vars())[
        f"pulsar_{cfg.slug}"
    ]
    assert runner["relay_url"] == "https://stargate.galaxyproject.eu/"
    assert runner["relay_username"] == cfg.slug
