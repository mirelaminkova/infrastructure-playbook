"""Fixtures for the BYOC sync tests: a throw-away checkout, no network."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import byoc_sync  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FAKE_VAULT_PASSWORD = "not-a-real-vault-password"
MQ_USER = "0123456789abcdef"
RELAY_USER = "fedcba9876543210"


def ansible_vault_binary():
    """Return the ansible-vault next to this interpreter, or on PATH."""
    local = Path(sys.executable).with_name("ansible-vault")
    if local.exists():
        return str(local)
    found = shutil.which("ansible-vault")
    if not found:
        pytest.skip("ansible-vault is not installed")
    return found


@pytest.fixture
def vault(tmp_path):
    """Return an AnsibleVault using a fake password."""
    password_file = tmp_path / "vault_password"
    password_file.write_text(FAKE_VAULT_PASSWORD + "\n")
    return byoc_sync.AnsibleVault(
        str(password_file), binary=ansible_vault_binary()
    )


@pytest.fixture
def checkout(tmp_path, vault):
    """Copy the real plain files and create fake vaults in a new tree."""
    root = tmp_path / "playbook"
    for path in byoc_sync.PLAIN_FILES:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / path, root / path)
    vault.write(
        root / byoc_sync.MQ_VAULT,
        "---\n# existing endpoints\n"
        "rabbitmq_password_galaxy_eosc01: dummyEosc01pw\n",
    )
    vault.write(
        root / byoc_sync.RELAY_VAULT,
        "---\nvault_pulsar_nemo_relay_username: dummy\n"
        "vault_pulsar_nemo_relay_password: dummy-password\n",
    )
    return root


@pytest.fixture
def mq_prefs():
    """Return valid mq preferences."""
    return {
        "transport": "mq",
        "username": "mypulsarendpoint",
        "password": "MyPulS4rP4ssw0rd",
        "max_accepted_cores": "8",
        "max_accepted_mem": 31,
        "min_accepted_gpus": 0,
        "max_accepted_gpus": "0",
    }


@pytest.fixture
def relay_prefs():
    """Return valid relay preferences (fake credentials)."""
    return {
        "transport": "relay",
        "relay_url": "https://Relay.Example.org/",
        "relay_username": "galaxy-pulsar",
        "relay_password": "fake.Pair-Password~123=@%",
        "relay_topic_prefix": "src_user1",
        "jobs_directory": "/data/pulsarstr",
        "max_accepted_cores": 16,
        "max_accepted_mem": 64,
        "min_accepted_gpus": 0,
        "max_accepted_gpus": 1,
    }


def git(*args, cwd):
    """Run git in ``cwd`` and return stdout."""
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


@pytest.fixture
def git_checkout(tmp_path, checkout):
    """Turn ``checkout`` into a clone of a local bare 'GitHub' remote."""
    remote = tmp_path / "remote.git"
    git("init", "--quiet", "--bare", "-b", "main", str(remote), cwd=tmp_path)
    git("init", "--quiet", "-b", "main", cwd=checkout)
    git("config", "user.name", "byoc-test", cwd=checkout)
    git("config", "user.email", "byoc-test@example.org", cwd=checkout)
    git("add", ".", cwd=checkout)
    git("commit", "--quiet", "-m", "base", cwd=checkout)
    git("remote", "add", "origin", str(remote), cwd=checkout)
    git("push", "--quiet", "origin", "main", cwd=checkout)
    return checkout, remote
