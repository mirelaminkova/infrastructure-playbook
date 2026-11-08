#!/usr/bin/env python3
"""Turn BYOC Pulsar user preferences into infrastructure-playbook PRs.

"Bring your own compute", variant 2: a user sets a credential on their own
Pulsar VM, enters it together with the compute details in the
``byoc_pulsar`` section of their usegalaxy.eu preferences, and this script
(run by CI with a Galaxy admin API key) proposes the playbook change that
routes that user's jobs to their Pulsar. An admin audits and merges the PR.

Two transports are supported:

``mq``
    RabbitMQ on mq.galaxyproject.eu, as for the existing BYOC endpoints:
    a ``rabbitmq_users`` entry, a ``PulsarMQJobRunner`` with ``amqp_url``
    and the password in ``secret_group_vars/pulsar.yml``.
``relay``
    An HTTPS pulsar-relay (e.g. the SURF Research Cloud component): a
    ``PulsarMQJobRunner`` with ``relay_url``/``relay_username``/
    ``relay_password`` (+ optional ``relay_topic_prefix``), credentials in
    ``group_vars/pulsar_relay/vault.yml`` like the NEMO relay runners.

Every generated block is fenced by ``# BEGIN BYOC <slug>`` / ``# END BYOC
<slug>`` markers so it can be replaced or removed without touching the
hand-written parts. Secrets only ever reach git encrypted by ansible-vault.

See scripts/byoc/README.md for the full design.
"""

from __future__ import annotations

import argparse
import dataclasses
import difflib
import hashlib
import hmac
import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import urlsplit

import yaml

SECTION = "byoc_pulsar"
MANAGED_BY = "managed by scripts/byoc/byoc_sync.py, do not edit"
STATE_TRAILER = "BYOC-State"
BRANCH_PREFIX = "byoc/"

JOB_CONF = "templates/galaxy/config/job_conf.yml"
DESTINATIONS = "files/galaxy/tpv/destinations.yml.j2"
MQ_VARS = "group_vars/mq.yml"
MQ_VAULT = "secret_group_vars/pulsar.yml"
RELAY_VAULT = "group_vars/pulsar_relay/vault.yml"
PLAIN_FILES = (JOB_CONF, DESTINATIONS, MQ_VARS)
VAULT_FILES = (MQ_VAULT, RELAY_VAULT)

MQ_HOST = "mq.galaxyproject.eu:5671"
GALAXY_URL = "https://usegalaxy.eu"

# Galaxy encoded ids: hex, one or two 8-byte blowfish blocks.
USER_ID_RE = re.compile(r"[0-9a-f]{16}|[0-9a-f]{32}")
# Used as RabbitMQ user and vhost name; lower case so it is unambiguous.
MQ_USERNAME_RE = re.compile(r"[a-z0-9][a-z0-9_]{2,31}")
MQ_PASSWORD_RE = re.compile(r"[A-Za-z0-9]{10,128}")
# The pair account rules of pulsar-relay's scripts/configure.py
# (PAIR_USERNAME_PATTERN / PAIR_PASSWORD_PATTERN).
RELAY_USERNAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{2,49}")
RELAY_PASSWORD_RE = re.compile(r"[A-Za-z0-9._~+=@%-]{16,128}")
# Topic names are [A-Za-z0-9_-] upstream (pulsar_relay.models.TopicName).
TOPIC_PREFIX_RE = re.compile(r"[A-Za-z0-9_-]{1,50}")
HOST_LABEL_RE = re.compile(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?")
JOBS_DIRECTORY_RE = re.compile(r"(/[A-Za-z0-9._-]+)+")

RESERVED_MQ_USERNAMES = {"admin", "guest", "mqadmin", "galaxy"}
RESERVED_MQ_PREFIXES = ("galaxy_",)
# Hosts run by usegalaxy.eu itself or not reachable on the internet: a
# user must not be able to point Galaxy's job handlers at them.
FORBIDDEN_HOST_SUFFIXES = (
    "galaxyproject.eu",
    "usegalaxy.eu",
    "privat",
    "local",
    "localhost",
    "internal",
    "lan",
    "home.arpa",
)

LIMITS = {
    "max_accepted_cores": (1, 1024),
    "max_accepted_mem": (1, 8192),
    "min_accepted_gpus": (0, 64),
    "max_accepted_gpus": (0, 64),
}
DISABLED_TRANSPORTS = {"", "none", "disabled"}
LEGACY_SECRET_PLACEHOLDER = "__SECRET_PLACEHOLDER__"


class ByocValidationError(ValueError):
    """A user's BYOC preferences cannot be turned into configuration."""


class ByocError(RuntimeError):
    """An operational error (API, vault, git) that aborts the run."""


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ByocConfig:
    """Validated BYOC configuration of one Galaxy user."""

    user_id: str
    transport: str
    max_accepted_cores: int
    max_accepted_mem: int
    min_accepted_gpus: int
    max_accepted_gpus: int
    jobs_directory: Optional[str] = None
    mq_username: Optional[str] = None
    mq_password: Optional[str] = dataclasses.field(default=None, repr=False)
    relay_url: Optional[str] = None
    relay_username: Optional[str] = dataclasses.field(default=None, repr=False)
    relay_password: Optional[str] = dataclasses.field(default=None, repr=False)
    relay_topic_prefix: Optional[str] = None

    @property
    def slug(self) -> str:
        """Return the identifier used for runner, destination and vars."""
        return slug_for(self.user_id)

    @property
    def tag(self) -> str:
        """Return the TPV scheduling tag; TPV tags use dashes."""
        return "byoc-" + self.user_id


def slug_for(user_id: str) -> str:
    """Return the playbook identifier for a Galaxy encoded user id."""
    return "byoc_" + user_id


def _text(prefs: dict, name: str) -> str:
    value = prefs.get(name)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ByocValidationError(f"{name} must be text")
    if value == LEGACY_SECRET_PLACEHOLDER:
        raise ByocValidationError(
            f"{name} is masked by Galaxy; this Galaxy does not expose "
            "password preferences to admins (see README, open questions)"
        )
    if value != value.strip():
        raise ByocValidationError(
            f"{name} must not start or end with whitespace"
        )
    return value


def _integer(prefs: dict, name: str) -> int:
    value = prefs.get(name)
    if isinstance(value, bool):
        raise ByocValidationError(f"{name} must be a whole number")
    if isinstance(value, float):
        if not value.is_integer():
            raise ByocValidationError(f"{name} must be a whole number")
        value = int(value)
    elif isinstance(value, str):
        if not re.fullmatch(r"[0-9]{1,6}", value.strip()):
            raise ByocValidationError(f"{name} must be a whole number")
        value = int(value.strip())
    elif not isinstance(value, int):
        raise ByocValidationError(f"{name} is required")
    low, high = LIMITS[name]
    if not low <= value <= high:
        raise ByocValidationError(f"{name} must be between {low} and {high}")
    return value


def _match(regex: re.Pattern, value: str, name: str, rule: str) -> str:
    if not regex.fullmatch(value):
        raise ByocValidationError(f"{name} must be {rule}")
    return value


def _yaml_safe(value: str, name: str) -> str:
    """Reject values YAML would not read back as the same string.

    Galaxy's job_conf.yml.j2 prints runner params unquoted, so e.g. a relay
    password of only digits would turn into an integer and ``@...`` is not
    valid YAML at all.
    """
    try:
        loaded = yaml.safe_load(f"k: {value}\n")
    except yaml.YAMLError:
        loaded = None
    if not isinstance(loaded, dict) or loaded.get("k") != value:
        raise ByocValidationError(
            f"{name} cannot be used safely in Galaxy's job configuration "
            "(it must not look like a number or start with a YAML "
            "indicator such as '@' or '%'); please choose another one"
        )
    return value


def normalize_relay_url(raw: str) -> str:
    """Validate a relay URL and return it as ``https://host[:port]``.

    Mirrors pulsar_relay_client._url.normalize_relay_url (no userinfo, no
    path, query or fragment) but is stricter: https only, a public DNS name
    (no IP literal, no internal domain) and no characters that are
    meaningful to YAML or Jinja.
    """
    if not raw:
        raise ByocValidationError("relay_url is required")
    if len(raw) > 255 or not re.fullmatch(r"[A-Za-z0-9.:/-]+", raw):
        raise ByocValidationError(
            "relay_url must be a plain https://host[:port] URL"
        )
    parts = urlsplit(raw)
    if parts.scheme != "https":
        raise ByocValidationError("relay_url must start with https://")
    if parts.username or parts.password or "@" in parts.netloc:
        raise ByocValidationError("relay_url must not contain credentials")
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise ByocValidationError(
            "relay_url must be the relay's origin only, without a path"
        )
    host = (parts.hostname or "").lower().rstrip(".")
    try:
        port = parts.port
    except ValueError:
        raise ByocValidationError("relay_url has an invalid port") from None
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ByocValidationError("relay_url must use a DNS name, not an IP")
    labels = host.split(".")
    if (
        len(labels) < 2
        or not all(HOST_LABEL_RE.fullmatch(label) for label in labels)
        or not re.fullmatch(r"[a-z]{2,63}", labels[-1])
    ):
        raise ByocValidationError("relay_url must use a public DNS name")
    for suffix in FORBIDDEN_HOST_SUFFIXES:
        if host == suffix or host.endswith("." + suffix):
            raise ByocValidationError(
                f"relay_url must not point at {suffix} hosts"
            )
    if port is not None and not 1 <= port <= 65535:
        raise ByocValidationError("relay_url has an invalid port")
    netloc = host if port in (None, 443) else f"{host}:{port}"
    return "https://" + netloc


def is_configured(prefs: dict) -> bool:
    """Return whether the user has opted in (transport mq or relay)."""
    transport = prefs.get("transport")
    if not isinstance(transport, str):
        return False
    return transport.strip().lower() not in DISABLED_TRANSPORTS


def validate(user_id: str, prefs: dict) -> Optional[ByocConfig]:
    """Validate one user's ``byoc_pulsar`` preferences.

    ``prefs`` maps input names (without the ``byoc_pulsar|`` prefix) to
    the stored values. Returns None when BYOC is not enabled and raises
    ByocValidationError when it is enabled but unusable.
    """
    if not isinstance(user_id, str) or not USER_ID_RE.fullmatch(user_id):
        raise ByocValidationError("invalid Galaxy user id")
    if not is_configured(prefs):
        return None
    transport = _text(prefs, "transport").lower()
    if transport not in ("mq", "relay"):
        raise ByocValidationError("transport must be 'mq' or 'relay'")
    resources = {name: _integer(prefs, name) for name in LIMITS}
    if resources["min_accepted_gpus"] > resources["max_accepted_gpus"]:
        raise ByocValidationError(
            "min_accepted_gpus must not exceed max_accepted_gpus"
        )
    jobs_directory = _text(prefs, "jobs_directory") or None
    if jobs_directory is not None:
        _match(
            JOBS_DIRECTORY_RE,
            jobs_directory,
            "jobs_directory",
            "an absolute path of letters, digits, '.', '_' and '-'",
        )
        if len(jobs_directory) > 255 or any(
            part in (".", "..") for part in jobs_directory.split("/")
        ):
            raise ByocValidationError("jobs_directory must be a plain path")
    if transport == "mq":
        username = _match(
            MQ_USERNAME_RE,
            _text(prefs, "username"),
            "username",
            "3-32 lower-case letters, digits or '_'",
        )
        if username in RESERVED_MQ_USERNAMES or username.startswith(
            RESERVED_MQ_PREFIXES
        ):
            raise ByocValidationError(f"username {username!r} is reserved")
        password = _match(
            MQ_PASSWORD_RE,
            _text(prefs, "password"),
            "password",
            "10-128 letters or digits",
        )
        return ByocConfig(
            user_id=user_id,
            transport="mq",
            jobs_directory=jobs_directory,
            mq_username=_yaml_safe(username, "username"),
            mq_password=password,
            **resources,
        )
    relay_url = normalize_relay_url(_text(prefs, "relay_url"))
    relay_username = _match(
        RELAY_USERNAME_RE,
        _text(prefs, "relay_username"),
        "relay_username",
        "3-50 letters, digits, '.', '_' or '-' (starting with a letter "
        "or digit)",
    )
    relay_password = _match(
        RELAY_PASSWORD_RE,
        _text(prefs, "relay_password"),
        "relay_password",
        "16-128 letters, digits or ._~+=@%-",
    )
    prefix = _text(prefs, "relay_topic_prefix") or None
    if prefix is not None:
        _match(
            TOPIC_PREFIX_RE,
            prefix,
            "relay_topic_prefix",
            "1-50 letters, digits, '_' or '-'",
        )
        _yaml_safe(prefix, "relay_topic_prefix")
    return ByocConfig(
        user_id=user_id,
        transport="relay",
        jobs_directory=jobs_directory,
        relay_url=relay_url,
        relay_username=_yaml_safe(relay_username, "relay_username"),
        relay_password=_yaml_safe(relay_password, "relay_password"),
        relay_topic_prefix=prefix,
        **resources,
    )


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------


def mq_password_var(slug: str) -> str:
    """Return the vault variable holding the RabbitMQ password."""
    return f"rabbitmq_password_galaxy_{slug}"


def relay_username_var(slug: str) -> str:
    """Return the vault variable holding the relay username."""
    return f"vault_pulsar_{slug}_relay_username"


def relay_password_var(slug: str) -> str:
    """Return the vault variable holding the relay password."""
    return f"vault_pulsar_{slug}_relay_password"


def begin_marker(slug: str) -> str:
    """Return the comment opening a generated block."""
    return f"# BEGIN BYOC {slug} ({MANAGED_BY})"


def end_marker(slug: str) -> str:
    """Return the comment closing a generated block."""
    return f"# END BYOC {slug}"


def _fence(slug: str, indent: str, body: list) -> list:
    return (
        [indent + begin_marker(slug)]
        + [indent + line if line else "" for line in body]
        + [indent + end_marker(slug)]
    )


def render_runner(cfg: ByocConfig) -> list:
    """Return the job_conf.yml ``galaxy_jobconf.plugins`` entry."""
    slug = cfg.slug
    body = [
        f"- id: pulsar_{slug}",
        "  load: galaxy.jobs.runners.pulsar:PulsarMQJobRunner",
        "  params:",
    ]
    if cfg.transport == "mq":
        user = cfg.mq_username
        body += [
            f'    amqp_url: "pyamqp://{user}:{{{{ {mq_password_var(slug)} }}}}'
            f'@{MQ_HOST}//pulsar/{user}?ssl=1"',
            f'    galaxy_url: "{GALAXY_URL}"',
            "    manager: production",
            '    amqp_acknowledge: "true"',
            "    amqp_ack_republish_time: 300",
            "    amqp_consumer_timeout: 2.0",
            '    amqp_publish_retry: "true"',
            "    amqp_publish_retry_max_retries: 60",
        ]
    else:
        body += [
            f'    relay_url: "{cfg.relay_url}"',
            f'    relay_username: "{{{{ {relay_username_var(slug)} }}}}"',
            f'    relay_password: "{{{{ {relay_password_var(slug)} }}}}"',
        ]
        if cfg.relay_topic_prefix:
            body.append(f'    relay_topic_prefix: "{cfg.relay_topic_prefix}"')
        body.append(f'    galaxy_url: "{GALAXY_URL}"')
    return _fence(slug, "    ", body)


def render_destination(cfg: ByocConfig) -> list:
    """Return the TPV destination for files/galaxy/tpv/destinations.yml.j2."""
    slug = cfg.slug
    body = [
        f"pulsar_{slug}_tpv:",
        "  inherits: pulsar_default",
        f"  runner: pulsar_{slug}",
    ]
    body += [f"  {name}: {getattr(cfg, name)}" for name in LIMITS]
    if cfg.jobs_directory:
        body += ["  params:", f"    jobs_directory: {cfg.jobs_directory}"]
    body += ["  scheduling:", "    require:", f"      - {cfg.tag}"]
    return _fence(slug, "  ", body)


def render_rabbitmq_user(cfg: ByocConfig) -> list:
    """Return the group_vars/mq.yml ``rabbitmq_users`` entry."""
    user = cfg.mq_username
    body = [
        f"- user: {user}",
        f'  password: "{{{{ {mq_password_var(cfg.slug)} }}}}"',
        f"  vhost: /pulsar/{user}",
    ]
    return _fence(cfg.slug, "  ", body)


def vault_values(cfg: Optional[ByocConfig], slug: str) -> dict:
    """Return ``{vault file: {var: value or None}}``; None removes a var."""
    mq = {mq_password_var(slug): None}
    relay = {relay_username_var(slug): None, relay_password_var(slug): None}
    if cfg is not None and cfg.transport == "mq":
        mq[mq_password_var(slug)] = cfg.mq_password
    elif cfg is not None:
        relay[relay_username_var(slug)] = cfg.relay_username
        relay[relay_password_var(slug)] = cfg.relay_password
    return {MQ_VAULT: mq, RELAY_VAULT: relay}


# --------------------------------------------------------------------------
# Editing marker-fenced blocks
# --------------------------------------------------------------------------

BLOCK_RE = re.compile(r"^\s*# BEGIN BYOC (byoc_[0-9a-f]+) ")


def _find_block(lines: list, slug: str) -> Optional[tuple]:
    begin = end = None
    for i, line in enumerate(lines):
        if line.strip() == begin_marker(slug):
            if begin is not None:
                raise ByocError(f"duplicate BYOC block {slug}")
            begin = i
        elif line.strip() == end_marker(slug):
            end = i
    if begin is None and end is None:
        return None
    if begin is None or end is None or end < begin:
        raise ByocError(f"broken BEGIN/END markers for {slug}")
    return begin, end


def managed_slugs(text: str) -> list:
    """Return the slugs of all generated blocks in a file."""
    slugs = []
    for line in text.splitlines():
        found = BLOCK_RE.match(line)
        if found:
            slugs.append(found.group(1))
    return slugs


def _anchor(lines: list, path: str) -> int:
    """Return the index generated blocks are inserted before."""
    if path == JOB_CONF:
        pattern, before_blank = r"  default_destination:", True
    elif path == DESTINATIONS:
        pattern, before_blank = r"  # LOCAL CONDOR DESTINATIONS #", False
    elif path == MQ_VARS:
        pattern, before_blank = r"rabbitmq_plugins:", True
    else:  # pragma: no cover - programming error
        raise ValueError(path)
    for i, line in enumerate(lines):
        if re.match(pattern, line):
            break
    else:
        raise ByocError(f"cannot find the insertion point in {path}")
    if path == DESTINATIONS:
        # keep the "####" banner above the LOCAL CONDOR header together
        while i > 0 and lines[i - 1].strip().startswith("####"):
            i -= 1
    if before_blank:
        while i > 0 and not lines[i - 1].strip():
            i -= 1
    return i


def set_block(text: str, path: str, slug: str, block: Optional[list]) -> str:
    """Replace, insert (sorted by slug) or, with block None, remove a block."""
    lines = text.split("\n")
    found = _find_block(lines, slug)
    if found is not None:
        begin, end = found
        del lines[begin : end + 1]
        if path == DESTINATIONS and begin < len(lines) and not lines[begin]:
            del lines[begin]
    if block is None:
        return "\n".join(lines)
    position = _anchor(lines, path)
    for i, line in enumerate(lines):
        other = BLOCK_RE.match(line)
        if other and other.group(1) > slug and i < position:
            position = i
            break
    if path == DESTINATIONS:
        block = block + [""]
    lines[position:position] = block
    return "\n".join(lines)


def block_text(text: str, slug: str) -> Optional[str]:
    """Return the generated block of ``slug`` in ``text``, if any."""
    lines = text.split("\n")
    found = _find_block(lines, slug)
    if found is None:
        return None
    return "\n".join(lines[found[0] : found[1] + 1])


def check_yaml(text: str, path: str, slug: str) -> None:
    """Fail if a rendered file (or, for a Jinja template, block) is broken.

    destinations.yml.j2 needs the whole Ansible inventory to render, so
    only our block is parsed there, wrapped as it sits under
    ``destinations:``; upstream's TPV workflow renders the full file.
    """
    if path == DESTINATIONS:
        block = block_text(text, slug)
        if block is None:
            return
        text = "destinations:\n" + block
    try:
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ByocError(f"rendering broke the YAML of {path}: {e}") from e


def existing_mq_users(text: str) -> dict:
    """Return ``{rabbitmq user: owning slug or None}`` from mq.yml."""
    owners: dict = {}
    current = None
    for line in text.split("\n"):
        found = BLOCK_RE.match(line)
        if found:
            current = found.group(1)
        elif line.strip().startswith("# END BYOC "):
            current = None
        user = re.match(r"\s*- user:\s*['\"]?([^'\"\s]+)", line)
        if user:
            owners[user.group(1)] = current
    return owners


# --------------------------------------------------------------------------
# Ansible vault
# --------------------------------------------------------------------------


class _UnsafeLoader(yaml.SafeLoader):
    """SafeLoader that understands Ansible's ``!unsafe`` tag."""


_UnsafeLoader.add_constructor(
    "!unsafe", lambda loader, node: loader.construct_scalar(node)
)


def set_vault_vars(plaintext: str, values: dict) -> str:
    """Set (or with None remove) top-level vars in a vault's plaintext.

    Works on lines so comments and the rest of the file stay untouched.
    Values are written as ``!unsafe`` single-quoted strings so Ansible never
    templates them; validation already excluded quotes and braces.
    """
    lines = plaintext.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    for name, value in values.items():
        pattern = re.compile(rf"{re.escape(name)}\s*:")
        index = [i for i, line in enumerate(lines) if pattern.match(line)]
        if len(index) > 1:
            raise ByocError(f"{name} is defined more than once in a vault")
        if value is not None and ("'" in value or "\n" in value):
            raise ByocError(f"refusing to write an unquotable {name}")
        new = None if value is None else f"{name}: !unsafe '{value}'"
        if index and new is None:
            del lines[index[0]]
        elif index:
            lines[index[0]] = new
        elif new is not None:
            lines.append(new)
    result = "\n".join(lines) + "\n"
    try:
        loaded = yaml.load(result, Loader=_UnsafeLoader)  # noqa: S506
    except yaml.YAMLError as e:
        raise ByocError(f"vault plaintext is not valid YAML: {e}") from e
    for name, value in values.items():
        if value is not None and (loaded or {}).get(name) != value:
            raise ByocError(f"{name} did not round-trip through YAML")
    return result


def _empty_ansible_cfg() -> Callable[[], Path]:
    path: list = []

    def get() -> Path:
        if not path:
            directory = Path(tempfile.mkdtemp(prefix="byoc-ansible-"))
            (directory / "ansible.cfg").write_text("[defaults]\n")
            path.append(directory / "ansible.cfg")
        return path[0]

    return get


_EMPTY_ANSIBLE_CFG = _empty_ansible_cfg()


class AnsibleVault:
    """Read and write ansible-vault files through the ansible-vault CLI.

    Plaintext only travels through pipes, never argv, temp files or git.
    """

    def __init__(self, password_file: str, binary: str = "ansible-vault"):
        """Use ``password_file`` (as in ansible.cfg) for every operation."""
        self.password_file = password_file
        self.binary = binary

    @staticmethod
    def _env() -> dict:
        """Return an environment with exactly one vault id: ours.

        ANSIBLE_VAULT_PASSWORD_FILE (as set for this script) or the
        playbook's ansible.cfg would add a second "default" id, and
        ansible-vault then refuses to encrypt.
        """
        env = {
            k: v
            for k, v in os.environ.items()
            if k
            not in (
                "ANSIBLE_VAULT_PASSWORD_FILE",
                "ANSIBLE_VAULT_IDENTITY_LIST",
                "ANSIBLE_VAULT_ID_MATCH",
            )
        }
        env["ANSIBLE_CONFIG"] = str(_EMPTY_ANSIBLE_CFG())
        return env

    def _run(self, args: list, stdin: Optional[str] = None) -> str:
        try:
            done = subprocess.run(
                [self.binary, args[0], "--vault-password-file"]
                + [self.password_file]
                + args[1:],
                input=stdin,
                capture_output=True,
                text=True,
                check=False,
                env=self._env(),
                cwd=str(_EMPTY_ANSIBLE_CFG().parent),
            )
        except FileNotFoundError as e:
            raise ByocError(f"{self.binary} not found") from e
        if done.returncode != 0:
            # stderr of ansible-vault never contains the plaintext
            raise ByocError(
                f"ansible-vault {args[0]} failed: {done.stderr.strip()}"
            )
        return done.stdout

    def read(self, path: Path) -> str:
        """Return the decrypted content ('' for a missing file)."""
        if not path.exists():
            return ""
        if not path.read_text().startswith("$ANSIBLE_VAULT"):
            raise ByocError(f"{path} is not vault-encrypted; refusing to use")
        return self._run(["decrypt", "--output", "-", str(path)])

    def write(self, path: Path, plaintext: str) -> None:
        """Encrypt ``plaintext`` into ``path``."""
        path.parent.mkdir(parents=True, exist_ok=True)
        self._run(["encrypt", "--output", str(path), "-"], stdin=plaintext)
        if not path.read_text().startswith("$ANSIBLE_VAULT"):
            raise ByocError(f"{path} was not encrypted")


# --------------------------------------------------------------------------
# Applying a change to a checkout
# --------------------------------------------------------------------------


@dataclasses.dataclass
class Change:
    """The effect of one user's BYOC state on a checkout."""

    slug: str
    cfg: Optional[ByocConfig]
    plain: dict  # path -> (old text, new text) for changed plain files
    vault: dict  # path -> {var: "set" | "removed"} for changed vault vars
    vault_plain: dict  # path -> new plaintext (never printed)
    existed: bool = False  # the user already had generated blocks

    @property
    def changed(self) -> bool:
        """Return whether anything would change."""
        return bool(self.plain or self.vault)

    @property
    def action(self) -> str:
        """Return 'add', 'update' or 'remove'."""
        if self.cfg is None:
            return "remove"
        return "update" if self.existed else "add"

    def diff(self) -> str:
        """Return a unified diff of plain files plus redacted vault notes."""
        out = []
        for path, (old, new) in sorted(self.plain.items()):
            out += difflib.unified_diff(
                old.splitlines(keepends=True),
                new.splitlines(keepends=True),
                f"a/{path}",
                f"b/{path}",
            )
        for path, names in sorted(self.vault.items()):
            for name, what in sorted(names.items()):
                out.append(f"vault {path}: {what} {name} (value redacted)\n")
        return "".join(out)


def plan_change(
    root: Path,
    slug: str,
    cfg: Optional[ByocConfig],
    vault: AnsibleVault,
) -> Change:
    """Compute (without writing) what ``cfg`` changes in checkout ``root``.

    ``cfg`` None means the user's BYOC configuration must be removed.
    """
    blocks = {
        JOB_CONF: render_runner(cfg) if cfg else None,
        DESTINATIONS: render_destination(cfg) if cfg else None,
        MQ_VARS: (
            render_rabbitmq_user(cfg)
            if cfg and cfg.transport == "mq"
            else None
        ),
    }
    plain = {}
    for path, block in blocks.items():
        old = (root / path).read_text()
        new = set_block(old, path, slug, block)
        if new != old:
            check_yaml(new, path, slug)
            plain[path] = (old, new)
    if cfg is not None and cfg.transport == "mq":
        owners = existing_mq_users((root / MQ_VARS).read_text())
        owner = owners.get(cfg.mq_username, slug)
        if cfg.mq_username in owners and owner != slug:
            raise ByocValidationError(
                f"RabbitMQ user {cfg.mq_username!r} already exists"
            )
    changes: dict = {}
    vault_plain = {}
    for path, values in vault_values(cfg, slug).items():
        old = vault.read(root / path)
        if not old and all(v is None for v in values.values()):
            continue
        new = set_vault_vars(old, values)
        if new != (old if old.endswith("\n") else old + "\n"):
            before = yaml.load(old, Loader=_UnsafeLoader) or {}  # noqa: S506
            after = yaml.load(new, Loader=_UnsafeLoader) or {}  # noqa: S506
            notes = {}
            for name in values:
                if before.get(name) != after.get(name):
                    notes[name] = "removed" if name not in after else "set"
            if notes:
                changes[path] = notes
                vault_plain[path] = new
    existed = begin_marker(slug) in (root / JOB_CONF).read_text()
    return Change(slug, cfg, plain, changes, vault_plain, existed)


def apply_change(root: Path, change: Change, vault: AnsibleVault) -> None:
    """Write a planned change into checkout ``root``."""
    for path, (_, new) in change.plain.items():
        (root / path).write_text(new)
    for path, plaintext in change.vault_plain.items():
        vault.write(root / path, plaintext)


def state_fingerprint(cfg: Optional[ByocConfig], key: bytes) -> str:
    """Return a keyed fingerprint of the desired state of one user.

    It goes into the commit message so a re-run can see that an open PR
    already proposes exactly this state, without decrypting anything. The
    HMAC key keeps the fingerprint from being a password oracle.
    """
    payload = json.dumps(
        dataclasses.asdict(cfg) if cfg else {"removed": True},
        sort_keys=True,
    ).encode()
    return hmac.new(key, payload, hashlib.sha256).hexdigest()[:32]


# --------------------------------------------------------------------------
# Galaxy sources
# --------------------------------------------------------------------------


@dataclasses.dataclass
class UserRecord:
    """A Galaxy user's id, deletion state and ``byoc_pulsar`` values."""

    user_id: str
    deleted: bool
    prefs: dict


class JsonFileSource:
    """Users from a JSON export (tests, or a DB/gxadmin dump at scale).

    Format: ``{"users": [{"id": ..., "deleted": false, "preferences":
    {"transport": "relay", ...}}]}``.
    """

    def __init__(self, path: str):
        """Load the export at ``path``."""
        data = json.loads(Path(path).read_text())
        self.users = {
            u["id"]: UserRecord(
                u["id"], bool(u.get("deleted")), u.get("preferences") or {}
            )
            for u in data.get("users", [])
        }

    def user_ids(self) -> Iterable:
        """Return all user ids."""
        return list(self.users)

    def get(self, user_id: str) -> Optional[UserRecord]:
        """Return one user or None if unknown."""
        return self.users.get(user_id)


class GalaxyApiSource:
    """Users from the Galaxy API, authenticated with an admin API key.

    Values come from ``GET /api/users/{id}/information/inputs``, which on
    Galaxy <= 26.1 (usegalaxy.eu today) still returns ``password`` inputs
    to admins. Galaxy 26.2 masks them; see the README.
    """

    page_size = 500

    def __init__(self, url: str, api_key: str, session: Any = None):
        """Talk to the Galaxy at ``url``."""
        if not url.startswith("https://"):
            raise ByocError("GALAXY_URL must be https")
        if session is None:
            import requests

            session = requests.Session()
        self.url = url.rstrip("/")
        self.session = session
        self.session.headers["x-api-key"] = api_key

    def _get(self, path: str, **params: Any) -> Any:
        response = self.session.get(
            self.url + path, params=params or None, timeout=60
        )
        if response.status_code == 404:
            # only "no such user" may lead to a removal; 401/403 (bad or
            # non-admin key) must abort, never look like deleted users
            return None
        if response.status_code != 200:
            raise ByocError(f"GET {path}: HTTP {response.status_code}")
        return response.json()

    def user_ids(self) -> Iterable:
        """Yield the encoded ids of all active users."""
        seen: set = set()
        offset = 0
        while True:
            page = self._get("/api/users", limit=self.page_size, offset=offset)
            if not page:
                return
            fresh = [u["id"] for u in page if u["id"] not in seen]
            if not fresh:
                return
            seen.update(fresh)
            yield from fresh
            if len(page) < self.page_size:
                return
            offset += self.page_size

    def get(self, user_id: str) -> Optional[UserRecord]:
        """Return the user's record, or None if the user does not exist."""
        if not USER_ID_RE.fullmatch(user_id):
            return None
        user = self._get(f"/api/users/{user_id}")
        if user is None:
            return None
        deleted = bool(user.get("deleted") or user.get("purged"))
        info = (
            None
            if deleted
            else self._get(f"/api/users/{user_id}/information/inputs")
        )
        prefs = {}
        for section in (info or {}).get("inputs", []):
            if section.get("name") == SECTION:
                for field in section.get("inputs", []):
                    if "value" in field:
                        prefs[field["name"]] = field["value"]
        return UserRecord(user_id, deleted, prefs)


# --------------------------------------------------------------------------
# Git / GitHub
# --------------------------------------------------------------------------

Runner = Callable[..., subprocess.CompletedProcess]


def run(args: list, cwd: Optional[Path] = None, **kw: Any):
    """Run a command, raising ByocError on failure."""
    done = subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, check=False, **kw
    )
    if done.returncode != 0:
        raise ByocError(
            f"{' '.join(args[:3])} failed: {done.stderr.strip()[:500]}"
        )
    return done


class GitHub:
    """Branches and PRs through git and the gh CLI."""

    def __init__(
        self,
        root: Path,
        repo: str,
        base: str,
        remote: str = "origin",
        gh: str = "gh",
        runner: Runner = run,
    ):
        """Open PRs in ``repo`` against ``base`` from branches on remote."""
        self.root, self.repo, self.base = root, repo, base
        self.remote, self.gh, self.run = remote, gh, runner

    def open_pr_state(self, branch: str) -> Optional[str]:
        """Return the BYOC-State of the open PR for ``branch``, if any."""
        done = self.run(
            [
                self.gh,
                "pr",
                "list",
                "--repo",
                self.repo,
                "--head",
                branch,
                "--state",
                "open",
                "--json",
                "number,body",
            ],
            cwd=self.root,
        )
        prs = json.loads(done.stdout or "[]")
        if not prs:
            return None
        found = re.search(
            rf"^{STATE_TRAILER}: ([0-9a-f]+)$", prs[0]["body"], re.M
        )
        return found.group(1) if found else ""

    def publish(
        self, change: Change, vault: AnsibleVault, title: str, body: str
    ) -> str:
        """Commit ``change`` on its own branch from base, push, open PR."""
        branch = BRANCH_PREFIX + change.slug
        tmp = Path(tempfile.mkdtemp(prefix="byoc-"))
        work = tmp / "wt"
        try:
            self.run(
                ["git", "fetch", "--quiet", self.remote, self.base],
                cwd=self.root,
            )
            self.run(
                [
                    "git",
                    "worktree",
                    "add",
                    "--quiet",
                    "-B",
                    branch,
                    str(work),
                    f"{self.remote}/{self.base}",
                ],
                cwd=self.root,
            )
            apply_change(work, change, vault)
            self.run(["git", "add", "--all", "."], cwd=work)
            trailer = body.rstrip().rsplit("\n", 1)[-1]
            self.run(
                ["git", "commit", "--quiet", "-m", title, "-m", trailer],
                cwd=work,
            )
            self.run(
                [
                    "git",
                    "push",
                    "--quiet",
                    "--force",
                    self.remote,
                    f"{branch}:{branch}",
                ],
                cwd=work,
            )
        finally:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(work)],
                cwd=self.root,
                capture_output=True,
                check=False,
            )
            shutil.rmtree(tmp, ignore_errors=True)
        if self.open_pr_state(branch) is not None:
            self.run(
                [
                    self.gh,
                    "pr",
                    "edit",
                    branch,
                    "--repo",
                    self.repo,
                    "--title",
                    title,
                    "--body",
                    body,
                ],
                cwd=self.root,
            )
            return f"updated PR for {branch}"
        done = self.run(
            [
                self.gh,
                "pr",
                "create",
                "--repo",
                self.repo,
                "--base",
                self.base,
                "--head",
                branch,
                "--title",
                title,
                "--body",
                body,
            ],
            cwd=self.root,
        )
        return done.stdout.strip()


def pr_text(change: Change, fingerprint: str) -> tuple:
    """Return the PR title and body for a change (no secrets, no email)."""
    cfg, slug = change.cfg, change.slug
    if cfg is None:
        title = f"BYOC: remove Pulsar endpoint of user {slug}"
        what = "The user disabled BYOC or no longer exists."
    else:
        verb = "Update" if change.action == "update" else "Add"
        title = f"BYOC: {verb.lower()} {cfg.transport} Pulsar for {slug}"
        what = "\n".join(
            [
                f"{verb} the BYOC Pulsar of Galaxy user id `{cfg.user_id}` "
                f"(look the user up in the admin UI).",
                "",
                f"- transport: `{cfg.transport}`",
                (
                    f"- relay URL: `{cfg.relay_url}`"
                    if cfg.transport == "relay"
                    else f"- RabbitMQ user/vhost: `{cfg.mq_username}`"
                ),
                f"- cores <= {cfg.max_accepted_cores}, "
                f"mem <= {cfg.max_accepted_mem} GB, "
                f"GPUs {cfg.min_accepted_gpus}-{cfg.max_accepted_gpus}",
                f"- jobs_directory: `{cfg.jobs_directory or 'default'}`",
                f"- TPV tag: `{cfg.tag}`",
            ]
        )
    vault_lines = [
        f"- `{path}`: {what_} `{name}`"
        for path, names in sorted(change.vault.items())
        for name, what_ in sorted(names.items())
    ]
    body = "\n".join(
        [
            what,
            "",
            "Vault changes (values never shown):",
            *(vault_lines or ["- none"]),
            "",
            "Admin checklist:",
            "- [ ] the user is known and allowed to bring their own compute",
            "- [ ] the endpoint belongs to the user's institution / project",
            "- [ ] the resource limits match the endpoint",
            "",
            "Generated by `scripts/byoc/byoc_sync.py`; re-running it "
            "updates this PR instead of opening a new one.",
            "",
            f"{STATE_TRAILER}: {fingerprint}",
        ]
    )
    return title, body


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------


@dataclasses.dataclass
class Result:
    """What happened to one user."""

    slug: str
    status: str  # unchanged|invalid|dry-run|pending|published|deferred
    detail: str = ""


def desired_states(source: Any, root: Path, only: Optional[set]) -> tuple:
    """Return ``({slug: cfg or None}, {slug: error})`` for all users.

    Users with generated blocks are always looked at so a disabled or
    deleted user gets a removal; an invalid configuration never removes
    anything.
    """
    present = set(managed_slugs((root / JOB_CONF).read_text()))
    ids = list(source.user_ids())
    ids += [s[len("byoc_") :] for s in sorted(present)]
    states: dict = {}
    errors: dict = {}
    for user_id in dict.fromkeys(ids):
        if not USER_ID_RE.fullmatch(str(user_id)):
            continue
        slug = slug_for(user_id)
        if only and user_id not in only:
            continue
        record = source.get(user_id)
        if record is None or record.deleted:
            if slug in present:
                states[slug] = None
            continue
        try:
            cfg = validate(user_id, record.prefs)
        except ByocValidationError as e:
            errors[slug] = str(e)
            continue
        if cfg is None and slug not in present:
            continue
        states[slug] = cfg
    usernames: dict = {}
    for slug, cfg in states.items():
        if cfg is not None and cfg.transport == "mq":
            usernames.setdefault(cfg.mq_username, []).append(slug)
    for name, slugs in usernames.items():
        if len(slugs) > 1:
            for slug in slugs:
                errors[slug] = f"RabbitMQ user {name!r} chosen by >1 user"
                del states[slug]
    return states, errors


def sync(
    source: Any,
    root: Path,
    vault: AnsibleVault,
    key: bytes,
    github: Optional[GitHub] = None,
    dry_run: bool = True,
    only: Optional[set] = None,
    max_prs: int = 20,
    out: Any = sys.stdout,
) -> list:
    """Compare Galaxy with checkout ``root`` and propose one PR per user."""
    states, errors = desired_states(source, root, only)
    results = [Result(slug, "invalid", err) for slug, err in errors.items()]
    published = 0
    for slug, cfg in sorted(states.items()):
        try:
            change = plan_change(root, slug, cfg, vault)
        except ByocValidationError as e:
            results.append(Result(slug, "invalid", str(e)))
            continue
        if not change.changed:
            results.append(Result(slug, "unchanged"))
            continue
        fingerprint = state_fingerprint(cfg, key)
        title, body = pr_text(change, fingerprint)
        if dry_run or github is None:
            print(f"### {title}", file=out)
            print(change.diff(), file=out)
            results.append(Result(slug, "dry-run", title))
            continue
        if github.open_pr_state(BRANCH_PREFIX + slug) == fingerprint:
            results.append(Result(slug, "pending", "open PR is current"))
            continue
        if published >= max_prs:
            results.append(Result(slug, "deferred", "--max-prs reached"))
            continue
        url = github.publish(change, vault, title, body)
        published += 1
        results.append(Result(slug, "published", url))
    return results


def fingerprint_key(password_file: str) -> bytes:
    """Return the HMAC key: BYOC_FINGERPRINT_KEY or the vault password."""
    env = os.environ.get("BYOC_FINGERPRINT_KEY")
    if env:
        return env.encode()
    return hashlib.sha256(Path(password_file).read_bytes()).digest()


def main(argv: Optional[list] = None) -> int:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--root", default=".", help="playbook checkout")
    parser.add_argument(
        "--prefs-json", help="read users from a JSON export, not the API"
    )
    parser.add_argument("--user", action="append", help="only this user id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--repo",
        default=os.environ.get("BYOC_REPO"),
        help="GitHub repo PRs are opened in (owner/name)",
    )
    parser.add_argument("--base", default=os.environ.get("BYOC_BASE"))
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--max-prs", type=int, default=20)
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    password_file = os.environ.get("ANSIBLE_VAULT_PASSWORD_FILE")
    if not password_file:
        parser.error("ANSIBLE_VAULT_PASSWORD_FILE must be set")
    if not args.dry_run and not (args.repo and args.base):
        parser.error("--repo and --base are required unless --dry-run")
    try:
        if args.prefs_json:
            source: Any = JsonFileSource(args.prefs_json)
        else:
            url = os.environ.get("GALAXY_URL", GALAXY_URL)
            api_key = os.environ.get("GALAXY_API_KEY")
            if not api_key:
                parser.error("GALAXY_API_KEY must be set")
            source = GalaxyApiSource(url, api_key)
        vault = AnsibleVault(password_file)
        github = (
            None
            if args.dry_run
            else GitHub(root, args.repo, args.base, args.remote)
        )
        results = sync(
            source,
            root,
            vault,
            fingerprint_key(password_file),
            github=github,
            dry_run=args.dry_run,
            only=set(args.user or []) or None,
            max_prs=args.max_prs,
        )
    except ByocError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    for r in results:
        print(f"{r.slug}: {r.status} {r.detail}".rstrip(), file=sys.stderr)
    return 1 if any(r.status == "invalid" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
