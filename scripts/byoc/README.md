# BYOC sync: user preferences → playbook PRs (variant 2, MQ and relay)

"Bring your own compute", variant 2:

1. The user sets a credential on their own Pulsar VM (SimpleVM / SURF
   Research Cloud wizard):
   - **mq**: the RabbitMQ username and password the Pulsar uses;
   - **relay**: the pulsar-relay *pair account* (default user
     `galaxy-pulsar`) shared by Galaxy and Pulsar. `configure.py` of the
     SRC relay component takes it from `RELAY_PAIR_PASSWORD`.
2. In usegalaxy.eu, *User → Preferences → Manage information → byoc_pulsar*,
   the user picks a transport and enters the endpoint, the credential and the
   resource limits.
3. `byoc_sync.py` (CI, Galaxy admin API key) reads the preferences,
   validates them and opens **one PR per user** that changes only that
   user's generated blocks and vault entries.
4. An admin audits and merges the PR and deploys. TPV routes the user's jobs
   to their Pulsar.

```
user ──prefs──▶ Galaxy (extra_user_preferences, byoc_pulsar|*)
                   │  GET /api/users, /api/users/{id}/information/inputs
                   ▼
            byoc_sync.py ── validate ─▶ render blocks ─▶ ansible-vault
                   │                                          │
                   └── git branch byoc/byoc_<id> ── gh pr create ─▶ admin review
                                                                     │ merge + deploy
 job ─▶ TPV rule byoc_pulsar: require tag byoc-<encoded id> ─▶ pulsar_byoc_<id>_tpv
        ─▶ runner pulsar_byoc_<id> (PulsarMQJobRunner)
           mq:    amqp_url → mq.galaxyproject.eu vhost /pulsar/<user>
           relay: relay_url/relay_username/relay_password → user's relay ◀─ Pulsar polls
```

## What a PR changes

Like the hand-written BYOC PRs #872 and #1384, but every addition is fenced
by `# BEGIN BYOC byoc_<id>` / `# END BYOC byoc_<id>` and sorted by id:

| file | mq | relay |
|---|---|---|
| `templates/galaxy/config/job_conf.yml` | runner with `amqp_url` | runner with `relay_url`, `relay_username`, `relay_password` (+ `relay_topic_prefix`) |
| `files/galaxy/tpv/destinations.yml.j2` | destination `pulsar_byoc_<id>_tpv`, `require: [byoc-<id>]`, limits, optional `jobs_directory` | same |
| `group_vars/mq.yml` | `rabbitmq_users` entry, vhost `/pulsar/<user>` | – |
| `secret_group_vars/pulsar.yml` (vault) | `rabbitmq_password_galaxy_byoc_<id>` | – |
| `group_vars/pulsar_relay/vault.yml` (vault) | – | `vault_pulsar_byoc_<id>_relay_username`, `..._relay_password` |

`<id>` is the user's Galaxy encoded id. The PR shows the transport, relay URL
or RabbitMQ user and the limits, never a credential or an email address.

The relay runner params are the ones Galaxy's `PulsarJobRunner` declares
(`lib/galaxy/jobs/runners/pulsar.py`: `relay_url`, `relay_username`,
`relay_password`, `relay_topic_prefix`, also in `release_26.1_europe`) and
passes to `pulsar.client.manager.build_client_manager`, which needs
`relay_username` + `relay_password` when `relay_url` is set. They match the
existing `pulsar_eu_nemo` runners.

One-off changes (in the PR that adds this script, not per user):

- `files/galaxy/config/user_preferences_extra_conf.yml`: `transport`
  (Disabled / mq / relay), `relay_url`, `relay_username`, `relay_password`,
  `relay_topic_prefix`, `jobs_directory`. The existing fields keep their
  names, so stored values stay valid.
- `files/galaxy/tpv/tool_defaults.yml.j2`: rule `byoc_pulsar`. It adds
  `require: byoc-<encode_id(user.id)>` when the user's transport is mq or
  relay, they chose no other remote resource, the tool is not `internal`,
  and a destination with that tag exists (that is, the PR was merged). The tag
  comes from `user.id`, not from anything the user types, so nobody can
  route jobs to someone else's Pulsar. Destinations that require the tag
  never match other users' jobs. Setting the transport to *Disabled* stops
  routing at once, before the removal PR is merged.

## Validation (strict, before anything is rendered)

- transport `mq` or `relay`; anything else (or unset, as in the old form) is
  "not configured".
- mq: username `[a-z0-9][a-z0-9_]{2,31}` and not `galaxy_*`, `mqadmin` or
  `guest`. It must not already be a RabbitMQ user, and two users cannot pick
  the same name. Password `[A-Za-z0-9]{10,128}`.
- relay: URL `https://host[:port]` only, normalized to the origin. No userinfo,
  path, query or fragment, no IP literal, no single-label host, no
  `galaxyproject.eu` / `usegalaxy.eu` / `.privat` / `.local` / `.internal`.
  Username: `configure.py`'s `PAIR_USERNAME_PATTERN`. Password:
  `PAIR_PASSWORD_PATTERN` (`[A-Za-z0-9._~+=@%-]{16,128}`). Topic prefix
  `[A-Za-z0-9_-]{1,50}`.
- every value that lands in YAML must read back as the same string
  (`job_conf.yml.j2` prints runner params unquoted). This rejects e.g. relay
  passwords that are all digits or start with `@`/`%`.
- limits are integers: cores 1–1024, mem 1–8192 GB, GPUs 0–64, min ≤ max.
- `jobs_directory`: a plain absolute path, no `.`/`..`.

Because of the character sets, no value can contain quotes, braces, `#`, `:`,
whitespace or newlines, so there is no YAML, Jinja or shell injection.
Vault values are additionally written as `!unsafe '...'`, so Ansible never
templates them. Commands are run with argument lists, never a shell.
Plaintext reaches `ansible-vault` only through stdin and stdout.

## Idempotency and safety

- If nothing changed, there is no diff and no PR. Vault files are compared
  decrypted and only re-encrypted when a value changed.
- Each PR body and commit carries `BYOC-State: <HMAC>` of the desired state,
  keyed with `BYOC_FINGERPRINT_KEY` (or the vault password). When it equals
  the open PR's value, nothing is pushed. A changed preference force-pushes
  `byoc/byoc_<id>` and edits the same PR.
- Invalid preferences are reported (exit 1) and never remove a working
  configuration. Removal happens only when the user disables BYOC or Galaxy
  answers 404 for the user. 401, 403 and 5xx abort the run.
- `--max-prs` (default 20) caps PRs per run.

## Usage

```bash
pip install -r scripts/byoc/requirements.txt
export ANSIBLE_VAULT_PASSWORD_FILE=.vault_password
export GALAXY_URL=https://usegalaxy.eu GALAXY_API_KEY=...   # admin key
python scripts/byoc/byoc_sync.py --dry-run                  # print diffs
python scripts/byoc/byoc_sync.py --repo usegalaxy-eu/infrastructure-playbook --base master
python scripts/byoc/byoc_sync.py --prefs-json export.json --dry-run  # offline
```

`.github/workflows/byoc-sync.yml` runs it daily, but only in
`usegalaxy-eu/infrastructure-playbook` and only once the `BYOC_*` secrets
exist.

## Tests

```bash
pip install -r scripts/byoc/requirements.txt -r scripts/byoc/requirements-tpv.txt
python -m pytest -q scripts/byoc/tests
```

The tests use no network and only fake credentials. They run against the
repository's real `job_conf.yml`, `destinations.yml.j2`, `mq.yml` and
`job_conf.yml.j2`, with throw-away vaults and a fake vault password, a local
bare git remote, and a fake `gh` and Galaxy API.

## Open questions for the usegalaxy.eu admins

1. **Reading passwords.** Galaxy 26.1 (usegalaxy.eu today) returns `password`
   preferences to admins via `information/inputs`. Galaxy 26.2 masks every
   password/secret preference (`{"is_set": true}`). After the upgrade, CI
   needs another path: a read-only DB query on the head node
   (`--prefs-json` from a gxadmin export), or storing the credentials in
   Galaxy's vault and reading them there.
2. **Scale.** The API path makes one request per user. A DB/gxadmin query
   filtered on `extra_user_preferences LIKE '%byoc_pulsar|transport%'` is far
   cheaper on usegalaxy.eu.
3. **Who may onboard?** Today any user can open a PR request by filling in
   the form, and the admin is the gate. A role or group check could filter
   earlier.
4. **Public data.** PRs and Action logs in the public repo show the relay
   host name or RabbitMQ user and the Galaxy encoded user id (no email).
   Is that acceptable?
5. **Relay vault location.** Relay secrets go to
   `group_vars/pulsar_relay/vault.yml` like NEMO's, which the stargate
   relay host also loads. A dedicated `secret_group_vars/byoc.yml` loaded
   only by `sn09.yml` would be tighter.
6. **Pulsar side defaults.** `pulsar_default` assumes the ViraaS layout
   (`/data/share/staging`, `manager: production` for MQ). The SRC component
   stages in `/data/pulsarstr` (hence the optional `jobs_directory`). Should
   singularity cache paths and the manager name also be per-user options?
7. **Vault conflicts.** Concurrent PRs all rewrite the same encrypted files.
   After one is merged, the next run regenerates the others (each branch is
   always rebuilt from the base branch).
8. **require vs prefer.** As with `remote_resources`, an onboarded user's
   jobs *require* their Pulsar, so tools exceeding its limits fail. `prefer`
   would fall back to usegalaxy.eu but depends on the ranking function
   (`weighted_random_sampling`).
9. **Multiple job handlers.** Every handler that loads a relay runner
   receives every status update, and Galaxy 26.x can then finish a job
   twice. The existing `pulsar_eu_nemo*` runners share this. Per-user relay
   runners need a single-handler assignment or a Galaxy fix.
10. **Lost status updates after a handler restart.** Galaxy keeps the relay
    cursor in memory only, and `PULSAR_PARAM_SPECS` does not accept
    `relay_cursor_path` / `relay_handler_id`, so the generated runner cannot
    set them either. Upstream Galaxy work is needed.
11. **Manager name.** If BYOC Pulsars use a non-default manager (as upstream
    BYOC galaxyproject/galaxy#22781 does), the form needs a field and the
    relay runner a `manager` param.
12. **Credential charset.** The form follows `configure.py`
    (`[A-Za-z0-9._~+=@%-]{16,128}`) plus a YAML round-trip check. The relay
    review recommends tightening both sides to `[A-Za-z0-9]{24,64}`,
    generated rather than invented. That is a one-line change in
    `RELAY_PASSWORD_RE` plus the form label.
