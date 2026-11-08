"""Validation of user preferences, including injection attempts."""

import pytest
from byoc_sync import ByocValidationError, normalize_relay_url, validate
from conftest import MQ_USER, RELAY_USER


def test_mq_valid(mq_prefs):
    """Valid mq preferences give a config with ints and the username."""
    cfg = validate(MQ_USER, mq_prefs)
    assert cfg.transport == "mq"
    assert cfg.mq_username == "mypulsarendpoint"
    assert (cfg.max_accepted_cores, cfg.max_accepted_mem) == (8, 31)
    assert cfg.slug == "byoc_" + MQ_USER
    assert cfg.tag == "byoc-" + MQ_USER


def test_relay_valid(relay_prefs):
    """Valid relay preferences are normalized."""
    cfg = validate(RELAY_USER, relay_prefs)
    assert cfg.transport == "relay"
    assert cfg.relay_url == "https://relay.example.org"
    assert cfg.relay_topic_prefix == "src_user1"
    assert cfg.jobs_directory == "/data/pulsarstr"


def test_repr_hides_secrets(mq_prefs, relay_prefs):
    """Secrets never show up in repr (logs, tracebacks)."""
    for user, prefs in ((MQ_USER, mq_prefs), (RELAY_USER, relay_prefs)):
        text = repr(validate(user, prefs))
        assert prefs.get("password", "-" * 20) not in text
        assert prefs.get("relay_password", "-" * 20) not in text
        assert "galaxy-pulsar" not in text


@pytest.mark.parametrize("transport", [None, "", "none", "None", "disabled"])
def test_disabled(mq_prefs, transport):
    """Without an enabled transport the user has no BYOC config."""
    mq_prefs["transport"] = transport
    assert validate(MQ_USER, mq_prefs) is None


def test_old_form_without_transport_is_ignored(mq_prefs):
    """Users of the old form (no transport) are not onboarded silently."""
    del mq_prefs["transport"]
    assert validate(MQ_USER, mq_prefs) is None


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://relay.example.org", "https://relay.example.org"),
        ("https://relay.example.org/", "https://relay.example.org"),
        ("https://RELAY.example.org:443", "https://relay.example.org"),
        ("https://relay.example.org:8443", "https://relay.example.org:8443"),
        (
            "https://pulsarrelay.srcdevops.src.surf-hosted.nl",
            "https://pulsarrelay.srcdevops.src.surf-hosted.nl",
        ),
    ],
)
def test_relay_url_normalized(url, expected):
    """Only the origin is kept."""
    assert normalize_relay_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "",
        "http://relay.example.org",
        "ftp://relay.example.org",
        "relay.example.org",
        "https://relay.example.org/api/v1",
        "https://relay.example.org?x=1",
        "https://relay.example.org#frag",
        "https://user:pw@relay.example.org",
        "https://relay.example.org:99999",
        "https://relay.example.org:abc",
        "https://10.0.0.1",
        "https://[::1]",
        "https://localhost",
        "https://relay",
        "https://relay.example.123",
        "https://stargate.galaxyproject.eu",
        "https://usegalaxy.eu",
        "https://mq.bi.privat",
        "https://relay.internal",
        "https://relay.example.org/{{ lookup('pipe','id') }}",
        "https://relay.example.org\n  evil: 1",
        'https://relay.example.org"',
        "https://reläy.example.org",
        "https://-relay.example.org",
        "https://relay.example.org/$(id)",
    ],
)
def test_relay_url_rejected(url):
    """Anything but a public https origin is rejected."""
    with pytest.raises(ByocValidationError):
        normalize_relay_url(url)


INJECTIONS = [
    "{{ lookup('pipe', 'id') }}",
    "{% raw %}x{% endraw %}",
    "a'; rm -rf / #'",
    'a"b',
    "abc\ndef: 1",
    "$(id)xxxxxxxxxxxxxxx",
    "`id`xxxxxxxxxxxxxxxx",
    "x" * 20 + " " + "y",
    "x: y" + "z" * 20,
    "!!python/object:os.system",
    "&anchor" + "x" * 20,
    "*alias" + "x" * 20,
]


@pytest.mark.parametrize("payload", INJECTIONS)
@pytest.mark.parametrize(
    "field",
    [
        "relay_url",
        "relay_username",
        "relay_password",
        "relay_topic_prefix",
        "jobs_directory",
    ],
)
def test_relay_injection_rejected(relay_prefs, field, payload):
    """YAML, Jinja and shell payloads never pass validation."""
    relay_prefs[field] = payload
    with pytest.raises(ByocValidationError):
        validate(RELAY_USER, relay_prefs)


@pytest.mark.parametrize("payload", INJECTIONS)
@pytest.mark.parametrize("field", ["username", "password"])
def test_mq_injection_rejected(mq_prefs, field, payload):
    """YAML, Jinja and shell payloads never pass validation (mq)."""
    mq_prefs[field] = payload
    with pytest.raises(ByocValidationError):
        validate(MQ_USER, mq_prefs)


@pytest.mark.parametrize(
    "password,ok",
    [
        ("a" * 16, True),
        ("a" * 128, True),
        ("A1._~+=@%-bcdefgh", True),
        ("a" * 15, False),
        ("a" * 129, False),
        ("abc,defghijklmnop", False),
        ("abc:defghijklmnop", False),
        ("abc defghijklmnop", False),
        (" abcdefghijklmnop", False),
        ("1234567890123456", False),  # YAML would read an int
        ("0123456701234567", False),  # YAML 1.1 octal
        ("1.5e+10000000000", False),  # YAML float
        ("@abcdefghijklmnop", False),  # YAML reserved indicator
        ("%abcdefghijklmnop", False),  # YAML directive indicator
        ("-abcdefghijklmnop", True),
        ("~abcdefghijklmnop", True),
    ],
)
def test_relay_password_rules(relay_prefs, password, ok):
    """configure.py pair-password rules plus YAML round-trip safety."""
    relay_prefs["relay_password"] = password
    if ok:
        assert validate(RELAY_USER, relay_prefs).relay_password == password
    else:
        with pytest.raises(ByocValidationError):
            validate(RELAY_USER, relay_prefs)


@pytest.mark.parametrize(
    "username,ok",
    [
        ("galaxy-pulsar", True),
        ("a.b_c-d", True),
        ("ab", False),
        ("x" * 51, False),
        ("-abc", False),
        ("_abc", False),
        ("abc def", False),
    ],
)
def test_relay_username_rules(relay_prefs, username, ok):
    """configure.py PAIR_USERNAME_PATTERN."""
    relay_prefs["relay_username"] = username
    if ok:
        assert validate(RELAY_USER, relay_prefs)
    else:
        with pytest.raises(ByocValidationError):
            validate(RELAY_USER, relay_prefs)


@pytest.mark.parametrize(
    "field,value",
    [
        ("username", "galaxy_eosc01"),
        ("username", "mqadmin"),
        ("username", "Upper"),
        ("username", "ab"),
        ("password", "short123"),
        ("password", "has-dash-1234"),
        ("password", ""),
    ],
)
def test_mq_rejected(mq_prefs, field, value):
    """Check RabbitMQ names (lower case, unreserved) and passwords."""
    mq_prefs[field] = value
    with pytest.raises(ByocValidationError):
        validate(MQ_USER, mq_prefs)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_accepted_cores", 0),
        ("max_accepted_cores", 100000),
        ("max_accepted_cores", True),
        ("max_accepted_cores", "8.5"),
        ("max_accepted_cores", 8.5),
        ("max_accepted_cores", "-1"),
        ("max_accepted_cores", None),
        ("max_accepted_mem", "1e3"),
        ("max_accepted_gpus", 65),
        ("min_accepted_gpus", 2),  # > max_accepted_gpus (0)
    ],
)
def test_resources_rejected(mq_prefs, field, value):
    """Resource limits must be integers in range with min <= max."""
    mq_prefs[field] = value
    with pytest.raises(ByocValidationError):
        validate(MQ_USER, mq_prefs)


def test_float_integral_accepted(mq_prefs):
    """Galaxy 'number' inputs may come back as 8.0."""
    mq_prefs["max_accepted_cores"] = 8.0
    assert validate(MQ_USER, mq_prefs).max_accepted_cores == 8


@pytest.mark.parametrize(
    "value", ["data", "/data/../etc", "/data/./x", "/data//x", "/"]
)
def test_jobs_directory_rejected(relay_prefs, value):
    """jobs_directory is a plain absolute path."""
    relay_prefs["jobs_directory"] = value
    with pytest.raises(ByocValidationError):
        validate(RELAY_USER, relay_prefs)


@pytest.mark.parametrize(
    "user_id", ["", "123", "0123456789ABCDEF", "../../etc", "0" * 17]
)
def test_bad_user_id(mq_prefs, user_id):
    """Only Galaxy encoded ids are accepted (they end up in names)."""
    with pytest.raises(ByocValidationError):
        validate(user_id, mq_prefs)


def test_transport_unknown(mq_prefs):
    """Only mq and relay are transports."""
    mq_prefs["transport"] = "rest"
    with pytest.raises(ByocValidationError):
        validate(MQ_USER, mq_prefs)


def test_masked_password_reported(relay_prefs):
    """Galaxy's secret placeholder gives a clear error, not a config."""
    relay_prefs["relay_password"] = "__SECRET_PLACEHOLDER__"
    with pytest.raises(ByocValidationError, match="masked"):
        validate(RELAY_USER, relay_prefs)
