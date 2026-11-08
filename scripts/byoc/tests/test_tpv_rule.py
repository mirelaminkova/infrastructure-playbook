"""The byoc_pulsar rule in tool_defaults.yml.j2, run against real TPV."""

import re
import types

import pytest
import yaml
from conftest import REPO

entities = pytest.importorskip("tpv.core.entities")
SchedulingTags = entities.SchedulingTags


def byoc_rule():
    """Return the byoc_pulsar rule from tool_defaults.yml.j2."""
    text = (REPO / "files/galaxy/tpv/tool_defaults.yml.j2").read_text()
    text = re.sub(r"\{%.*?%\}", "", text, flags=re.S)
    text = re.sub(r"\{\{.*?\}\}", "x", text, flags=re.S)
    rules = yaml.safe_load(text)["tools"]["default"]["rules"]
    return next(r for r in rules if r["id"] == "byoc_pulsar")


class Entity:
    """Just enough of a TPV entity for the rule."""

    def __init__(self, tags):
        """Start with the given scheduling tags."""
        self.tpv_tags = tags


class Destination:
    """Just enough of a TPV destination for the rule."""

    def __init__(self, require):
        """Require the given tags."""
        self.tpv_dest_tags = SchedulingTags(require=require)


def run_rule(prefs, dests, tags=None, job_wrapper=object()):
    """Evaluate the rule's if/execute like TPV does; return the tags."""
    rule = byoc_rule()
    context = {
        "job_wrapper": job_wrapper,
        "user": types.SimpleNamespace(id=7, extra_preferences=prefs),
        "entity": Entity(tags or SchedulingTags()),
        "app": types.SimpleNamespace(
            security=types.SimpleNamespace(encode_id=lambda i: f"enc{i}")
        ),
        "mapper": types.SimpleNamespace(
            destinations={str(i): Destination(d) for i, d in enumerate(dests)}
        ),
    }
    lines = rule["if"].strip().split("\n")
    code = "\n".join(lines[:-1]) + f"\n__result = {lines[-1]}\n"
    exec(code, context)  # noqa: S102 - trusted repo content in a test
    if context["__result"]:
        exec(rule["execute"], context)  # noqa: S102
    return context["entity"].tpv_tags


def test_routes_user_with_destination():
    """An onboarded user's jobs require their own tag."""
    tags = run_rule({"byoc_pulsar|transport": "relay"}, [["byoc-enc7"]])
    assert tags.require == ["byoc-enc7"]
    assert tags.match(SchedulingTags(require=["byoc-enc7"]))
    assert not tags.match(SchedulingTags(require=["byoc-enc8"]))


def test_other_users_never_match_byoc_destination():
    """A job without the tag cannot land on someone's BYOC destination."""
    byoc = SchedulingTags(require=["byoc-enc7"])
    assert not SchedulingTags().match(byoc)
    assert not SchedulingTags(require=["nemo-pulsar"]).match(byoc)


@pytest.mark.parametrize(
    "prefs",
    [
        {},
        {"byoc_pulsar|transport": "none"},
        {
            "byoc_pulsar|transport": "mq",
            "distributed_compute|remote_resources": "nemo-pulsar",
        },
    ],
)
def test_not_applied(prefs):
    """Disabled BYOC or another chosen remote resource: no tag."""
    tags = run_rule(prefs, [["byoc-enc7"]])
    assert not tags.require


def test_not_applied_before_merge():
    """Prefs set but no destination yet (PR not merged): no tag."""
    tags = run_rule({"byoc_pulsar|transport": "mq"}, [["nemo-pulsar"]])
    assert not tags.require


def test_internal_tools_stay_local():
    """Tools needing Galaxy's environment are never sent to BYOC."""
    tags = run_rule(
        {"byoc_pulsar|transport": "mq"},
        [["byoc-enc7"]],
        tags=SchedulingTags(require=["internal"]),
    )
    assert tags.require == ["internal"]


def test_dry_run_mode_skipped():
    """TPV dry-run (no job wrapper) does not apply the rule."""
    tags = run_rule(
        {"byoc_pulsar|transport": "mq"}, [["byoc-enc7"]], job_wrapper=None
    )
    assert not tags.require
