# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for resilience functional test helpers."""

from tests.functional import (
    test_resilience,
)


class FakeJuju:
    """Small fake that records Juju CLI calls."""

    def __init__(self):
        self.cli_calls = []

    def cli(self, *args):
        """Record a Juju CLI invocation."""
        self.cli_calls.append(args)


def _unit_status(
    workload_current: str,
    *,
    workload_message: str = "",
    juju_current: str = "idle",
) -> dict:
    return {
        "workload-status": {
            "current": workload_current,
            "message": workload_message,
        },
        "juju-status": {
            "current": juju_current,
        },
    }


def _status_payload(unit_status: dict) -> dict:
    return {
        "applications": {
            "rabbitmq-k8s": {
                "units": {
                    "rabbitmq-k8s/0": unit_status,
                    "rabbitmq-k8s/1": unit_status,
                    "rabbitmq-k8s/2": unit_status,
                },
            },
        },
    }


def test_stop_hook_recovery_resolves_maintenance_stop_state(monkeypatch):
    """Juju may expose failed stop hooks as maintenance while awaiting resolve."""
    juju = FakeJuju()
    statuses = [
        _status_payload(
            _unit_status(
                "maintenance",
                workload_message="stopping charm software",
            )
        ),
        _status_payload(_unit_status("active")),
    ]

    monkeypatch.setattr(test_resilience.time, "sleep", lambda _interval: None)
    monkeypatch.setattr(test_resilience.time, "monotonic", lambda: 0)
    monkeypatch.setattr(
        test_resilience,
        "status_payload",
        lambda _juju: statuses.pop(0),
    )

    test_resilience._resolve_stop_hook_failure_after_pod_recreation(
        juju,
        "rabbitmq-k8s",
        units=3,
        timeout=10,
    )

    assert juju.cli_calls == [("resolved", "--all")]
    assert statuses == []


def test_stop_hook_recovery_keeps_polling_after_resolve(monkeypatch):
    """The helper should wait for active/idle after resolving a stop hook."""
    juju = FakeJuju()
    statuses = [
        _status_payload(
            _unit_status(
                "error",
                workload_message=test_resilience.STOP_HOOK_FAILURE,
            )
        ),
        _status_payload(_unit_status("active")),
    ]

    monkeypatch.setattr(test_resilience.time, "sleep", lambda _interval: None)
    monkeypatch.setattr(test_resilience.time, "monotonic", lambda: 0)
    monkeypatch.setattr(
        test_resilience,
        "status_payload",
        lambda _juju: statuses.pop(0),
    )

    test_resilience._resolve_stop_hook_failure_after_pod_recreation(
        juju,
        "rabbitmq-k8s",
        units=3,
        timeout=10,
    )

    assert juju.cli_calls == [("resolved", "--all")]
    assert statuses == []


def test_stop_hook_recovery_returns_after_stop_hook_clears(monkeypatch):
    """Full broker recovery is handled by the following wait_for_app call."""
    juju = FakeJuju()
    statuses = [
        _status_payload(
            _unit_status(
                "maintenance",
                workload_message=test_resilience.STOP_HOOK_MAINTENANCE,
            )
        ),
        _status_payload(
            _unit_status(
                "blocked",
                workload_message="RabbitMQ not running",
            )
        ),
    ]

    monkeypatch.setattr(test_resilience.time, "sleep", lambda _interval: None)
    monkeypatch.setattr(test_resilience.time, "monotonic", lambda: 0)
    monkeypatch.setattr(
        test_resilience,
        "status_payload",
        lambda _juju: statuses.pop(0),
    )

    test_resilience._resolve_stop_hook_failure_after_pod_recreation(
        juju,
        "rabbitmq-k8s",
        units=3,
        timeout=10,
    )

    assert juju.cli_calls == [("resolved", "--all")]
    assert statuses == []
