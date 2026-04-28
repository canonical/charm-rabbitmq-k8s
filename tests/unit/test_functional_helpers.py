# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for functional test helpers."""

from tests.functional import (
    helpers,
)


class FakeJuju:
    """Small fake that records Juju SSH calls made by helpers."""

    def __init__(self):
        self.ssh_calls = []

    def ssh(self, *args, **kwargs):
        """Record a Juju SSH invocation."""
        self.ssh_calls.append((args, kwargs))
        return ""


def test_hold_rabbitmq_service_stopped_stops_checks_before_service():
    """The observability fault injection avoids pod force deletion."""
    juju = FakeJuju()

    helpers.hold_rabbitmq_service_stopped(juju, "rabbitmq-k8s/0")

    assert juju.ssh_calls == [
        (
            (
                "rabbitmq-k8s/0",
                "/charm/bin/pebble",
                "stop-checks",
                "alive",
                "ready",
            ),
            {"container": "rabbitmq"},
        ),
        (
            (
                "rabbitmq-k8s/0",
                "/charm/bin/pebble",
                "stop",
                "rabbitmq",
            ),
            {"container": "rabbitmq"},
        ),
    ]


def test_release_rabbitmq_service_stop_starts_service_and_checks():
    """The helper restores the service and checks it deliberately disabled."""
    juju = FakeJuju()

    helpers.release_rabbitmq_service_stop(juju, "rabbitmq-k8s/0")

    assert juju.ssh_calls == [
        (
            (
                "rabbitmq-k8s/0",
                "/charm/bin/pebble",
                "start",
                "rabbitmq",
            ),
            {"container": "rabbitmq"},
        ),
        (
            (
                "rabbitmq-k8s/0",
                "/charm/bin/pebble",
                "start-checks",
                "alive",
                "ready",
            ),
            {"container": "rabbitmq"},
        ),
    ]
