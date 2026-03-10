# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering normal RabbitMQ operator tasks."""

from __future__ import (
    annotations,
)

import uuid
from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    deploy_local,
    leader_unit,
    run_action,
)


def test_normal_operations(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Verify standard operational actions against a one-unit deployment."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    operator_info = run_action(juju, f"{app_name}/0", "get-operator-info")
    assert operator_info.results["operator-user"] == "operator"
    assert operator_info.results["operator-password"]

    leader = leader_unit(juju, app_name)
    username = f"svc-{uuid.uuid4().hex[:8]}"
    vhost = f"vhost-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": username, "vhost": vhost},
    )
    assert service_account.results["username"] == username
    assert service_account.results["vhost"] == vhost
    assert service_account.results["password"]
    assert int(service_account.results["port"]) == 5672
    assert service_account.results["url"].startswith("rabbit://")

    queue_ha = run_action(
        juju,
        leader,
        "ensure-queue-ha",
        {"dry-run": True},
    )
    assert queue_ha.results["dry-run"] == "True"
    assert int(queue_ha.results["replicated-queues"]) == 0
    assert int(queue_ha.results["undersized-queues"]) == 0
