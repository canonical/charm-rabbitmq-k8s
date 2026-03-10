# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for Jubilant-based functional tests."""

from __future__ import (
    annotations,
)

import json
import time
from pathlib import (
    Path,
)

import jubilant

RABBITMQ_CONTAINER = "rabbitmq"


def wait_for_app(
    juju: jubilant.Juju, app_name: str, units: int
) -> jubilant.Status:
    """Wait until the application is active, idle, and at the expected scale."""

    def is_ready(status: jubilant.Status) -> bool:
        app = status.apps.get(app_name)
        if app is None:
            return False
        return (
            len(app.units) == units
            and jubilant.all_active(status, app_name)
            and jubilant.all_agents_idle(status, app_name)
        )

    return juju.wait(is_ready, timeout=20 * 60)


def deploy_local(
    juju: jubilant.Juju,
    app_name: str,
    charm_file: Path,
    rabbitmq_image: str,
    base: str,
    units: int = 1,
) -> jubilant.Status:
    """Deploy the local charm artifact and wait for it to settle."""
    juju.deploy(
        str(charm_file),
        app=app_name,
        base=base,
        num_units=units,
        resources={"rabbitmq-image": rabbitmq_image},
        trust=True,
    )
    return wait_for_app(juju, app_name, units)


def deploy_stable(
    juju: jubilant.Juju,
    app_name: str,
    stable_channel: str,
    base: str,
    units: int = 1,
) -> jubilant.Status:
    """Deploy the charm from Charmhub stable and wait for it to settle."""
    juju.deploy(
        app_name,
        app=app_name,
        base=base,
        channel=stable_channel,
        num_units=units,
        trust=True,
    )
    return wait_for_app(juju, app_name, units)


def run_action(
    juju: jubilant.Juju,
    unit_name: str,
    action_name: str,
    params: dict | None = None,
) -> jubilant.Task:
    """Run an action and assert that it completed successfully."""
    task = juju.run(unit_name, action_name, params or {})
    assert task.success, task
    return task


def expected_rabbit_node(app_name: str, unit_name: str) -> str:
    """Return the RabbitMQ node name for a Juju unit."""
    return f"rabbit@{unit_name.replace('/', '-')}.{app_name}-endpoints"


def cluster_status(juju: jubilant.Juju, unit_name: str) -> dict:
    """Return parsed cluster status from inside the workload container."""
    output = juju.ssh(
        unit_name,
        "rabbitmqctl",
        "cluster_status",
        "--formatter=json",
        container=RABBITMQ_CONTAINER,
    )
    return json.loads(output)


def wait_for_running_nodes(
    juju: jubilant.Juju,
    unit_name: str,
    expected_nodes: set[str],
    timeout: int = 180,
    interval: int = 5,
) -> dict:
    """Wait until RabbitMQ reports the expected running nodes."""
    deadline = time.monotonic() + timeout
    last_status = {}
    while time.monotonic() < deadline:
        last_status = cluster_status(juju, unit_name)
        if set(last_status["running_nodes"]) == expected_nodes:
            return last_status
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for running nodes {expected_nodes}; "
        f"last status was {last_status}"
    )
