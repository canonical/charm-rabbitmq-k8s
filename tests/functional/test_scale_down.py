# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering RabbitMQ scale-down behaviour."""

from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    deploy_local,
    expected_rabbit_node,
    wait_for_app,
    wait_for_running_nodes,
)


def test_scale_down_cluster_membership(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Scale from three units to one and verify removed nodes are forgotten."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base, units=3)
    juju.config(app_name, {"auto-forget-stale-nodes": "true"})
    wait_for_app(juju, app_name, units=3)

    juju.cli("scale-application", app_name, "1")
    wait_for_app(juju, app_name, units=1)

    status = juju.status()
    surviving_units = sorted(status.apps[app_name].units)
    assert len(surviving_units) == 1
    surviving_unit = surviving_units[0]
    expected_nodes = {expected_rabbit_node(app_name, surviving_unit)}
    status = wait_for_running_nodes(juju, surviving_unit, expected_nodes)
    assert set(status["running_nodes"]) == expected_nodes
