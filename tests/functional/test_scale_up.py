# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering RabbitMQ scale-up behaviour."""

from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    cluster_status,
    deploy_local,
    expected_rabbit_node,
    wait_for_app,
)


def test_scale_up_cluster_membership(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Scale from one unit to three and verify RabbitMQ cluster membership."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    juju.add_unit(app_name, num_units=2)
    wait_for_app(juju, app_name, units=3)

    status = cluster_status(juju, f"{app_name}/0")
    expected_nodes = {
        expected_rabbit_node(app_name, f"{app_name}/0"),
        expected_rabbit_node(app_name, f"{app_name}/1"),
        expected_rabbit_node(app_name, f"{app_name}/2"),
    }
    assert set(status["running_nodes"]) == expected_nodes
