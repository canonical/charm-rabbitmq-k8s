# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering stable-to-local refresh behaviour."""

from __future__ import (
    annotations,
)

import uuid
from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    cluster_status,
    deploy_stable,
    expected_rabbit_node,
    leader_unit,
    run_action,
    wait_for_app,
)


def test_refresh_from_stable_to_local(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
    stable_channel: str,
) -> None:
    """Refresh a stable deployment to the local charm artifact."""
    deploy_stable(juju, app_name, stable_channel, base)

    pre_refresh = run_action(juju, f"{app_name}/0", "get-operator-info")
    assert pre_refresh.results["operator-user"] == "operator"
    assert pre_refresh.results["operator-password"]

    # Pin the OCI image to the local build's upstream-source during
    # refresh.  The stable channel bundles its own image; we override
    # it here to test the exact image this charm revision ships with.
    juju.refresh(
        app_name,
        path=str(charm_file),
        resources={"rabbitmq-image": rabbitmq_image},
        trust=True,
    )
    wait_for_app(juju, app_name, units=1)

    post_refresh = run_action(juju, f"{app_name}/0", "get-operator-info")
    assert post_refresh.results["operator-user"] == "operator"
    assert post_refresh.results["operator-password"]

    leader = leader_unit(juju, app_name)
    username = f"refresh-{uuid.uuid4().hex[:8]}"
    vhost = f"refresh-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": username, "vhost": vhost},
    )
    assert service_account.results["username"] == username
    assert service_account.results["vhost"] == vhost
    assert service_account.results["password"]

    status = cluster_status(juju, f"{app_name}/0")
    expected_nodes = {expected_rabbit_node(app_name, f"{app_name}/0")}
    assert set(status["running_nodes"]) == expected_nodes
