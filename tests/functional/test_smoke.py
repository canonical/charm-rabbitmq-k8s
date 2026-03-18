# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional smoke tests: install, remove, reinstall, and isolation."""

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
    k8s_pods_with_label,
    k8s_service_exists,
    run_action,
    wait_for_app_removed,
    wait_for_k8s_service_gone,
)


def test_clean_removal_leaves_no_k8s_garbage(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Deploy, verify LB service exists, remove, and assert clean teardown."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    lb_service = f"{app_name}-lb"
    assert k8s_service_exists(
        juju, lb_service
    ), f"Expected LoadBalancer service {lb_service!r} to exist after deploy"

    juju.remove_application(app_name, destroy_storage=True)
    wait_for_app_removed(juju, app_name)
    wait_for_k8s_service_gone(juju, lb_service)

    assert not k8s_service_exists(
        juju, lb_service
    ), f"LoadBalancer service {lb_service!r} still exists after removal"
    pods = k8s_pods_with_label(juju, f"app.kubernetes.io/name={app_name}")
    assert not pods, f"Pods still running after removal: {pods}"


def test_reinstall_after_clean_removal(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Reinstall after the previous test's clean removal."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    operator_info = run_action(juju, f"{app_name}/0", "get-operator-info")
    assert operator_info.results["operator-user"] == "operator"
    assert operator_info.results["operator-password"]


def test_two_apps_isolated_clusters(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Two RabbitMQ apps deployed side-by-side form independent clusters."""
    app_b = "rabbitmq-k8s-b"
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)
    deploy_local(juju, app_b, charm_file, rabbitmq_image, base)

    try:
        status_a = cluster_status(juju, f"{app_name}/0")
        status_b = cluster_status(juju, f"{app_b}/0")

        nodes_a = set(status_a.get("running_nodes", []))
        nodes_b = set(status_b.get("running_nodes", []))

        expected_a = {expected_rabbit_node(app_name, f"{app_name}/0")}
        expected_b = {expected_rabbit_node(app_b, f"{app_b}/0")}

        assert (
            nodes_a == expected_a
        ), f"App A cluster has unexpected nodes: {nodes_a}"
        assert (
            nodes_b == expected_b
        ), f"App B cluster has unexpected nodes: {nodes_b}"
        assert (
            not nodes_a & nodes_b
        ), f"Clusters share nodes: {nodes_a & nodes_b}"
    finally:
        juju.remove_application(app_b, destroy_storage=True)
        wait_for_app_removed(juju, app_b)
