# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering RabbitMQ resiliency under quorum queue load."""

from __future__ import (
    annotations,
)

import time
import uuid
from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    amqp_uri,
    delete_all_app_pods,
    delete_pod,
    deploy_local,
    expected_rabbit_node,
    leader_unit,
    loadbalancer_address,
    resilience_profile,
    run_action,
    start_perf_test,
    wait_for_app,
    wait_for_pod_ready,
    wait_for_running_nodes,
)


def test_resilience_during_scale_events(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Sustain quorum queue traffic while the cluster scales down and recovers."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base, units=3)

    loadbalancer_ip = loadbalancer_address(juju, app_name)
    leader = leader_unit(juju, app_name)
    username = f"resilience-{uuid.uuid4().hex[:8]}"
    vhost = f"resilience-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": username, "vhost": vhost},
    )
    uri = amqp_uri(
        username=service_account.results["username"],
        password=service_account.results["password"],
        host=loadbalancer_ip,
        vhost=service_account.results["vhost"],
    )

    profile = resilience_profile()
    perf_test = start_perf_test(
        uri=uri,
        profile=profile,
        test_id=f"resilience-{uuid.uuid4().hex[:8]}",
        queue_prefix="resilience",
    )
    stdout, stderr = "", ""
    try:
        time.sleep(20)

        juju.cli("scale-application", app_name, "2")
        wait_for_app(juju, app_name, units=2)
        status = juju.status()
        surviving_units = sorted(status.apps[app_name].units)
        expected_nodes = {
            expected_rabbit_node(app_name, unit) for unit in surviving_units
        }
        wait_for_running_nodes(juju, surviving_units[0], expected_nodes)

        juju.add_unit(app_name, num_units=1)
        wait_for_app(juju, app_name, units=3)
        status = juju.status()
        current_units = sorted(status.apps[app_name].units)
        expected_nodes = {
            expected_rabbit_node(app_name, unit) for unit in current_units
        }
        wait_for_running_nodes(juju, current_units[0], expected_nodes)

        stdout, stderr = perf_test.communicate(
            timeout=profile["duration"] + 60
        )
    finally:
        if perf_test.poll() is None:
            perf_test.terminate()
            perf_test.wait(timeout=30)

    assert perf_test.returncode == 0, (
        "rabbitmq-perf-test failed during resilience test\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )


def test_resilience_during_pod_restarts(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Sustain quorum queue traffic while RabbitMQ pods are recreated."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base, units=3)

    loadbalancer_ip = loadbalancer_address(juju, app_name)
    leader = leader_unit(juju, app_name)
    username = f"restart-{uuid.uuid4().hex[:8]}"
    vhost = f"restart-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": username, "vhost": vhost},
    )
    uri = amqp_uri(
        username=service_account.results["username"],
        password=service_account.results["password"],
        host=loadbalancer_ip,
        vhost=service_account.results["vhost"],
    )

    profile = resilience_profile()
    perf_test = start_perf_test(
        uri=uri,
        profile=profile,
        test_id=f"restart-{uuid.uuid4().hex[:8]}",
        queue_prefix="restart",
    )
    stdout, stderr = "", ""
    try:
        time.sleep(20)

        for unit_name in (f"{app_name}/1", f"{app_name}/0"):
            delete_pod(juju, unit_name)
            wait_for_pod_ready(juju, unit_name)
            wait_for_app(juju, app_name, units=3)
            expected_nodes = {
                expected_rabbit_node(app_name, current)
                for current in sorted(juju.status().apps[app_name].units)
            }
            wait_for_running_nodes(juju, f"{app_name}/0", expected_nodes)

        stdout, stderr = perf_test.communicate(
            timeout=profile["duration"] + 60
        )
    finally:
        if perf_test.poll() is None:
            perf_test.terminate()
            perf_test.wait(timeout=30)

    assert perf_test.returncode == 0, (
        "rabbitmq-perf-test failed during pod restart test\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )


def test_full_cluster_crash_and_recovery(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Kill all pods simultaneously and verify cluster recovers."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base, units=3)

    loadbalancer_ip = loadbalancer_address(juju, app_name)
    leader = leader_unit(juju, app_name)
    username = f"crash-{uuid.uuid4().hex[:8]}"
    vhost = f"crash-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": username, "vhost": vhost},
    )
    uri = amqp_uri(
        username=service_account.results["username"],
        password=service_account.results["password"],
        host=loadbalancer_ip,
        vhost=service_account.results["vhost"],
    )

    profile = resilience_profile()
    perf_test = start_perf_test(
        uri=uri,
        profile=profile,
        test_id=f"crash-{uuid.uuid4().hex[:8]}",
        queue_prefix="crash",
    )
    try:
        time.sleep(20)

        # Kill all pods simultaneously
        delete_all_app_pods(juju, app_name)

        # Wait for full recovery
        wait_for_app(juju, app_name, units=3)
        status = juju.status()
        current_units = sorted(status.apps[app_name].units)
        expected_nodes = {
            expected_rabbit_node(app_name, unit) for unit in current_units
        }
        wait_for_running_nodes(juju, current_units[0], expected_nodes)
    finally:
        # Terminate the in-flight perf-test (it will have failed, expected)
        if perf_test.poll() is None:
            perf_test.terminate()
            perf_test.wait(timeout=30)

    # Re-create credentials after full crash (users may be lost)
    leader = leader_unit(juju, app_name)
    verify_username = f"crash-v-{uuid.uuid4().hex[:8]}"
    verify_vhost = f"crash-v-{uuid.uuid4().hex[:8]}"
    verify_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": verify_username, "vhost": verify_vhost},
    )
    verify_uri = amqp_uri(
        username=verify_account.results["username"],
        password=verify_account.results["password"],
        host=loadbalancer_ip,
        vhost=verify_account.results["vhost"],
    )

    # Start a new short perf-test to verify the cluster is functional
    verify_profile = dict(profile, duration=30)
    verify_perf_test = start_perf_test(
        uri=verify_uri,
        profile=verify_profile,
        test_id=f"crash-verify-{uuid.uuid4().hex[:8]}",
        queue_prefix="crash-verify",
    )
    stdout, stderr = "", ""
    try:
        stdout, stderr = verify_perf_test.communicate(
            timeout=verify_profile["duration"] + 60
        )
    finally:
        if verify_perf_test.poll() is None:
            verify_perf_test.terminate()
            verify_perf_test.wait(timeout=30)

    assert verify_perf_test.returncode == 0, (
        "rabbitmq-perf-test failed after full cluster crash recovery\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )
