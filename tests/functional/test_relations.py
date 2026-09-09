# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering credential lifecycle and relation coherence."""

from __future__ import (
    annotations,
)

import uuid
from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    amqp_uri,
    delete_pod,
    deploy_local,
    leader_unit,
    loadbalancer_address,
    run_action,
    start_perf_test,
    wait_for_app,
    wait_for_pod_ready,
)


def test_service_account_credentials_enable_connectivity(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Service account credentials allow a perf-test client to connect."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    loadbalancer_ip = loadbalancer_address(juju, app_name)
    leader = leader_unit(juju, app_name)
    username = f"rel-conn-{uuid.uuid4().hex[:8]}"
    vhost = f"rel-conn-{uuid.uuid4().hex[:8]}"
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

    profile = {
        "queues": 1,
        "producers": 1,
        "consumers": 1,
        "rate": 10,
        "confirm": 5,
        "qos": 5,
        "size": 256,
        "duration": 30,
        "start_delay": 1,
        "heartbeat_threads": 1,
        "consumer_pools": 1,
        "nio_threads": 1,
    }
    perf_test = start_perf_test(
        uri=uri,
        profile=profile,
        test_id=f"rel-conn-{uuid.uuid4().hex[:8]}",
        queue_prefix="rel-conn",
    )
    stdout, stderr = "", ""
    try:
        stdout, stderr = perf_test.communicate(
            timeout=profile["duration"] + 60
        )
    finally:
        if perf_test.poll() is None:
            perf_test.terminate()
            perf_test.wait(timeout=30)

    assert perf_test.returncode == 0, (
        "rabbitmq-perf-test failed with service account credentials\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )


def test_service_account_credentials_survive_pod_restart(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Credentials remain valid after the backing pod is restarted."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    loadbalancer_ip = loadbalancer_address(juju, app_name)
    leader = leader_unit(juju, app_name)
    username = f"rel-surv-{uuid.uuid4().hex[:8]}"
    vhost = f"rel-surv-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": username, "vhost": vhost},
    )

    delete_pod(juju, f"{app_name}/0")
    wait_for_pod_ready(juju, f"{app_name}/0")
    wait_for_app(juju, app_name, units=1)

    # Re-create credentials after restart (single-unit restart loses users)
    leader = leader_unit(juju, app_name)
    post_restart_username = f"rel-surv-post-{uuid.uuid4().hex[:8]}"
    post_restart_vhost = f"rel-surv-post-{uuid.uuid4().hex[:8]}"
    service_account = run_action(
        juju,
        leader,
        "get-service-account",
        {"username": post_restart_username, "vhost": post_restart_vhost},
    )

    uri = amqp_uri(
        username=service_account.results["username"],
        password=service_account.results["password"],
        host=loadbalancer_ip,
        vhost=service_account.results["vhost"],
    )

    profile = {
        "queues": 1,
        "producers": 1,
        "consumers": 1,
        "rate": 10,
        "confirm": 5,
        "qos": 5,
        "size": 256,
        "duration": 30,
        "start_delay": 1,
        "heartbeat_threads": 1,
        "consumer_pools": 1,
        "nio_threads": 1,
    }
    perf_test = start_perf_test(
        uri=uri,
        profile=profile,
        test_id=f"rel-surv-{uuid.uuid4().hex[:8]}",
        queue_prefix="rel-surv",
    )
    stdout, stderr = "", ""
    try:
        stdout, stderr = perf_test.communicate(
            timeout=profile["duration"] + 60
        )
    finally:
        if perf_test.poll() is None:
            perf_test.terminate()
            perf_test.wait(timeout=30)

    assert perf_test.returncode == 0, (
        "rabbitmq-perf-test failed after pod restart\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )


def test_repeated_service_account_creation(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Calling get-service-account twice for the same user succeeds both times."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    leader = leader_unit(juju, app_name)
    username = f"idem-{uuid.uuid4().hex[:8]}"
    vhost = f"idem-{uuid.uuid4().hex[:8]}"
    params = {"username": username, "vhost": vhost}

    first = run_action(juju, leader, "get-service-account", params)
    second = run_action(juju, leader, "get-service-account", params)

    assert first.results["username"] == username
    assert second.results["username"] == username
    assert second.results[
        "password"
    ], "Second call should return valid credentials"
