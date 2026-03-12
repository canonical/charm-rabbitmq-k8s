# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering RabbitMQ observability with COS Lite."""

from __future__ import (
    annotations,
)

import time
from pathlib import (
    Path,
)

import jubilant
import pytest

from .helpers import (
    alertmanager_alerts,
    deploy_cos,
    deploy_local,
    hold_rabbitmq_service_stopped,
    integrate_observability,
    prometheus_alerts,
    prometheus_query,
    prometheus_rules,
    raw_status,
    release_rabbitmq_service_stop,
    run_action,
    wait_for_loki_logs,
    wait_for_metrics_endpoint_data,
    wait_for_observability_integrations,
    wait_for_prometheus_query_result,
)


def _wait_until(
    predicate,
    *,
    timeout: int,
    failure_message: str,
    interval: int = 5,
) -> None:
    """Poll until a predicate succeeds or time out."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(failure_message)


@pytest.fixture(scope="module")
def rabbitmq_with_cos(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
    cos_channel: str,
) -> str:
    """Deploy COS Lite, deploy RabbitMQ, and integrate observability."""
    deploy_cos(juju, cos_channel)
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)
    integrate_observability(juju, app_name)
    wait_for_observability_integrations(juju, app_name)
    wait_for_metrics_endpoint_data(juju, f"{app_name}/0")
    return app_name


def test_prometheus_scrapes_rabbitmq_metrics(
    juju: jubilant.Juju, rabbitmq_with_cos: str
) -> None:
    """Prometheus should scrape RabbitMQ and return broker metrics."""
    app_name = rabbitmq_with_cos

    up_result = wait_for_prometheus_query_result(
        juju, f'up{{juju_application="{app_name}"}}'
    )
    assert all(sample["value"][1] == "1" for sample in up_result)

    build_info = prometheus_query(
        juju, f'rabbitmq_build_info{{juju_application="{app_name}"}}'
    )
    assert build_info, build_info


def test_loki_receives_rabbitmq_logs(
    juju: jubilant.Juju, rabbitmq_with_cos: str
) -> None:
    """Loki should receive RabbitMQ log lines after integration."""
    app_name = rabbitmq_with_cos
    run_action(juju, f"{app_name}/0", "get-operator-info")

    streams = wait_for_loki_logs(juju, f'{{juju_application="{app_name}"}}')
    assert any(stream["values"] for stream in streams), streams


def test_rabbitmq_rule_group_is_loaded(
    juju: jubilant.Juju, rabbitmq_with_cos: str
) -> None:
    """Prometheus should load the bundled RabbitMQ alert rules."""
    groups = prometheus_rules(juju)["groups"]
    rabbitmq_groups = []
    for group in groups:
        rule_names = {
            rule.get("name") or rule.get("alert")
            for rule in group.get("rules", [])
        }
        if {"RabbitMQNodeDown", "RabbitMQDiskAlarm"} <= rule_names:
            rabbitmq_groups.append(group)
    assert rabbitmq_groups, groups

    rule_names = {
        rule.get("name") or rule.get("alert")
        for rule in rabbitmq_groups[0]["rules"]
    }
    assert "RabbitMQNodeDown" in rule_names
    assert "RabbitMQDiskAlarm" in rule_names


def test_rabbitmq_node_down_alert_fires_and_clears(
    juju: jubilant.Juju, rabbitmq_with_cos: str
) -> None:
    """Holding the RabbitMQ service down should trigger and then clear the alert."""
    unit_name = f"{rabbitmq_with_cos}/0"

    hold_rabbitmq_service_stopped(juju, unit_name)
    wait_for_prometheus_query_result(
        juju, f'up{{juju_application="{rabbitmq_with_cos}"}} == 0'
    )
    _wait_until(
        lambda: any(
            alert.get("labels", {}).get("alertname") == "RabbitMQNodeDown"
            and alert.get("labels", {}).get("juju_application")
            == rabbitmq_with_cos
            and alert.get("state") == "firing"
            for alert in prometheus_alerts(juju)
        ),
        timeout=8 * 60,
        failure_message="Timed out waiting for RabbitMQNodeDown in Prometheus",
    )
    _wait_until(
        lambda: any(
            alert.get("labels", {}).get("alertname") == "RabbitMQNodeDown"
            and alert.get("labels", {}).get("juju_application")
            == rabbitmq_with_cos
            for alert in alertmanager_alerts(juju)
        ),
        timeout=3 * 60,
        failure_message="Timed out waiting for RabbitMQNodeDown in Alertmanager",
    )

    release_rabbitmq_service_stop(juju, unit_name)
    _wait_until(
        lambda: (
            lambda status: (
                status["applications"][rabbitmq_with_cos]["units"][unit_name][
                    "workload-status"
                ]["current"]
                == "active"
                and status["applications"][rabbitmq_with_cos]["units"][
                    unit_name
                ]["juju-status"]["current"]
                == "idle"
            )
        )(raw_status(juju)),
        timeout=3 * 60,
        failure_message="Timed out waiting for RabbitMQ to recover",
    )
    _wait_until(
        lambda: (
            (
                samples := prometheus_query(
                    juju, f'up{{juju_application="{rabbitmq_with_cos}"}}'
                )
            )
            and all(sample["value"][1] == "1" for sample in samples)
        ),
        timeout=5 * 60,
        failure_message="Timed out waiting for Prometheus scrape recovery",
    )
    _wait_until(
        lambda: not any(
            alert.get("labels", {}).get("alertname") == "RabbitMQNodeDown"
            and alert.get("labels", {}).get("juju_application")
            == rabbitmq_with_cos
            for alert in alertmanager_alerts(juju)
        ),
        timeout=5 * 60,
        failure_message="Timed out waiting for RabbitMQNodeDown to clear",
    )
