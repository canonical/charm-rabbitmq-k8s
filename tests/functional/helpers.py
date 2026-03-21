# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for Jubilant-based functional tests."""

from __future__ import (
    annotations,
)

import json
import math
import os
import shlex
import shutil
import subprocess  # nosec B404
import time
from pathlib import (
    Path,
)
from urllib.parse import (
    quote,
)

import jubilant
import pytest

ALERTMANAGER_APP = "alertmanager"
COS_BUNDLE = "cos-lite"
GRAFANA_APP = "grafana"
LOKI_APP = "loki"
PROMETHEUS_APP = "prometheus"
RABBITMQ_CONTAINER = "rabbitmq"
RABBITMQ_FILLER_PATH = "/var/lib/rabbitmq/.functional-fill"
RABBITMQ_DATA_DIR = "/var/lib/rabbitmq"
RABBITMQ_TIMER_NOTICE = "rabbitmq.local/timer"

_RABBITMQ_STOP_PROCESSES: dict[tuple[str, str], subprocess.Popen] = {}


def status_payload(juju: jubilant.Juju) -> dict:
    """Return the full Juju status payload."""
    return json.loads(juju.cli("status", "--format=json"))


def app_exists(juju: jubilant.Juju, app_name: str) -> bool:
    """Return whether the application already exists in the model."""
    return app_name in status_payload(juju).get("applications", {})


def wait_for_model_empty(
    juju: jubilant.Juju,
    timeout: int = 10 * 60,
    interval: int = 5,
) -> None:
    """Wait until the model has no applications and no storage."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = status_payload(juju)
        apps = payload.get("applications", {})
        storage = payload.get("storage", {}).get("storage", {})
        if not apps and not storage:
            return
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for model to be empty; "
        f"apps={list(apps)}, storage={list(storage)}"
    )


def wait_for_apps(
    juju: jubilant.Juju,
    app_names: tuple[str, ...] | list[str],
    *,
    require_idle: bool = True,
) -> jubilant.Status:
    """Wait until all named applications are active and idle."""

    def is_ready(status: jubilant.Status) -> bool:
        for app_name in app_names:
            app = status.apps.get(app_name)
            if app is None:
                return False
            if not jubilant.all_active(status, app_name):
                return False
            if require_idle and not jubilant.all_agents_idle(status, app_name):
                return False
        return True

    return juju.wait(is_ready, timeout=20 * 60)


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


def wait_for_app_stable(
    juju: jubilant.Juju,
    app_name: str,
    units: int,
    *,
    timeout: int = 20 * 60,
    interval: int = 5,
    stable_window: int = 30,
) -> jubilant.Status:
    """Wait until the application stays active and idle for a sustained window."""
    successes = max(1, math.ceil(stable_window / interval))

    def is_ready(status: jubilant.Status) -> bool:
        app = status.apps.get(app_name)
        if app is None:
            return False
        return (
            len(app.units) == units
            and jubilant.all_active(status, app_name)
            and jubilant.all_agents_idle(status, app_name)
        )

    return juju.wait(
        is_ready,
        delay=interval,
        timeout=timeout,
        successes=successes,
    )


def deploy_local(
    juju: jubilant.Juju,
    app_name: str,
    charm_file: Path,
    rabbitmq_image: str,
    base: str,
    units: int = 1,
) -> jubilant.Status:
    """Deploy the local charm artifact and wait for it to settle."""
    if not app_exists(juju, app_name):
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
    if not app_exists(juju, app_name):
        juju.deploy(
            app_name,
            app=app_name,
            base=base,
            channel=stable_channel,
            num_units=units,
            trust=True,
        )
    return wait_for_app(juju, app_name, units)


def deploy_cos(juju: jubilant.Juju, cos_channel: str) -> jubilant.Status:
    """Deploy COS Lite into the current model and wait for key apps."""
    if not app_exists(juju, PROMETHEUS_APP):
        juju.deploy(COS_BUNDLE, channel=cos_channel, trust=True)
    return wait_for_apps(
        juju,
        (
            ALERTMANAGER_APP,
            GRAFANA_APP,
            LOKI_APP,
            PROMETHEUS_APP,
        ),
        require_idle=False,
    )


def relation_exists(juju: jubilant.Juju, provider: str, requirer: str) -> bool:
    """Return whether a Juju relation already exists between two endpoints."""
    output = juju.cli("status", "--relations")
    for line in output.splitlines():
        if (
            not line
            or line.startswith("Model ")
            or line.startswith("App ")
            or line.startswith("Unit ")
            or line.startswith("Relation ")
            or line.startswith("Relations:")
        ):
            continue
        if provider in line and requirer in line:
            return True
    return False


def integrate_observability(juju: jubilant.Juju, app_name: str) -> None:
    """Integrate RabbitMQ with the COS applications in the same model."""
    integrations = (
        (f"{app_name}:metrics-endpoint", f"{PROMETHEUS_APP}:metrics-endpoint"),
        (f"{app_name}:grafana-dashboard", f"{GRAFANA_APP}:grafana-dashboard"),
        (f"{app_name}:logging", f"{LOKI_APP}:logging"),
    )
    for provider, requirer in integrations:
        if relation_exists(juju, provider, requirer):
            continue
        juju.integrate(provider, requirer)


def wait_for_observability_integrations(
    juju: jubilant.Juju, app_name: str
) -> jubilant.Status:
    """Wait until RabbitMQ is ready and the relevant COS apps are active."""
    return wait_for_apps(
        juju,
        (
            ALERTMANAGER_APP,
            GRAFANA_APP,
            LOKI_APP,
            PROMETHEUS_APP,
            app_name,
        ),
        require_idle=False,
    )


def wait_for_metrics_endpoint_data(
    juju: jubilant.Juju,
    related_unit: str,
    *,
    timeout: int = 300,
    interval: int = 5,
) -> dict:
    """Wait until Prometheus receives metrics-endpoint scrape data."""
    deadline = time.monotonic() + timeout
    last_relation = {}
    while time.monotonic() < deadline:
        payload = json.loads(
            juju.cli(
                "show-unit",
                f"{PROMETHEUS_APP}/0",
                "--endpoint",
                "metrics-endpoint",
                "--related-unit",
                related_unit,
                "--format=json",
            )
        )
        relation_info = payload[f"{PROMETHEUS_APP}/0"]["relation-info"]
        if relation_info:
            for relation in relation_info:
                related_units = relation.get("related-units", {})
                if related_unit not in related_units:
                    continue
                last_relation = relation
                app_data = relation.get("application-data", {})
                if app_data.get("scrape_jobs"):
                    return relation
        time.sleep(interval)

    raise AssertionError(
        f"Timed out waiting for metrics-endpoint data for {related_unit}; "
        f"last relation was {last_relation}"
    )


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
    timeout: int = 10 * 60,
    interval: int = 5,
) -> dict:
    """Wait until RabbitMQ reports the expected running nodes."""
    deadline = time.monotonic() + timeout
    last_status = {}
    last_error = None
    while time.monotonic() < deadline:
        try:
            last_status = cluster_status(juju, unit_name)
            last_error = None
        except Exception as e:
            last_error = e
            time.sleep(interval)
            continue
        if set(last_status.get("running_nodes", [])) == expected_nodes:
            return last_status
        time.sleep(interval)
    if last_error:
        raise AssertionError(
            f"Timed out waiting for running nodes {expected_nodes}; "
            f"last error: {last_error}"
        )
    raise AssertionError(
        f"Timed out waiting for running nodes {expected_nodes}; "
        f"last status was {last_status}"
    )


def model_name(juju: jubilant.Juju) -> str:
    """Return the active Juju model name."""
    return juju.show_model().name.split("/")[-1]


def _unit_http_json(juju: jubilant.Juju, unit_name: str, url: str) -> dict:
    """Fetch JSON from a local HTTP endpoint inside a unit."""
    script = "python3 -c " + shlex.quote(
        "import urllib.request; "
        "print(urllib.request.urlopen(" + repr(url) + ").read().decode())"
    )
    output = juju.ssh(unit_name, script)
    return json.loads(output)


def prometheus_query(juju: jubilant.Juju, query: str) -> list[dict]:
    """Run an instant query against the Prometheus API."""
    payload = _unit_http_json(
        juju,
        f"{PROMETHEUS_APP}/0",
        f"http://localhost:9090/api/v1/query?query={quote(query, safe='')}",
    )
    assert payload["status"] == "success", payload
    return payload["data"]["result"]


def prometheus_rules(juju: jubilant.Juju) -> dict:
    """Return the loaded Prometheus rules."""
    payload = _unit_http_json(
        juju,
        f"{PROMETHEUS_APP}/0",
        "http://localhost:9090/api/v1/rules",
    )
    assert payload["status"] == "success", payload
    return payload["data"]


def prometheus_alerts(juju: jubilant.Juju) -> list[dict]:
    """Return active and pending alerts from Prometheus."""
    payload = _unit_http_json(
        juju,
        f"{PROMETHEUS_APP}/0",
        "http://localhost:9090/api/v1/alerts",
    )
    assert payload["status"] == "success", payload
    return payload["data"]["alerts"]


def wait_for_prometheus_query_result(
    juju: jubilant.Juju,
    query: str,
    *,
    timeout: int = 300,
    interval: int = 5,
) -> list[dict]:
    """Wait until a Prometheus query returns at least one series."""
    deadline = time.monotonic() + timeout
    last_result: list[dict] = []
    while time.monotonic() < deadline:
        last_result = prometheus_query(juju, query)
        if last_result:
            return last_result
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for Prometheus query result for {query!r}; "
        f"last result was {last_result}"
    )


def wait_for_prometheus_alert(
    juju: jubilant.Juju,
    alert_name: str,
    *,
    app_name: str,
    state: str | None = None,
    timeout: int = 300,
    interval: int = 5,
) -> dict:
    """Wait until Prometheus reports the named alert for RabbitMQ."""
    deadline = time.monotonic() + timeout
    last_alerts: list[dict] = []
    while time.monotonic() < deadline:
        last_alerts = prometheus_alerts(juju)
        for alert in last_alerts:
            labels = alert.get("labels", {})
            if (
                labels.get("alertname") == alert_name
                and labels.get("juju_application") == app_name
            ):
                if state is None or alert.get("state") == state:
                    return alert
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for Prometheus alert {alert_name!r}"
        f"{f' in state {state!r}' if state else ''}; "
        f"last alerts were {last_alerts}"
    )


def alertmanager_alerts(juju: jubilant.Juju) -> list[dict]:
    """Return active alerts from Alertmanager."""
    return _unit_http_json(
        juju,
        f"{ALERTMANAGER_APP}/0",
        "http://localhost:9093/api/v2/alerts",
    )


def wait_for_alertmanager_alert(
    juju: jubilant.Juju,
    alert_name: str,
    *,
    app_name: str,
    timeout: int = 300,
    interval: int = 5,
) -> dict:
    """Wait until Alertmanager reports the named alert for RabbitMQ."""
    deadline = time.monotonic() + timeout
    last_alerts: list[dict] = []
    while time.monotonic() < deadline:
        last_alerts = alertmanager_alerts(juju)
        for alert in last_alerts:
            labels = alert.get("labels", {})
            if (
                labels.get("alertname") == alert_name
                and labels.get("juju_application") == app_name
            ):
                return alert
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for Alertmanager alert {alert_name!r}; "
        f"last alerts were {last_alerts}"
    )


def wait_for_alertmanager_alert_to_clear(
    juju: jubilant.Juju,
    alert_name: str,
    *,
    app_name: str,
    timeout: int = 300,
    interval: int = 5,
) -> None:
    """Wait until Alertmanager no longer reports the named alert."""
    deadline = time.monotonic() + timeout
    last_alerts: list[dict] = []
    while time.monotonic() < deadline:
        last_alerts = alertmanager_alerts(juju)
        if not any(
            alert.get("labels", {}).get("alertname") == alert_name
            and alert.get("labels", {}).get("juju_application") == app_name
            for alert in last_alerts
        ):
            return
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for Alertmanager alert {alert_name!r} to clear; "
        f"last alerts were {last_alerts}"
    )


def loki_query_range(
    juju: jubilant.Juju,
    query: str,
    *,
    seconds: int = 300,
    limit: int = 100,
) -> list[dict]:
    """Query recent log streams from Loki."""
    now_ns = int(time.time() * 1_000_000_000)
    start_ns = now_ns - seconds * 1_000_000_000
    payload = _unit_http_json(
        juju,
        f"{LOKI_APP}/0",
        (
            "http://localhost:3100/loki/api/v1/query_range"
            f"?query={quote(query, safe='')}"
            f"&limit={limit}"
            f"&start={start_ns}"
            f"&end={now_ns}"
            "&direction=BACKWARD"
        ),
    )
    assert payload["status"] == "success", payload
    return payload["data"]["result"]


def wait_for_loki_logs(
    juju: jubilant.Juju,
    query: str,
    *,
    timeout: int = 300,
    interval: int = 5,
) -> list[dict]:
    """Wait until Loki returns at least one matching stream."""
    deadline = time.monotonic() + timeout
    last_result: list[dict] = []
    while time.monotonic() < deadline:
        last_result = loki_query_range(juju, query)
        if any(stream.get("values") for stream in last_result):
            return last_result
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for Loki logs for {query!r}; "
        f"last result was {last_result}"
    )


def wait_for_app_removed(
    juju: jubilant.Juju,
    app_name: str,
    timeout: int = 10 * 60,
    interval: int = 5,
) -> None:
    """Wait until the application no longer exists in the model."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not app_exists(juju, app_name):
            return
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for application {app_name!r} to be removed"
    )


def k8s_service_exists(juju: jubilant.Juju, service_name: str) -> bool:
    """Return whether a Kubernetes service exists in the model namespace."""
    output = subprocess.run(
        [
            kubectl(),
            "get",
            "service",
            service_name,
            "-n",
            model_name(juju),
            "--ignore-not-found",
            "-o",
            "name",
        ],
        check=True,
        capture_output=True,
        text=True,
    )  # nosec B603
    return bool(output.stdout.strip())


def k8s_pods_with_label(juju: jubilant.Juju, label_selector: str) -> list[str]:
    """Return pod names matching a label selector in the model namespace."""
    output = subprocess.run(
        [
            kubectl(),
            "get",
            "pods",
            "-l",
            label_selector,
            "-n",
            model_name(juju),
            "--ignore-not-found",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )  # nosec B603
    return output.stdout.strip().split() if output.stdout.strip() else []


def wait_for_k8s_service_gone(
    juju: jubilant.Juju,
    service_name: str,
    timeout: int = 5 * 60,
    interval: int = 5,
) -> None:
    """Wait until a Kubernetes service no longer exists."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not k8s_service_exists(juju, service_name):
            return
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for Kubernetes service {service_name!r} to be removed"
    )


def delete_all_app_pods(juju: jubilant.Juju, app_name: str) -> None:
    """Force-delete all pods for an application."""
    subprocess.run(
        [
            kubectl(),
            "delete",
            "pod",
            "-l",
            f"app.kubernetes.io/name={app_name}",
            "-n",
            model_name(juju),
            "--grace-period=0",
            "--force",
        ],
        check=True,
        capture_output=True,
        text=True,
    )  # nosec B603


def raw_status(juju: jubilant.Juju) -> dict:
    """Return the raw Juju status payload."""
    return json.loads(juju.cli("status", "--format=json"))


def leader_unit(juju: jubilant.Juju, app_name: str) -> str:
    """Return the current leader unit for an application."""
    status = raw_status(juju)
    units = status["applications"][app_name]["units"]
    for unit_name, unit_data in units.items():
        if unit_data.get("leader"):
            return unit_name

    # Fall back to the oneline marker if the JSON shape does not expose leadership.
    oneline = juju.cli("status", app_name, "--format=oneline")
    for line in oneline.splitlines():
        if line.startswith(f"{app_name}/") and "*" in line.split()[0]:
            return line.split()[0].replace("*", "")

    raise AssertionError(f"Unable to determine leader for {app_name}")


def wait_for_unit_workload_status(
    juju: jubilant.Juju,
    unit_name: str,
    *,
    current: str,
    message_substring: str | None = None,
    timeout: int = 300,
    interval: int = 5,
) -> dict:
    """Wait until a unit reports the expected workload status."""
    app_name = unit_name.split("/")[0]
    deadline = time.monotonic() + timeout
    last_status = {}
    while time.monotonic() < deadline:
        last_status = raw_status(juju)
        unit_status = last_status["applications"][app_name]["units"][
            unit_name
        ]["workload-status"]
        if unit_status["current"] == current:
            info = unit_status.get("message", "")
            if message_substring is None or message_substring in info:
                return unit_status
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for {unit_name} workload-status={current!r}; "
        f"last status was {last_status}"
    )


def rabbitmqctl_stop_app(juju: jubilant.Juju, unit_name: str) -> None:
    """Stop the RabbitMQ application inside the workload container."""
    juju.ssh(
        unit_name,
        "rabbitmqctl",
        "stop_app",
        container=RABBITMQ_CONTAINER,
    )


def rabbitmqctl_start_app(juju: jubilant.Juju, unit_name: str) -> None:
    """Start the RabbitMQ application inside the workload container."""
    juju.ssh(
        unit_name,
        "rabbitmqctl",
        "start_app",
        container=RABBITMQ_CONTAINER,
    )


def hold_rabbitmq_service_stopped(juju: jubilant.Juju, unit_name: str) -> None:
    """Keep the RabbitMQ unit unavailable by repeatedly deleting its pod."""
    key = (model_name(juju), unit_name)
    process = _RABBITMQ_STOP_PROCESSES.get(key)
    if process and process.poll() is None:
        return

    _RABBITMQ_STOP_PROCESSES[key] = subprocess.Popen(  # nosec B603
        [
            "bash",
            "-lc",
            (
                f"while true; do "
                f"{shlex.quote(kubectl())} delete pod "
                f"{shlex.quote(pod_name_for_unit(unit_name))} "
                f"-n {shlex.quote(model_name(juju))} "
                "--ignore-not-found=true --grace-period=0 --force "
                ">/dev/null 2>&1 || true; "
                "sleep 5; "
                "done"
            ),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def release_rabbitmq_service_stop(juju: jubilant.Juju, unit_name: str) -> None:
    """Stop holding the RabbitMQ unit down and let Kubernetes recreate it."""
    key = (model_name(juju), unit_name)
    process = _RABBITMQ_STOP_PROCESSES.pop(key, None)
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def rabbitmq_service_state(juju: jubilant.Juju, unit_name: str) -> str:
    """Return the Pebble-reported state of the RabbitMQ service."""
    output = juju.ssh(
        unit_name,
        "/charm/bin/pebble",
        "services",
        "rabbitmq",
        container=RABBITMQ_CONTAINER,
    )
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0] == "rabbitmq":
            return parts[2]
    raise AssertionError(f"Unable to parse Pebble services output: {output}")


def wait_for_rabbitmq_service_state(
    juju: jubilant.Juju,
    unit_name: str,
    *,
    state: str,
    timeout: int = 120,
    interval: int = 2,
) -> str:
    """Wait until Pebble reports the expected RabbitMQ service state."""
    deadline = time.monotonic() + timeout
    last_state = ""
    while time.monotonic() < deadline:
        last_state = rabbitmq_service_state(juju, unit_name)
        if last_state == state:
            return last_state
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for {unit_name} rabbitmq service state {state!r}; "
        f"last state was {last_state!r}"
    )


def trigger_reconcile_notice(juju: jubilant.Juju, unit_name: str) -> None:
    """Emit the charm's periodic Pebble notice immediately."""
    juju.ssh(
        unit_name,
        "/charm/bin/pebble",
        "notify",
        RABBITMQ_TIMER_NOTICE,
        container=RABBITMQ_CONTAINER,
    )


def kubectl() -> str:
    """Return the kubectl executable or skip if unavailable."""
    kubectl_path = shutil.which("kubectl")
    if kubectl_path is None:
        pytest.skip("kubectl is required for Kubernetes functional tests")
    return kubectl_path


def loadbalancer_address(
    juju: jubilant.Juju,
    app_name: str,
    timeout: int = 300,
    interval: int = 5,
) -> str:
    """Return the external address of the RabbitMQ load balancer service."""
    namespace = model_name(juju)
    service_name = f"{app_name}-lb"
    jsonpath = "{.status.loadBalancer.ingress[0].ip}{.status.loadBalancer.ingress[0].hostname}"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        output = subprocess.run(
            [
                kubectl(),
                "get",
                "service",
                service_name,
                "-n",
                namespace,
                "-o",
                f"jsonpath={jsonpath}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )  # nosec B603
        address = output.stdout.strip()
        if address:
            return address
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for LoadBalancer address for {service_name}"
    )


def pod_name_for_unit(unit_name: str) -> str:
    """Return the Kubernetes pod name for a Juju unit."""
    return unit_name.replace("/", "-")


def delete_pod(juju: jubilant.Juju, unit_name: str) -> None:
    """Delete the Kubernetes pod backing a RabbitMQ unit."""
    subprocess.run(
        [
            kubectl(),
            "delete",
            "pod",
            pod_name_for_unit(unit_name),
            "-n",
            model_name(juju),
            "--wait=true",
        ],
        check=True,
        capture_output=True,
        text=True,
    )  # nosec B603


def wait_for_pod_ready(
    juju: jubilant.Juju,
    unit_name: str,
    *,
    timeout: int = 300,
    interval: int = 5,
) -> None:
    """Wait until a Kubernetes pod becomes Ready again."""
    deadline = time.monotonic() + timeout
    pod_name = pod_name_for_unit(unit_name)
    namespace = model_name(juju)
    while time.monotonic() < deadline:
        output = subprocess.run(
            [
                kubectl(),
                "get",
                "pod",
                pod_name,
                "-n",
                namespace,
                "-o",
                "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )  # nosec B603
        if output.stdout.strip() == "True":
            return
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for pod {pod_name} to become Ready"
    )


def resilience_profile() -> dict[str, int]:
    """Build a conservative perf-test profile based on local CPU capacity."""
    cpus = max(2, os.cpu_count() or 2)
    return {
        "queues": min(48, max(8, cpus * 2)),
        "producers": min(16, max(4, cpus // 2)),
        "consumers": min(16, max(4, cpus // 2)),
        "rate": min(160, max(40, cpus * 4)),
        "confirm": 10,
        "qos": 10,
        "size": 1024,
        "duration": 120,
        "start_delay": min(30, max(10, cpus)),
        "heartbeat_threads": min(4, max(1, cpus // 8)),
        "consumer_pools": min(8, max(2, cpus // 3)),
        "nio_threads": min(8, max(2, cpus // 4)),
    }


def start_perf_test(
    *,
    uri: str,
    profile: dict[str, int],
    test_id: str,
    queue_prefix: str,
) -> subprocess.Popen[str]:
    """Start a bounded rabbitmq-perf-test process."""
    perf_test = shutil.which("rabbitmq-perf-test")
    if perf_test is None:
        pytest.skip(
            "rabbitmq-perf-test is required for resiliency functional tests"
        )

    args = [
        perf_test,
        "--uri",
        uri,
        "--id",
        test_id,
        "--queue-pattern",
        f"{queue_prefix}-%03d",
        "--queue-pattern-from",
        "1",
        "--queue-pattern-to",
        str(profile["queues"]),
        "--producers",
        str(profile["producers"]),
        "--consumers",
        str(profile["consumers"]),
        "--quorum-queue",
        "--leader-locator",
        "balanced",
        "--confirm",
        str(profile["confirm"]),
        "--qos",
        str(profile["qos"]),
        "--multi-ack-every",
        "5",
        "--rate",
        str(profile["rate"]),
        "--size",
        str(profile["size"]),
        "--producer-random-start-delay",
        str(profile["start_delay"]),
        "--heartbeat-sender-threads",
        str(profile["heartbeat_threads"]),
        "--consumers-thread-pools",
        str(profile["consumer_pools"]),
        "--nio-threads",
        str(profile["nio_threads"]),
        "--time",
        str(profile["duration"]),
    ]
    return subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )  # nosec B603


def amqp_uri(
    *,
    username: str,
    password: str,
    host: str,
    vhost: str,
    port: int = 5672,
) -> str:
    """Build an AMQP URI for rabbitmq-perf-test."""
    return (
        f"amqp://{username}:{password}@{host}:{port}/{quote(vhost, safe='')}"
    )


def rabbitmq_available_bytes(juju: jubilant.Juju, unit_name: str) -> int:
    """Return available bytes for the RabbitMQ data directory."""
    output = juju.ssh(
        unit_name,
        f"df -P -B1 {RABBITMQ_DATA_DIR} | awk 'NR==2 {{print $4}}'",
        container=RABBITMQ_CONTAINER,
    )
    return int(output.strip())


def fill_rabbitmq_disk_to_alarm(juju: jubilant.Juju, unit_name: str) -> int:
    """Create a filler file that should push RabbitMQ below its free-space limit."""
    available = rabbitmq_available_bytes(juju, unit_name)
    target_remaining = min(
        64 * 1024 * 1024, max(16 * 1024 * 1024, available // 8)
    )
    bytes_to_fill = available - target_remaining
    if bytes_to_fill <= 0:
        raise AssertionError(
            "RabbitMQ data directory does not have enough headroom to "
            "simulate disk pressure"
        )

    count_mib = max(1, math.ceil(bytes_to_fill / 1024 / 1024))
    juju.ssh(
        unit_name,
        (
            f"rm -f {RABBITMQ_FILLER_PATH} && "
            f"dd if=/dev/zero of={RABBITMQ_FILLER_PATH} "
            f"bs=1M count={count_mib} conv=fsync"
        ),
        container=RABBITMQ_CONTAINER,
    )
    return count_mib * 1024 * 1024


def clear_rabbitmq_filler_file(juju: jubilant.Juju, unit_name: str) -> None:
    """Remove the disk pressure filler file from the RabbitMQ data directory."""
    juju.ssh(
        unit_name,
        f"rm -f {RABBITMQ_FILLER_PATH} && sync",
        container=RABBITMQ_CONTAINER,
    )
