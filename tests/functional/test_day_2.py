# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering RabbitMQ day-2 recovery operations."""

from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)

import jubilant

from .helpers import (
    clear_rabbitmq_filler_file,
    deploy_local,
    fill_rabbitmq_disk_to_alarm,
    leader_unit,
    run_action,
    trigger_reconcile_notice,
    wait_for_app,
    wait_for_unit_workload_status,
)

OPERATOR_USER_RECOVERY_MESSAGE = (
    "Operator user missing or invalid in RabbitMQ; "
    "run recreate-operator-user action"
)


def test_recover_from_disk_pressure(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Fill the RabbitMQ data directory and recover by cleaning space."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    filled_bytes = fill_rabbitmq_disk_to_alarm(juju, f"{app_name}/0")
    assert filled_bytes > 0

    wait_for_unit_workload_status(
        juju,
        f"{app_name}/0",
        current="blocked",
        message_substring="Protection mode: Local alarms active",
        timeout=5 * 60,
    )

    clear_rabbitmq_filler_file(juju, f"{app_name}/0")
    wait_for_app(juju, app_name, units=1)

    operator_info = run_action(
        juju, leader_unit(juju, app_name), "get-operator-info"
    )
    assert operator_info.results["operator-user"] == "operator"
    assert operator_info.results["operator-password"]


def test_recreate_operator_user_action_recovers_admin_access(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Delete the operator user and recover with the day-2 action."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    juju.ssh(
        f"{app_name}/0",
        "rabbitmqctl",
        "delete_user",
        "operator",
        container="rabbitmq",
    )
    trigger_reconcile_notice(juju, f"{app_name}/0")
    wait_for_unit_workload_status(
        juju,
        f"{app_name}/0",
        current="blocked",
        message_substring=OPERATOR_USER_RECOVERY_MESSAGE,
        timeout=5 * 60,
    )

    task = run_action(
        juju, leader_unit(juju, app_name), "recreate-operator-user"
    )
    assert task.results["operator-user"] == "operator"

    wait_for_app(juju, app_name, units=1)
    operator_info = run_action(
        juju, leader_unit(juju, app_name), "get-operator-info"
    )
    assert operator_info.results["operator-user"] == "operator"
    assert operator_info.results["operator-password"]
