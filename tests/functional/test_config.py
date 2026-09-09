# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional tests covering config boundary validation."""

from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)

import jubilant
import pytest

from .helpers import (
    deploy_local,
    wait_for_app,
    wait_for_unit_workload_status,
)


@pytest.fixture(scope="module")
def deployed_rabbitmq(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> str:
    """Deploy RabbitMQ once for all config tests and return the app name."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)
    return app_name


def test_invalid_cluster_partition_handling_blocks(
    juju: jubilant.Juju,
    deployed_rabbitmq: str,
) -> None:
    """Invalid cluster-partition-handling value puts the unit into blocked."""
    app = deployed_rabbitmq
    try:
        juju.config(app, {"cluster-partition-handling": "invalid"})
        wait_for_unit_workload_status(
            juju,
            f"{app}/0",
            current="blocked",
            message_substring="cluster-partition-handling must be one of",
        )
    finally:
        juju.config(app, reset="cluster-partition-handling")
        wait_for_app(juju, app, units=1)


def test_invalid_disk_free_limit_bytes_non_numeric_blocks(
    juju: jubilant.Juju,
    deployed_rabbitmq: str,
) -> None:
    """Non-numeric disk-free-limit-bytes value puts the unit into blocked."""
    app = deployed_rabbitmq
    try:
        juju.config(app, {"disk-free-limit-bytes": "not_a_number"})
        wait_for_unit_workload_status(
            juju,
            f"{app}/0",
            current="blocked",
            message_substring="Invalid disk-free-limit-bytes value",
        )
    finally:
        juju.config(app, reset="disk-free-limit-bytes")
        wait_for_app(juju, app, units=1)


def test_invalid_disk_free_limit_bytes_zero_blocks(
    juju: jubilant.Juju,
    deployed_rabbitmq: str,
) -> None:
    """Zero disk-free-limit-bytes value puts the unit into blocked."""
    app = deployed_rabbitmq
    try:
        juju.config(app, {"disk-free-limit-bytes": "0"})
        wait_for_unit_workload_status(
            juju,
            f"{app}/0",
            current="blocked",
            message_substring="must resolve to a value greater than 0",
        )
    finally:
        juju.config(app, reset="disk-free-limit-bytes")
        wait_for_app(juju, app, units=1)


def test_invalid_loadbalancer_annotations_blocks(
    juju: jubilant.Juju,
    deployed_rabbitmq: str,
) -> None:
    """Invalid loadbalancer_annotations value puts the unit into blocked."""
    app = deployed_rabbitmq
    try:
        juju.config(app, {"loadbalancer_annotations": "bad-format"})
        wait_for_unit_workload_status(
            juju,
            f"{app}/0",
            current="blocked",
            message_substring="Invalid config value 'loadbalancer_annotations'",
        )
    finally:
        juju.config(app, reset="loadbalancer_annotations")
        wait_for_app(juju, app, units=1)
