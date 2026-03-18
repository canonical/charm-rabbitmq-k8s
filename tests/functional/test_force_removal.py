# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Functional test for force-removal and reinstall.

This test gets its own module (and thus its own Juju model) because
force-removing a CAAS application can leave orphaned storage references
that prevent redeployment in the same model.
"""

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
    run_action,
    wait_for_model_empty,
)


@pytest.mark.xfail(
    reason=(
        "Juju CAAS bug: force-removing an application leaves orphaned storage "
        "references in the StatefulSet, causing redeployment to fail with "
        "'volume not found'. Related: LP#2031931, LP#1977865."
    ),
    strict=False,
)
def test_reinstall_after_force_removal(
    juju: jubilant.Juju,
    app_name: str,
    base: str,
    charm_file: Path,
    rabbitmq_image: str,
) -> None:
    """Force-remove and reinstall, verifying credentials are available."""
    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    juju.cli(
        "remove-application",
        "--no-prompt",
        "--force",
        "--no-wait",
        "--destroy-storage",
        app_name,
    )
    wait_for_model_empty(juju)

    deploy_local(juju, app_name, charm_file, rabbitmq_image, base)

    operator_info = run_action(juju, f"{app_name}/0", "get-operator-info")
    assert operator_info.results["operator-user"] == "operator"
    assert operator_info.results["operator-password"]
