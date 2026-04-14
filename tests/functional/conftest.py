# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for Jubilant-based functional tests."""

from __future__ import (
    annotations,
)

import pathlib
import subprocess  # nosec B404
from typing import (
    Any,
)

import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).parents[2]

GENERATED_METADATA_FILES = (
    "actions.yaml",
    "config.yaml",
    "metadata.yaml",
)


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register charm-specific command-line options."""
    parser.addoption(
        "--charm-file",
        action="store",
        default=None,
        help="Path to a pre-built charm artifact to deploy or refresh to.",
    )
    parser.addoption(
        "--rabbitmq-image",
        action="store",
        default=None,
        help="Override the rabbitmq-image OCI resource used for local deploys.",
    )
    parser.addoption(
        "--stable-channel",
        action="store",
        default="3.12/stable",
        help="Charmhub channel used as the refresh source baseline.",
    )
    parser.addoption(
        "--cos-channel",
        action="store",
        default="latest/stable",
        help="Charmhub channel used for the COS Lite bundle.",
    )


@pytest.fixture(scope="session")
def charmcraft_config() -> dict[str, Any]:
    """Return the parsed charmcraft configuration."""
    with (PROJECT_ROOT / "charmcraft.yaml").open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


@pytest.fixture(scope="session")
def app_name(charmcraft_config: dict[str, Any]) -> str:
    """Return the application name."""
    return charmcraft_config["name"]


@pytest.fixture(scope="session")
def base(charmcraft_config: dict[str, Any]) -> str:
    """Return the charm base."""
    return charmcraft_config["base"]


@pytest.fixture(scope="session")
def rabbitmq_image(
    charmcraft_config: dict[str, Any], pytestconfig: pytest.Config
) -> str:
    """Return the OCI image reference to use for local deploys."""
    override = pytestconfig.getoption("--rabbitmq-image")
    if override:
        return override
    return charmcraft_config["resources"]["rabbitmq-image"]["upstream-source"]


@pytest.fixture(scope="session")
def stable_channel(pytestconfig: pytest.Config) -> str:
    """Return the Charmhub channel used for refresh baseline deploys."""
    return pytestconfig.getoption("--stable-channel")


@pytest.fixture(scope="session")
def cos_channel(pytestconfig: pytest.Config) -> str:
    """Return the Charmhub channel used for COS Lite deployments."""
    return pytestconfig.getoption("--cos-channel")


@pytest.fixture(scope="session")
def charm_file(app_name: str, pytestconfig: pytest.Config) -> pathlib.Path:
    """Return a packed charm artifact, packing one if needed."""
    supplied = pytestconfig.getoption("--charm-file")
    if supplied:
        return pathlib.Path(supplied).resolve()

    for generated_file in GENERATED_METADATA_FILES:
        (PROJECT_ROOT / generated_file).unlink(missing_ok=True)

    try:
        subprocess.run(
            ["charmcraft", "pack"],
            check=True,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )  # nosec B603, B607
    except subprocess.CalledProcessError as exc:
        raise OSError(
            f"Error packing charm: {exc}\nStderr:\n{exc.stderr}"
        ) from exc

    charms = sorted(PROJECT_ROOT.glob(f"{app_name}*.charm"))
    if not charms:
        raise FileNotFoundError(f"Unable to find packed charm for {app_name}")
    if len(charms) > 1:
        raise RuntimeError(
            f"Found more than one packed charm for {app_name}: {charms}"
        )
    return charms[0].resolve()
