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

import jubilant
import pytest
import yaml

GENERATED_METADATA_FILES = (
    "actions.yaml",
    "config.yaml",
    "metadata.yaml",
)


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register custom command-line options for functional tests."""
    parser.addoption(
        "--cloud",
        action="store",
        default=None,
        help="Cloud to use when creating a temporary model.",
    )
    parser.addoption(
        "--controller",
        action="store",
        default=None,
        help="Controller to use when creating a temporary model.",
    )
    parser.addoption(
        "--model",
        action="store",
        default=None,
        help="Existing Juju model to run the tests against.",
    )
    parser.addoption(
        "--charm-file",
        action="store",
        default=None,
        help="Path to a pre-built charm artifact to deploy or refresh to.",
    )
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="Keep temporary models after the test run completes.",
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
    with pathlib.Path("charmcraft.yaml").open(encoding="utf-8") as stream:
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
        pathlib.Path(generated_file).unlink(missing_ok=True)

    try:
        subprocess.run(
            ["charmcraft", "pack"],
            check=True,
            capture_output=True,
            text=True,
        )  # nosec B603, B607
    except subprocess.CalledProcessError as exc:
        raise OSError(
            f"Error packing charm: {exc}\nStderr:\n{exc.stderr}"
        ) from exc

    charms = sorted(pathlib.Path(".").glob(f"{app_name}*.charm"))
    if not charms:
        raise FileNotFoundError(f"Unable to find packed charm for {app_name}")
    if len(charms) > 1:
        raise RuntimeError(
            f"Found more than one packed charm for {app_name}: {charms}"
        )
    return charms[0].resolve()


@pytest.fixture(scope="module")
def juju(
    request: pytest.FixtureRequest,
) -> jubilant.Juju:
    """Provide a temporary Juju model per functional test module."""

    def show_debug_log(client: jubilant.Juju) -> None:
        if request.session.testsfailed:
            try:
                print(client.debug_log(limit=1000), end="")
            except Exception:
                # Temporary models may already be gone by the time teardown runs.
                return

    model = request.config.getoption("--model")
    if model:
        client = jubilant.Juju(model=model)
        client.wait_timeout = 20 * 60
        yield client
        show_debug_log(client)
        return

    keep_models = bool(request.config.getoption("--keep-models"))
    cloud = request.config.getoption("--cloud")
    controller = request.config.getoption("--controller")
    with jubilant.temp_model(
        keep=keep_models,
        cloud=cloud,
        controller=controller,
    ) as client:
        client.wait_timeout = 20 * 60
        yield client
        show_debug_log(client)
