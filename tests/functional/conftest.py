# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for Jubilant-based functional tests."""

from __future__ import (
    annotations,
)

import json
import pathlib
import shutil
import subprocess  # nosec B404
from typing import (
    Any,
)

import jubilant
import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).parents[2]
LOG_DIR = PROJECT_ROOT / "logs"

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


def _collect_pod_logs(
    kubectl_path: str, namespace: str, dest: pathlib.Path
) -> None:
    """Dump container logs for every pod in the namespace."""
    try:
        result = subprocess.run(
            [kubectl_path, "-n", namespace, "get", "pods", "-o", "json"],
            capture_output=True,
            text=True,
            timeout=30,
        )  # nosec B603
        pods = json.loads(result.stdout).get("items", [])
    except Exception:
        return

    for pod in pods:
        pod_name = pod["metadata"]["name"]
        for container in pod.get("spec", {}).get("containers", []):
            container_name = container["name"]
            try:
                result = subprocess.run(
                    [
                        kubectl_path,
                        "-n",
                        namespace,
                        "logs",
                        pod_name,
                        "-c",
                        container_name,
                        "--tail=5000",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )  # nosec B603
                (dest / f"pod-{pod_name}-{container_name}.log").write_text(
                    result.stdout
                )
            except Exception:
                pass


def _collect_logs(client: jubilant.Juju, dest: pathlib.Path) -> None:
    """Dump juju and pod logs for post-mortem analysis."""
    dest.mkdir(parents=True, exist_ok=True)

    try:
        (dest / "juju-debug-log.txt").write_text(client.debug_log(limit=5000))
    except Exception:
        pass

    try:
        (dest / "juju-status.json").write_text(
            client.cli("status", "--format=json")
        )
    except Exception:
        pass

    kubectl_path = shutil.which("kubectl")
    if kubectl_path:
        _collect_pod_logs(kubectl_path, client.model, dest)


@pytest.fixture(scope="module")
def juju(
    request: pytest.FixtureRequest,
) -> jubilant.Juju:
    """Provide a temporary Juju model per functional test module."""

    def collect_on_failure(client: jubilant.Juju) -> None:
        if not request.session.testsfailed:
            return
        module_name = request.module.__name__.rsplit(".", 1)[-1]
        _collect_logs(client, LOG_DIR / module_name)

    model = request.config.getoption("--model")
    if model:
        client = jubilant.Juju(model=model)
        client.wait_timeout = 20 * 60
        yield client
        collect_on_failure(client)
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
        collect_on_failure(client)
