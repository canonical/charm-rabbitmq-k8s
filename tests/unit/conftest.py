# Copyright 2026 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared fixtures for RabbitMQ unit tests."""

from pathlib import (
    Path,
)
from tempfile import (
    TemporaryDirectory,
)

import pytest
import yaml
from ops import (
    testing,
)

import charm

REPO_ROOT = Path(__file__).parents[2]
CHARMCRAFT = yaml.safe_load((REPO_ROOT / "charmcraft.yaml").read_text())
META_FIELDS = {
    "assumes",
    "containers",
    "description",
    "extra-bindings",
    "links",
    "min-juju-version",
    "name",
    "peers",
    "payloads",
    "provides",
    "requires",
    "resources",
    "storage",
    "subordinate",
    "summary",
    "tags",
    "terms",
}
METADATA = {
    key: value for key, value in CHARMCRAFT.items() if key in META_FIELDS
}
ACTIONS = CHARMCRAFT.get("actions", {})
CONFIG = CHARMCRAFT.get("config", {})


def _build_test_charm_root(root: Path) -> None:
    """Create an isolated charm root for ops-scenario tests."""
    (root / "src").mkdir()
    (root / "lib").mkdir()
    (root / "src" / "grafana_dashboards").symlink_to(
        REPO_ROOT / "src" / "grafana_dashboards",
        target_is_directory=True,
    )
    (root / "src" / "prometheus_alert_rules").symlink_to(
        REPO_ROOT / "src" / "prometheus_alert_rules",
        target_is_directory=True,
    )
    (root / "metadata.yaml").write_text(yaml.safe_dump(METADATA))
    (root / "actions.yaml").write_text(yaml.safe_dump(ACTIONS))
    (root / "config.yaml").write_text(yaml.safe_dump({"options": CONFIG}))


class TestableRabbitMQOperatorCharm(charm.RabbitMQOperatorCharm):
    """Charm variant used by unit tests.

    Scenario tests should not hit the live Kubernetes API while verifying
    event handling, so the load-balancer reconciliation hook is disabled.
    """

    def _reconcile_lb(self, _):
        return None


def build_peer_relation(
    operator_password: str = "foobar",
    operator_user_created: str | None = "rmqadmin",
    erlang_cookie: str | None = "magicsecurity",
):
    """Create the peer relation used by scenario tests."""
    app_data = {}
    if operator_password is not None:
        app_data["operator_password"] = operator_password
    if operator_user_created is not None:
        app_data["operator_user_created"] = operator_user_created
    if erlang_cookie is not None:
        app_data["erlang_cookie"] = erlang_cookie
    return testing.PeerRelation(
        endpoint="peers",
        local_app_data=app_data,
        local_unit_data={},
    )


def build_amqp_relation(
    *,
    local_app_data: dict | None = None,
    remote_app_data: dict | None = None,
):
    """Create an AMQP relation for scenario tests."""
    return testing.Relation(
        endpoint="amqp",
        remote_app_name="amqp-client-app",
        local_app_data=local_app_data or {},
        remote_app_data=remote_app_data
        or {
            "username": "client",
            "vhost": "client-vhost",
        },
        remote_units_data={0: {"ingress-address": "10.20.0.5"}},
    )


@pytest.fixture()
def ctx():
    """Create the scenario test context."""
    with TemporaryDirectory(prefix="rabbitmq-scenario-") as tmpdir:
        charm_root = Path(tmpdir)
        _build_test_charm_root(charm_root)
        yield testing.Context(
            TestableRabbitMQOperatorCharm,
            meta=METADATA,
            actions=ACTIONS,
            config=CONFIG,
            charm_root=charm_root,
            capture_deferred_events=True,
        )


@pytest.fixture()
def rabbitmq_container():
    """A connectable rabbitmq container."""
    return testing.Container(name=charm.RABBITMQ_CONTAINER, can_connect=True)


@pytest.fixture()
def networks():
    """Network bindings used by scenario tests."""
    return [
        testing.Network(
            "peers",
            bind_addresses=[
                testing.BindAddress(addresses=[testing.Address("10.10.1.1")])
            ],
        ),
        testing.Network(
            "amqp",
            bind_addresses=[
                testing.BindAddress(addresses=[testing.Address("10.5.0.1")])
            ],
            ingress_addresses=["10.5.0.1"],
        ),
    ]


@pytest.fixture()
def timer_notice():
    """The custom notice emitted by the notifier service."""
    return testing.Notice(key=charm.TIMER_NOTICE)
