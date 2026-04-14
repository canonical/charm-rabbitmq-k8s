#!/usr/bin/env python3
#
# Copyright 2021 David Ames
# Copyright 2021 Canonical Ltd.
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

"""RabbitMQ Operator Charm."""

import collections
import functools
import http.client
import json
import logging
import re
import secrets
from ipaddress import (
    IPv4Address,
    IPv6Address,
)
from pathlib import (
    Path,
)
from typing import cast

import ops
import requests
from charms.grafana_k8s.v0.grafana_dashboard import (
    GrafanaDashboardProvider,
)
from charms.loki_k8s.v1.loki_push_api import (
    LogForwarder,
)
from charms.prometheus_k8s.v0.prometheus_scrape import (
    MetricsEndpointProvider,
)
from charms.rabbitmq_k8s.v0.rabbitmq import (
    RabbitMQProvides,
)
from charms.traefik_k8s.v1.ingress import (
    IngressPerAppRequirer,
)
from jinja2 import (
    Environment,
    FileSystemLoader,
)
from lightkube.core.client import (
    Client,
)
from lightkube.core.exceptions import (
    ApiError,
)
from lightkube.models.core_v1 import (
    ServicePort,
    ServiceSpec,
)
from lightkube.models.meta_v1 import (
    ObjectMeta,
)
from lightkube.resources.core_v1 import (
    Service,
)
from lightkube_extensions.batch import (
    KubernetesResourceManager,
    create_charm_default_labels,
)
from ops.charm import (
    ActionEvent,
    CharmBase,
    CollectStatusEvent,
    PebbleCheckFailedEvent,
    PebbleCheckRecoveredEvent,
    PebbleCustomNoticeEvent,
)
from ops.framework import (
    EventBase,
    StoredState,
)
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    ModelError,
    WaitingStatus,
)
from ops.pebble import (
    APIError,
    ChangeError,
    ExecError,
    PathError,
)

import interface_rabbitmq_peers
import rabbit_extended_api

logger = logging.getLogger(__name__)

RABBITMQ_CONTAINER = "rabbitmq"
RABBITMQ_SERVICE = "rabbitmq"
RABBITMQ_USER = "rabbitmq"
RABBITMQ_GROUP = "rabbitmq"
RABBITMQ_STORAGE_NAME = "rabbitmq-data"
RABBITMQ_DATA_DIR = "/var/lib/rabbitmq"
RABBITMQ_COOKIE_PATH = "/var/lib/rabbitmq/.erlang.cookie"
RABBITMQ_MNESIA_DIR = "/var/lib/rabbitmq/mnesia"

SELECTOR_ALL = "all"
SELECTOR_EVEN = "even"
SELECTOR_INDIVIDUAL = "individual"

NOTIFIER_SERVICE = "notifier"
TIMER_NOTICE = "rabbitmq.local/timer"

LB_LABEL = "rabbitmq-loadbalancer"
RABBITMQ_SERVICE_PORT = 5672
RABBITMQ_MANAGEMENT_PORT = 15672
RABBITMQ_PROMETHEUS_PORT = 15692
RABBITMQ_PROTECTOR_MARKER = (
    f"{RABBITMQ_DATA_DIR}/.rabbitmq-protect-members-suspended"
)
RABBITMQ_SAFETY_REASON_FILE = f"{RABBITMQ_DATA_DIR}/.safety-check-reason"
RABBITMQ_ALIVE_CHECK_PATH = "/usr/bin/rabbitmq-alive-check"
RABBITMQ_SAFETY_CHECK_PATH = "/usr/bin/rabbitmq-safety-check"
HEALTH_CHECK_INTERVAL = "10s"
ALIVE_CHECK_TIMEOUT = "5s"
READY_CHECK_TIMEOUT = "10s"
HEALTH_CHECK_THRESHOLD = 3
RABBITMQ_STARTUP_GRACE_SECONDS = 180
SAFETY_REASON_NOT_RUNNING = "RabbitMQ not running"
SAFETY_REASON_LOCAL_ALARMS = "Local alarms active"
SAFETY_REASON_CLUSTER_STATUS = "Cluster status unavailable"
HEALTH_CHECK_WAITING_MESSAGE = "Waiting for local RabbitMQ cluster join"
OPERATOR_USER_RECOVERY_MESSAGE = (
    "Operator user missing or invalid in RabbitMQ; "
    "run recreate-operator-user action"
)

AMQP_RELATION = "amqp"
METRICS_ENDPOINT_RELATION = "metrics-endpoint"
VALID_CLUSTER_PARTITION_HANDLING = {
    "pause_minority",
    "autoheal",
    "ignore",
}
BYTE_SUFFIXES = {
    "K": 1000,
    "KB": 1000,
    "KI": 1024,
    "M": 1000**2,
    "MB": 1000**2,
    "MI": 1024**2,
    "G": 1000**3,
    "GB": 1000**3,
    "GI": 1024**3,
    "T": 1000**4,
    "TB": 1000**4,
    "TI": 1024**4,
    "P": 1000**5,
    "PB": 1000**5,
    "PI": 1024**5,
}

# Regex for Kubernetes annotation values:
# Kubernetes annotation values have no character restrictions — only a total
# size limit (256 KiB combined across all annotations).  We validate only
# that the value is a non-empty string.

# Based on
# https://github.com/kubernetes/apimachinery/blob/v0.31.3/pkg/util/validation/validation.go#L204
# Regex for DNS1123 subdomains:
# - Starts with a lowercase letter or number ([a-z0-9])
# - May contain dashes (-), but not consecutively, and must not start or end with them
# - Segments can be separated by dots (.)
# - Example valid: "example.com", "my-app.io", "sub.domain"
# - Example invalid: "-example.com", "example..com", "example-.com"
DNS1123_SUBDOMAIN_PATTERN = re.compile(
    r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
)

# Based on
# https://github.com/kubernetes/apimachinery/blob/v0.31.3/pkg/util/validation/validation.go#L32
# Regex for Kubernetes qualified names:
# - Starts with an alphanumeric character ([A-Za-z0-9])
# - Can include dashes (-), underscores (_), dots (.), or alphanumeric characters in the middle
# - Ends with an alphanumeric character
# - Must not be empty
# - Example valid: "annotation", "my.annotation", "annotation-name"
# - Example invalid: ".annotation", "annotation.", "-annotation", "annotation@key"
QUALIFIED_NAME_PATTERN = re.compile(
    r"^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$"
)

WAL_MAX_SIZE_CAP = 512 * 1024 * 1024  # 512 MiB hard cap for Raft WAL size

TEMPLATES_DIR = Path(__file__).parent / "templates"
TEMPLATE_ENV = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=False,
    keep_trailing_newline=True,
)


class RabbitOperatorError(Exception):
    """Common base class for all RabbitMQ Operator exceptions."""


class RabbitMQOperatorCharm(CharmBase):
    """RabbitMQ Operator Charm."""

    _stored = StoredState()
    _operator_user = "operator"

    def __init__(self, *args):
        super().__init__(*args)

        self._lightkube_client = None
        self._lightkube_field_manager: str = self.app.name
        self._lb_name: str = f"{self.app.name}-lb"
        self._departing_unit_count: int = 0

        # Open ports on default cluster IP
        # OpenStack services uses cluster IP from service rabbitmq
        self.unit.set_ports(
            RABBITMQ_SERVICE_PORT,
            RABBITMQ_MANAGEMENT_PORT,
            RABBITMQ_PROMETHEUS_PORT,
        )

        # As the service ports are not dynamic, loadbalancer need not
        # be created/patched on upgrade-hook or update-status hook
        # In case the service ports are modified, _reconcile_lb should
        # run on upgrade-hook
        # NOTE(jamespage): This should become part of what Juju
        # does at some point in time.
        self.framework.observe(self.on.install, self._reconcile_lb)
        self.framework.observe(self.on.leader_elected, self._reconcile)
        self.framework.observe(self.on.rabbitmq_pebble_ready, self._reconcile)
        self.framework.observe(self.on.upgrade_charm, self._reconcile)
        self.framework.observe(self.on.config_changed, self._reconcile)
        self.framework.observe(
            self.on.rabbitmq_data_storage_attached, self._reconcile
        )
        self.framework.observe(
            self.on.get_operator_info_action, self._on_get_operator_info_action
        )
        self.framework.observe(
            self.on.recreate_operator_user_action,
            self._on_recreate_operator_user_action,
        )
        self.framework.observe(
            self.on.add_member_action, self._on_add_member_action
        )
        self.framework.observe(
            self.on.delete_member_action, self._on_delete_member_action
        )
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(
            self.on.collect_unit_status, self._on_collect_unit_status
        )
        # Peers
        self.peers = interface_rabbitmq_peers.RabbitMQOperatorPeers(
            self, "peers"
        )
        self.framework.observe(
            self.on["peers"].relation_created,
            self._reconcile,
        )
        self.framework.observe(
            self.on["peers"].relation_changed,
            self._reconcile,
        )
        self.framework.observe(
            self.peers.on.leaving,
            self._on_peer_relation_leaving,
        )
        # AMQP Provides
        self.amqp_provider = RabbitMQProvides(
            self, AMQP_RELATION, self._noop_amqp_credentials_callback
        )
        self.framework.observe(
            self.on[AMQP_RELATION].relation_joined,
            self._reconcile,
        )
        self.framework.observe(
            self.on[AMQP_RELATION].relation_changed,
            self._reconcile,
        )
        self.framework.observe(
            self.on[AMQP_RELATION].relation_broken,
            self._reconcile,
        )
        self.framework.observe(self.on.remove, self._on_remove)

        self._stored.set_default(enabled_plugins=[])
        self._stored.set_default(rabbitmq_version=None)

        self._enable_plugin("rabbitmq_management")
        self._enable_plugin("rabbitmq_peer_discovery_k8s")
        self._enable_plugin("rabbitmq_prometheus")

        self.metrics_endpoint = MetricsEndpointProvider(
            self,
            jobs=[
                {
                    "static_configs": [
                        {"targets": [f"*:{RABBITMQ_PROMETHEUS_PORT}"]}
                    ]
                }
            ],
            refresh_event=[
                self.on[RABBITMQ_CONTAINER].pebble_ready,
                self.on[METRICS_ENDPOINT_RELATION].relation_created,
                self.on[METRICS_ENDPOINT_RELATION].relation_joined,
                self.on[METRICS_ENDPOINT_RELATION].relation_changed,
            ],
        )
        self.grafana_dashboard_provider = GrafanaDashboardProvider(self)

        # NOTE: ingress for management WebUI/API only
        self.management_ingress = IngressPerAppRequirer(
            self,
            "ingress",
            port=RABBITMQ_MANAGEMENT_PORT,
        )
        # Logging relation
        self.logging = LogForwarder(self, relation_name="logging")
        # Tracing relation
        self.tracing = ops.tracing.Tracing(
            self,
            tracing_relation_name="charm-tracing",
            ca_relation_name="receive-ca-cert",
        )

        self.framework.observe(
            self.on.get_service_account_action, self._get_service_account
        )

        self.framework.observe(
            self.on.ensure_queue_ha_action, self._ensure_queue_ha_action
        )

        self.framework.observe(
            self.on.rebalance_quorum_action,
            self._on_rebalance_quorum_action,
        )

        self.framework.observe(self.on.grow_action, self._on_grow_action)

        self.framework.observe(self.on.shrink_action, self._on_shrink_action)
        self.framework.observe(
            self.on.forget_cluster_node_action,
            self._on_forget_cluster_node_action,
        )

        self.framework.observe(
            self.on[RABBITMQ_CONTAINER].pebble_custom_notice,
            self._on_pebble_custom_notice,
        )
        self.framework.observe(
            self.on[RABBITMQ_CONTAINER].pebble_check_failed,
            self._on_pebble_check_failed,
        )
        self.framework.observe(
            self.on[RABBITMQ_CONTAINER].pebble_check_recovered,
            self._on_pebble_check_recovered,
        )

    def _pebble_ready(self) -> bool:
        """Check whether RabbitMQ container is up and configurable."""
        return self.unit.get_container(RABBITMQ_CONTAINER).can_connect()

    def _rabbitmq_running(self) -> bool:
        """Check whether the broker is actually running."""
        if not self._pebble_ready():
            return False

        container = self.unit.get_container(RABBITMQ_CONTAINER)
        try:
            process = container.exec(
                ["rabbitmq-diagnostics", "check_running"], timeout=30
            )
            process.wait_output()
        except (ExecError, ModelError):
            return False
        return True

    def _noop_amqp_credentials_callback(self, *args, **kwargs) -> None:
        """Disable callback-style AMQP provisioning in favor of reconciliation."""

    def _reconcile(self, event: EventBase | None) -> None:
        """Reconcile charm state from the current model and workload."""
        if (
            isinstance(event, PebbleCustomNoticeEvent)
            and event.notice.key != TIMER_NOTICE
        ):
            return

        self._reconcile_lb(None)
        self._reconcile_health_checks()

        if not self._ensure_broker_running(event):
            return

        if not self._reconcile_operator_user(event):
            return

        self._reconcile_running_broker_state()
        self._reconcile_health_checks()

        if not self._reconcile_amqp_relations(event):
            return

        self._cleanup_stale_amqp_users()

        if not self._reconcile_queue_membership(event):
            return

        self._publish_relation_data()

    def _workload_reconcile_prerequisites(  # noqa: C901
        self, event: EventBase | None = None
    ) -> bool:
        """Check whether enough state is present to reconcile the workload."""
        if not self._pebble_ready():
            if event is not None:
                event.defer()
            return False

        if not self.unit.is_leader() and not self.peers.erlang_cookie:
            if event is not None:
                event.defer()
            return False

        if not self.unit.is_leader() and not self.peers.operator_user_created:
            if event is not None:
                event.defer()
            return False

        if self.peers_bind_address is None:
            logger.debug("Waiting for binding address on peers interface")
            if event is not None:
                event.defer()
            return False

        if not self._set_ownership_on_data_dir():
            if event is not None:
                event.defer()
            return False

        return True

    def _reconcile_workload(self, event: EventBase | None = None) -> bool:
        """Render config, refresh Pebble layer, and ensure services are running."""
        if not self._workload_reconcile_prerequisites(event):
            return False

        try:
            changed_services = self._render_and_push_config_files()
        except RabbitOperatorError as e:
            logger.error("Unable to render workload configuration: %s", e)
            return False

        changed_services.update(self._render_and_push_workload_scripts())
        self._ensure_erlang_cookie()

        container = self.unit.get_container(RABBITMQ_CONTAINER)
        layer_changed = self._reconcile_workload_layer(container)
        self._ensure_workload_services(
            container, changed_services, layer_changed=layer_changed
        )
        return True

    def _reconcile_running_broker_state(self) -> None:
        """Refresh broker-managed state that only exists once RabbitMQ is up."""
        if not self._rabbitmq_running():
            return

        if not self.peers.operator_user_created:
            return

        if self._operator_user_recovery_required():
            return

        self._refresh_rabbitmq_version()
        self._ensure_cluster_name()
        self._render_and_push_safety_check()

    def _on_forget_cluster_node_action(self, event: ActionEvent) -> None:
        """Remove stale cluster members no longer present in Juju peer data.

        This is an operator-initiated action because rabbitmqctl
        forget_cluster_node permanently removes a node and deletes its
        quorum queue replicas.  It should only be run when the operator
        is certain the removed nodes will not rejoin with existing data.
        """
        if not self._require_queue_management_ready(event):
            return

        if self.peers.peers_rel is None:
            event.fail("Peer relation not available")
            return

        container = self.unit.get_container(RABBITMQ_CONTAINER)
        try:
            process = container.exec(
                ["rabbitmqctl", "cluster_status", "--formatter=json"],
                timeout=30,
            )
            output, _ = process.wait_output()
            cluster_status = json.loads(output)
        except (ExecError, ModelError, json.JSONDecodeError) as e:
            event.fail(f"Failed to retrieve cluster status: {e}")
            return

        expected_nodes = {
            self.generate_nodename(unit_name)
            for unit_name in {
                self.unit.name,
                *(unit.name for unit in self.peers.peers_rel.units),
            }
        }
        running_nodes = set(cluster_status.get("running_nodes", []))
        all_unexpected = [
            node
            for node in cluster_status.get("disk_nodes", [])
            if node not in expected_nodes
        ]
        stale_nodes = [n for n in all_unexpected if n not in running_nodes]
        still_running = [n for n in all_unexpected if n in running_nodes]

        forgotten = []
        failed = []
        for node in stale_nodes:
            try:
                process = container.exec(
                    ["rabbitmqctl", "forget_cluster_node", node],
                    timeout=5 * 60,
                )
                process.wait_output()
                forgotten.append(node)
            except ExecError as e:
                if "not in the cluster" in e.stderr:
                    forgotten.append(node)
                    continue
                failed.append(node)
                logger.warning(
                    "Unable to forget stale cluster node %s: %s", node, e
                )

        event.set_results(
            {
                "forgotten": json.dumps(forgotten),
                "failed": json.dumps(failed),
                "still-running": json.dumps(still_running),
            }
        )

    def _ensure_broker_running(self, event: EventBase | None = None) -> bool:
        """Ensure the local broker is running, starting it when possible."""
        if self._rabbitmq_running():
            return True

        if not self._reconcile_workload(event):
            return False

        if not self._rabbitmq_running():
            if event is not None:
                logger.debug(
                    "RabbitMQ not running yet after reconciliation, deferring %s",
                    type(event).__name__,
                )
                event.defer()
            return False

        return True

    def _reconcile_operator_user(self, event: EventBase | None = None) -> bool:
        """Ensure the leader has bootstrapped the operator user."""
        if not self.unit.is_leader():
            return True

        if self.peers.operator_user_created:
            return True

        if not self._rabbitmq_running():
            return False

        if self._recover_operator_user_peer_flag():
            self.peers.set_operator_user_created(self._operator_user)
            return True

        return self._bootstrap_operator_user(event)

    def _recover_operator_user_peer_flag(self) -> bool:
        """Recover missing peer metadata if the operator user already works."""
        return self._operator_user_auth_valid()

    def _bootstrap_operator_user(self, event: EventBase | None = None) -> bool:
        """Create the operator user and defer on transient bootstrap races."""
        try:
            self._initialize_operator_user()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.warning("RabbitMQ auth not ready, deferring bootstrap")
                if event is not None:
                    event.defer()
                return False
            raise
        except requests.exceptions.ConnectionError as e:
            logger.warning("RabbitMQ not ready. Deferring. %s", e)
            if event is not None:
                event.defer()
            return False
        return True

    def _requested_amqp_relations(
        self,
    ) -> list[tuple[ops.Relation, str, str, bool]]:
        """Return AMQP relations that have requested credentials."""
        relations = []
        for relation in self.model.relations[AMQP_RELATION]:
            if not relation.active:
                continue
            username = self.amqp_provider.username(relation)
            vhost = self.amqp_provider.vhost(relation)
            if not username or not vhost:
                continue
            relations.append(
                (
                    relation,
                    username,
                    vhost,
                    self.amqp_provider.external_connectivity(relation),
                )
            )
        return relations

    def _ensure_relation_credentials(
        self,
        relation: ops.Relation,
        username: str,
        vhost: str,
        external_connectivity: bool,
    ) -> None:
        """Ensure the AMQP relation has credentials and hostname."""
        if not relation.data[self.app].get("password"):
            if not self.does_vhost_exist(vhost):
                self.create_vhost(vhost)
            if not self.does_user_exist(username):
                password = self.create_user(username)
                self.peers.store_password(username, password)
            password = self.peers.retrieve_password(username)
            if password is None:
                raise RabbitOperatorError(
                    f"Password for {username} not found in peer data"
                )
            self.set_user_permissions(username, vhost)
            relation.data[self.app]["password"] = password

        relation.data[self.app]["hostname"] = self.get_hostname(
            external_connectivity
        )

    def _reconcile_amqp_relations(
        self, event: EventBase | None = None
    ) -> bool:
        """Ensure all active AMQP relations have the desired server state."""
        if (
            not self.unit.is_leader()
            or not self.peers.operator_user_created
            or not self._rabbitmq_running()
        ):
            return True

        if self._operator_user_recovery_required():
            logger.warning(OPERATOR_USER_RECOVERY_MESSAGE)
            return True

        for (
            relation,
            username,
            vhost,
            external_connectivity,
        ) in self._requested_amqp_relations():
            try:
                self._ensure_relation_credentials(
                    relation, username, vhost, external_connectivity
                )
            except requests.exceptions.HTTPError as http_e:
                if (
                    http_e.response is not None
                    and http_e.response.status_code == 401
                ):
                    logger.warning(
                        "RabbitMQ auth not ready for relation %s, deferring",
                        relation.id,
                    )
                    if event is not None:
                        event.defer()
                    return False
                raise
            except requests.exceptions.ConnectionError as e:
                logger.warning("RabbitMQ is not ready, deferring. %s", e)
                if event is not None:
                    event.defer()
                return False
        return True

    def _stored_amqp_usernames(self) -> set[str]:
        """Return usernames with passwords persisted in peer app data."""
        if not self.peers.peers_rel:
            return set()

        reserved = {
            interface_rabbitmq_peers.RabbitMQOperatorPeers.OPERATOR_PASSWORD,
            interface_rabbitmq_peers.RabbitMQOperatorPeers.OPERATOR_USER_CREATED,
            interface_rabbitmq_peers.RabbitMQOperatorPeers.ERLANG_COOKIE,
        }
        app_data = self.peers.peers_rel.data[self.peers.peers_rel.app]
        return {key for key in app_data if key not in reserved}

    def _cleanup_stale_amqp_users(self) -> None:
        """Remove users that are no longer requested by any active AMQP relation."""
        if (
            not self.unit.is_leader()
            or not self.peers.operator_user_created
            or not self._rabbitmq_running()
        ):
            return

        if self._operator_user_recovery_required():
            logger.warning(OPERATOR_USER_RECOVERY_MESSAGE)
            return

        active_usernames = {
            username for _, username, _, _ in self._requested_amqp_relations()
        }
        api = self._get_admin_api()
        for username in self._stored_amqp_usernames():
            if username in active_usernames:
                continue
            try:
                if self.does_user_exist(username):
                    api.delete_user(username)
                self.peers.delete_user(username)
            except (
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
            ):
                logger.warning("Failed to clean up user %s", username)

    def _reconcile_queue_membership(
        self, event: EventBase | None = None
    ) -> bool:
        """Ensure queue membership matches the current cluster topology."""
        if (
            not self.unit.is_leader()
            or not self._manage_queues()
            or not self.peers.operator_user_created
            or not self._rabbitmq_running()
        ):
            return True

        if self._operator_user_recovery_required():
            logger.warning(OPERATOR_USER_RECOVERY_MESSAGE)
            return True

        try:
            self.ensure_queue_ha()
        except requests.exceptions.HTTPError as http_e:
            if (
                http_e.response is not None
                and http_e.response.status_code == 401
            ):
                logger.warning(OPERATOR_USER_RECOVERY_MESSAGE)
                if event is not None:
                    event.defer()
                    return False
                return True
            raise
        except requests.exceptions.ConnectionError as e:
            logger.warning(
                "RabbitMQ is not ready for queue reconcile, skipping. %s", e
            )
            if event is not None:
                event.defer()
                return False
            return True
        except RabbitOperatorError as e:
            logger.debug("Queue HA reconcile skipped: %s", e)
        return True

    def _rabbitmq_layer(self) -> dict:
        """Pebble layer definition for RabbitMQ."""
        # NOTE(jamespage)
        # Use the full path to the rabbitmq-server binary
        # rather than the helper wrapper script to avoid
        # redirection of console output to a log file.
        layer = {
            "summary": "RabbitMQ layer",
            "description": "pebble config layer for RabbitMQ",
            "services": {
                RABBITMQ_SERVICE: {
                    "override": "replace",
                    "summary": "RabbitMQ Server",
                    "command": "/usr/lib/rabbitmq/bin/rabbitmq-server",
                    "startup": "enabled",
                    "user": RABBITMQ_USER,
                    "group": RABBITMQ_GROUP,
                },
                NOTIFIER_SERVICE: {
                    "override": "replace",
                    "summary": "Pebble notifier",
                    "command": "/usr/bin/notifier",
                    "startup": "enabled",
                    # Workaround to avoid bug
                    # https://github.com/canonical/pebble/issues/525
                    "requires": [RABBITMQ_SERVICE],
                },
            },
        }

        if not self.peers.operator_user_created:
            return layer

        layer["checks"] = {
            "alive": {
                "override": "replace",
                "level": "alive",
                "startup": "disabled",
                "period": HEALTH_CHECK_INTERVAL,
                "timeout": ALIVE_CHECK_TIMEOUT,
                "threshold": HEALTH_CHECK_THRESHOLD,
                "exec": {"command": RABBITMQ_ALIVE_CHECK_PATH},
            },
            "ready": {
                "override": "replace",
                "level": "ready",
                "startup": "disabled",
                "period": HEALTH_CHECK_INTERVAL,
                "timeout": READY_CHECK_TIMEOUT,
                "threshold": HEALTH_CHECK_THRESHOLD,
                "exec": {"command": RABBITMQ_SAFETY_CHECK_PATH},
            },
        }
        return layer

    def _reconcile_workload_layer(self, container: ops.Container) -> bool:
        """Apply the desired Pebble layer only when it has drifted."""
        desired_layer = self._rabbitmq_layer()
        current_plan = container.get_plan().to_dict()
        current_layer = {
            "services": current_plan.get("services", {}),
            "checks": current_plan.get("checks", {}),
        }
        desired_subset = {
            "services": desired_layer.get("services", {}),
            "checks": desired_layer.get("checks", {}),
        }
        if current_layer == desired_subset:
            logger.debug("Pebble layer unchanged, skipping replan")
            return False

        container.add_layer("rabbitmq", desired_layer, combine=True)
        try:
            container.replan()
        except ChangeError as exc:
            if f'Start service "{NOTIFIER_SERVICE}"' not in str(exc):
                raise
            logger.warning(
                "Ignoring transient notifier replan failure: %s", exc
            )
        except http.client.RemoteDisconnected as exc:
            logger.warning(
                "Ignoring transient Pebble disconnect during replan: %s", exc
            )
        return True

    def _render_and_push_workload_scripts(self) -> set[str]:
        """Render and push workload scripts, returning affected services."""
        changed_services = set()
        self._render_and_push_alive_check()
        if self._render_and_push_pebble_notifier():
            changed_services.add(NOTIFIER_SERVICE)
        self._render_and_push_safety_check()
        return changed_services

    def _render_and_push_alive_check(self) -> bool:
        """Render the workload alive-check script."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        script = self._render_template(
            "rabbitmq-alive-check.sh.j2",
            startup_grace_seconds=RABBITMQ_STARTUP_GRACE_SECONDS,
            safety_reason_not_running=SAFETY_REASON_NOT_RUNNING,
        )
        return self._push_text_file(
            container,
            RABBITMQ_ALIVE_CHECK_PATH,
            script,
            description="alive-check script",
            permissions=0o755,
            user=RABBITMQ_USER,
            group=RABBITMQ_GROUP,
        )

    def _ensure_workload_services(
        self,
        container: ops.Container,
        changed_services: set[str],
        *,
        layer_changed: bool = False,
    ) -> None:
        """Ensure Pebble services are started and restarted when needed."""
        started_services = set()

        if (
            not layer_changed
            and not container.get_service(RABBITMQ_SERVICE).is_running()
        ):
            logging.info("Autostarting rabbitmq")
            container.start(RABBITMQ_SERVICE)
            started_services.add(RABBITMQ_SERVICE)
            for service_name in (NOTIFIER_SERVICE,):
                container.start(service_name)
                started_services.add(service_name)
        elif not layer_changed:
            logging.debug("RabbitMQ service is running")
            for service_name in (NOTIFIER_SERVICE,):
                if not container.get_service(service_name).is_running():
                    container.start(service_name)
                    started_services.add(service_name)

        for service_name in changed_services - started_services:
            if layer_changed and service_name == RABBITMQ_SERVICE:
                continue
            service = container.get_service(service_name)
            if service.is_running():
                container.restart(service_name)
            elif not layer_changed:
                container.start(service_name)

    def _local_node_in_running_cluster(self) -> bool:
        """Return whether the local node appears in RabbitMQ running_nodes."""
        if not self._pebble_ready():
            return False

        container = self.unit.get_container(RABBITMQ_CONTAINER)
        try:
            process = container.exec(
                [
                    "rabbitmq-diagnostics",
                    "cluster_status",
                    "--formatter=json",
                ],
                timeout=30,
            )
            output, _ = process.wait_output()
            cluster_status = json.loads(output)
        except (ExecError, ModelError, json.JSONDecodeError):
            return False

        local_node = self.generate_nodename(self.unit.name)
        return local_node in cluster_status.get("running_nodes", [])

    def _health_checks_ready(self) -> bool:
        """Return whether Pebble health checks should currently run."""
        if not self.peers.operator_user_created:
            return False

        if not self._rabbitmq_running():
            return False

        return self._local_node_in_running_cluster()

    def _reconcile_health_checks(self) -> None:
        """Start or stop health checks to match local broker readiness."""
        if not self._pebble_ready():
            return

        container = self.unit.get_container(RABBITMQ_CONTAINER)
        try:
            ready_check = container.get_check("ready")
        except ModelError:
            return

        try:
            if self._health_checks_ready():
                if (
                    ready_check.status == ops.pebble.CheckStatus.INACTIVE
                    and container.exists(RABBITMQ_PROTECTOR_MARKER)
                ):
                    self._resume_listeners(
                        container, "before re-enabling health checks"
                    )
                container.start_checks("alive", "ready")
                return

            container.stop_checks("alive", "ready")
        except (APIError, ModelError) as exc:
            logger.warning("Unable to reconcile Pebble health checks: %s", exc)

    def _on_peer_relation_leaving(self, event: EventBase) -> None:
        self._departing_unit_count = 1
        try:
            self._reconcile(event)
        finally:
            self._departing_unit_count = 0

    def get_queue_growth_selector(self, min_q_len: int, max_q_len: int):
        """Select a queue growth strategy.

        Select a queue growth strategy from:
            ALL: All queues add a new replica
            NONE: No queues have additional replica added
            EVEN: Queues with an even number of replicas have additional replica added
            INDIVIDUAL: Each queue is expanded individually

        NOTE: INDIVIDUAL is expensive as an api call needs to be made
              for each queue.
        """
        if min_q_len == 0:
            selector = SELECTOR_ALL
        elif min_q_len == max_q_len:
            if max_q_len < self.min_replicas():
                # 1 -> 2
                # 2 -> 3
                selector = SELECTOR_ALL
            else:
                # All queues have enough members but queues should
                # not have an even number of replicas
                selector = SELECTOR_EVEN
        elif min_q_len > 1:
            # 2->3
            # 3->3
            # 4->5 (no even queues)
            selector = SELECTOR_EVEN
        elif min_q_len == 1:
            if max_q_len < self.min_replicas():
                # 1 -> 2
                # 2 -> 3
                selector = SELECTOR_ALL
            else:
                # Cannot use "even" as the queues with 1 node need expanding,
                # cannot use "all" as there are queues with 3+ members
                selector = SELECTOR_INDIVIDUAL
        return selector

    def unit_in_cluster(self, unit: str) -> bool:
        """Is unit in cluster according to rabbit api."""
        api = self._get_admin_api()
        joining_node = self.generate_nodename(unit)
        clustered_nodes = [n["name"] for n in api.list_nodes()]
        logging.debug(f"Found cluster nodes {clustered_nodes}")
        return joining_node in clustered_nodes

    def grow_queues_onto_unit(self, unit) -> None:
        """Grow any undersized queues onto unit."""
        api = self._get_admin_api()
        joining_node = self.generate_nodename(unit)
        queue_members = [len(q["members"]) for q in api.list_quorum_queues()]
        if not queue_members:
            logging.debug("No queues found, queue growth skipped")
            return
        queue_members.sort()
        selector = self.get_queue_growth_selector(
            queue_members[0], queue_members[-1]
        )
        logging.debug(f"selector: {selector}")
        if selector in [SELECTOR_ALL, SELECTOR_EVEN]:
            api.grow_queue(joining_node, selector)
        elif selector == SELECTOR_INDIVIDUAL:
            undersized_queues = self.get_undersized_queues()
            for q in undersized_queues:
                if joining_node not in q["members"]:
                    api.add_member(joining_node, q["vhost"], q["name"])
        else:
            logging.error(f"Unknown selectore type {selector}")

    def _on_add_member_action(self, event: ActionEvent) -> None:
        """Handle add_member charm action."""
        if not self._require_queue_management_ready(event):
            return
        api = self._get_admin_api()
        node = self.generate_nodename(event.params["unit-name"])
        vhost = event.params.get("vhost") or "/"
        queue_name = event.params["queue-name"]
        try:
            api.add_member(node, vhost, queue_name)
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ) as e:
            event.fail(f"Failed to add member: {e}")
            return
        event.set_results(
            {"queue-name": queue_name, "node": node, "vhost": vhost}
        )

    def _on_delete_member_action(self, event: ActionEvent) -> None:
        """Handle delete_member charm action."""
        if not self._require_queue_management_ready(event):
            return
        api = self._get_admin_api()
        node = self.generate_nodename(event.params["unit-name"])
        vhost = event.params.get("vhost") or "/"
        queue_name = event.params["queue-name"]
        try:
            api.delete_member(node, vhost, queue_name)
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ) as e:
            event.fail(f"Failed to delete member: {e}")
            return
        event.set_results(
            {"queue-name": queue_name, "node": node, "vhost": vhost}
        )

    def _require_queue_management_ready(self, event: ActionEvent) -> bool:
        """Check common preconditions for queue management actions.

        Returns True if the charm is ready. Returns False after calling
        event.fail() on the first failed check.
        """
        if not self.unit.is_leader():
            event.fail("Not leader unit")
            return False
        if not self._rabbitmq_running():
            event.fail("RabbitMQ not running")
            return False
        if not self.peers.operator_user_created:
            event.fail("Operator user not created")
            return False
        if self._operator_user_recovery_required():
            event.fail(OPERATOR_USER_RECOVERY_MESSAGE)
            return False
        return True

    def _on_rebalance_quorum_action(self, event: ActionEvent) -> None:
        """Handle rebalance-quorum charm action."""
        if not self._require_queue_management_ready(event):
            return
        api = self._get_admin_api()
        try:
            api.rebalance_queues()
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ) as e:
            event.fail(f"Failed to rebalance queues: {e}")
            return
        event.set_results({"status": "rebalance initiated"})

    def _on_grow_action(self, event: ActionEvent) -> None:
        """Handle grow charm action."""
        if not self._require_queue_management_ready(event):
            return
        api = self._get_admin_api()
        node = self.generate_nodename(event.params["unit-name"])
        selector = event.params["selector"]
        vhost_pattern = event.params.get("vhost-pattern") or ".*"
        queue_pattern = event.params.get("queue-pattern") or ".*"
        try:
            api.grow_queue(node, selector, vhost_pattern, queue_pattern)
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ) as e:
            event.fail(f"Failed to grow queue: {e}")
            return
        event.set_results(
            {
                "node": node,
                "selector": selector,
                "vhost-pattern": vhost_pattern,
                "queue-pattern": queue_pattern,
                "status": "grow initiated",
            }
        )

    def _on_shrink_action(self, event: ActionEvent) -> None:
        """Handle shrink charm action."""
        if not self._require_queue_management_ready(event):
            return
        api = self._get_admin_api()
        node = self.generate_nodename(event.params["unit-name"])
        result = api.shrink_queue(node)
        error_only = event.params.get("error-only", False)
        deleted = result.get("deleted", [])
        errors = result.get("errors", [])
        results = {
            "deleted-count": str(len(deleted)),
            "error-count": str(len(errors)),
        }
        if not error_only:
            results["deleted"] = json.dumps(deleted)
        if errors:
            results["errors"] = json.dumps(errors)
        event.set_results(results)



    def _on_pebble_custom_notice(self, event: PebbleCustomNoticeEvent):
        """Handle the periodic notice by reconciling desired state."""
        self._reconcile(event)

    @property
    def peers_bind_address(self) -> str | None:
        """Bind address for peer interface."""
        return self._peers_bind_address()

    def _peers_bind_address(self) -> str | None:
        """Bind address for the rabbit node to its peers.

        :returns: IP address to use for peering, or None if not yet available
        """
        address = self.model.get_binding("peers").network.bind_address
        if address is None:
            return address
        return str(address)

    @property
    def amqp_bind_address(self) -> str:
        """AMQP endpoint bind address."""
        return self._amqp_bind_address()

    def _amqp_bind_address(self) -> str:
        """Bind address for AMQP.

        :returns: IP address to use for AMQP endpoint.
        """
        return str(self.model.get_binding(AMQP_RELATION).network.bind_address)

    @property
    def ingress_address(self) -> IPv4Address | IPv6Address:
        """Network IP address for access to the RabbitMQ service."""
        return self.model.get_binding(AMQP_RELATION).network.ingress_addresses[
            0
        ]

    @property
    def _loadbalancer_annotations(self) -> dict[str, str] | None:
        """Parse and return annotations to apply to the LoadBalancer service."""
        lb_annotations = cast(
            str | None, self.config.get("loadbalancer_annotations", None)
        )
        return parse_annotations(lb_annotations)

    @property
    def _annotations_valid(self) -> bool:
        """Check if the annotations are valid.

        :return: True if the annotations are valid, False otherwise.
        """
        if self._loadbalancer_annotations is None:
            logger.error("Annotations are invalid or could not be parsed.")
            return False
        return True

    def rabbitmq_url(self, username, password, vhost) -> str:
        """URL to access RabbitMQ unit."""
        return (
            f"rabbit://{username}:{password}"
            f"@{self.ingress_address}:{RABBITMQ_SERVICE_PORT}/{vhost}"
        )

    def does_user_exist(self, username: str) -> bool:
        """Does the username exist in RabbitMQ?

        :param username: username to check for
        :returns: whether username was found
        """
        api = self._get_admin_api()
        try:
            api.get_user(username)
        except requests.exceptions.HTTPError as e:
            # Username does not exist
            if e.response is not None and e.response.status_code == 404:
                return False
            raise
        return True

    def does_vhost_exist(self, vhost: str) -> bool:
        """Does the vhost exist in RabbitMQ?

        :param vhost: vhost to check for
        :returns: whether vhost was found
        """
        api = self._get_admin_api()
        try:
            api.get_vhost(vhost)
        except requests.exceptions.HTTPError as e:
            # Vhost does not exist
            if e.response is not None and e.response.status_code == 404:
                return False
            raise
        return True

    def create_user(self, username: str) -> str:
        """Create user in rabbitmq.

        Return the password for the user.

        :param username: username to create
        :returns: password for username
        """
        api = self._get_admin_api()
        _password = secrets.token_urlsafe(12)
        api.create_user(username, _password)
        return _password

    def set_user_permissions(
        self,
        username: str,
        vhost: str,
        configure: str = ".*",
        write: str = ".*",
        read: str = ".*",
    ) -> None:
        """Set permissions for a RabbitMQ user.

        :param username: User to change permissions for
        :param configure: Configure perms
        :param write: Write perms
        :param read: Read perms
        """
        api = self._get_admin_api()
        api.create_user_permission(
            username, vhost, configure=configure, write=write, read=read
        )

    def create_vhost(self, vhost: str) -> None:
        """Create virtual host in RabbitMQ.

        :param vhost: virtual host to create.
        """
        api = self._get_admin_api()
        api.create_vhost(vhost)

    def _enable_plugin(self, plugin: str) -> None:
        """Enable plugin.

        Enable a RabbitMQ plugin.

        :param plugin: Plugin to enable.
        """
        if plugin not in self._stored.enabled_plugins:
            self._stored.enabled_plugins.append(plugin)

    def _disable_plugin(self, plugin: str) -> None:
        """Disable plugin.

        Disable a RabbitMQ plugin.

        :param plugin: Plugin to disable.
        """
        if plugin in self._stored.enabled_plugins:
            self._stored.enabled_plugins.remove(plugin)

    @property
    def hostname(self) -> str:
        """Hostname for access to RabbitMQ."""
        return f"{self.app.name}-endpoints.{self.model.name}.svc.cluster.local"

    def get_hostname(self, external_connectivity: bool = False) -> str:
        """Hostname for access to RabbitMQ."""
        if not external_connectivity:
            return self.hostname

        return self._get_loadbalancer_ip() or self.hostname

    @property
    def _rabbitmq_mgmt_url(self) -> str:
        """Management URL for RabbitMQ."""
        # Use localhost for admin ACL
        return f"http://localhost:{RABBITMQ_MANAGEMENT_PORT}"

    @property
    def _operator_password(self) -> str | None:
        """Return the operator password.

        If the operator password does not exist on the peer relation, create a
        new one and update the peer relation. It is necessary to store this on
        the peer relation so that it is not lost on any one unit's local
        storage. If the leader is deposed, the new leader continues to have
        administrative access to the message queue.

        :returns: String password or None
        :rtype: Unition[str, None]
        """
        if not self.peers.operator_password and self.unit.is_leader():
            self.peers.set_operator_password(secrets.token_urlsafe(12))
        return self.peers.operator_password

    @property
    def cluster_name(self) -> str:
        """Cluster name exposed by RabbitMQ."""
        return f"{self.model.name}-{self.app.name}"

    @property
    def rabbit_running(self) -> bool:
        """Check whether RabbitMQ is running by probing the broker."""
        running = self._rabbitmq_running()
        if running:
            self._refresh_rabbitmq_version()
        return running

    def _ensure_cluster_name(self) -> None:
        """Ensure a stable cluster name is configured for this deployment."""
        if not self.unit.is_leader():
            return

        try:
            api = self._get_admin_api()
            if api.get_cluster_name() == self.cluster_name:
                return
            api.set_cluster_name(self.cluster_name)
        except requests.exceptions.ConnectionError as e:
            logger.warning(
                "RabbitMQ not ready to reconcile cluster name yet: %s", e
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.warning(
                    "RabbitMQ auth not ready to reconcile cluster name yet"
                )
                return
            raise

    def _get_admin_api(
        self, username: str | None = None, password: str | None = None
    ) -> rabbit_extended_api.ExtendedAdminApi:
        """Return an administrative API for RabbitMQ.

        :username: Username to access RMQ API
                   (defaults to generated operator user)
        :password: Password to access RMQ API
                   (defaults to generated operator password)
        :returns: The RabbitMQ administrative API object
        """
        username = username or self._operator_user
        password = password or self._operator_password
        return rabbit_extended_api.ExtendedAdminApi(
            url=self._rabbitmq_mgmt_url, auth=(username, password)
        )

    def _run_rabbitmqctl(
        self, *args: str, timeout: int = 30
    ) -> tuple[str, str]:
        """Run a rabbitmqctl command in the workload container."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        process = container.exec(["rabbitmqctl", *args], timeout=timeout)
        return process.wait_output()

    def _operator_user_auth_valid(self) -> bool:
        """Whether the stored operator credentials are valid on the broker."""
        if not self._operator_password:
            return False
        if not self._rabbitmq_running():
            return False

        try:
            self._run_rabbitmqctl(
                "authenticate_user",
                self._operator_user,
                self._operator_password,
            )
        except (ExecError, ModelError):
            return False
        return True

    def _operator_user_recovery_required(self) -> bool:
        """Whether the leader needs manual operator-user recovery."""
        if not self.unit.is_leader():
            return False
        if not self.peers.operator_user_created:
            return False
        return not self._operator_user_auth_valid()

    @property
    def cluster_partition_handling(self) -> str:
        """Cluster partition handling policy."""
        value = self.config["cluster-partition-handling"]
        if value not in VALID_CLUSTER_PARTITION_HANDLING:
            raise RabbitOperatorError(
                "cluster-partition-handling must be one of "
                + ", ".join(sorted(VALID_CLUSTER_PARTITION_HANDLING))
            )
        return value

    @property
    def protect_members(self) -> bool:
        """Whether fail-closed protection is enabled."""
        return bool(self.config["protect-members"])

    @property
    def resolved_disk_free_limit_bytes(self) -> int:
        """Return the numeric RabbitMQ disk free limit."""
        configured = str(self.config["disk-free-limit-bytes"]).strip()
        pvc_capacity = self._rabbitmq_data_pvc_capacity_bytes
        try:
            parsed = self._bytes_from_string(configured)
        except ValueError as e:
            raise RabbitOperatorError(
                f"Invalid disk-free-limit-bytes value: {configured}"
            ) from e

        if parsed == "auto":
            resolved = int(pvc_capacity * 0.10)
        else:
            resolved = parsed

        if resolved <= 0:
            raise RabbitOperatorError(
                "disk-free-limit-bytes must resolve to a value greater than 0"
            )
        if resolved > pvc_capacity:
            raise RabbitOperatorError(
                "disk-free-limit-bytes exceeds bound rabbitmq-data PVC capacity"
            )
        return resolved

    @property
    def resolved_wal_max_size_bytes(self) -> int:
        """Return the safe Raft WAL max size in bytes."""
        pvc_capacity = self._rabbitmq_data_pvc_capacity_bytes
        disk_free_limit = self.resolved_disk_free_limit_bytes
        wal_max_size = min(
            WAL_MAX_SIZE_CAP, (pvc_capacity - disk_free_limit) // 2
        )
        if wal_max_size <= 0:
            raise RabbitOperatorError(
                "PVC capacity too small for a safe Raft WAL size"
            )
        return wal_max_size

    def _bytes_from_string(self, value: str) -> int | str:
        """Parse a raw byte value or human-readable size string."""
        normalized = value.strip()
        if normalized.lower() == "auto":
            return "auto"
        if normalized.isdigit():
            return int(normalized)

        match = re.fullmatch(
            r"(?i)\s*(\d+)\s*([kmgtp](?:i|b)?)\s*", normalized
        )
        if not match:
            raise ValueError(f"Unsupported size value: {value}")

        amount = int(match.group(1))
        suffix = match.group(2).upper()
        return amount * BYTE_SUFFIXES[suffix]

    @functools.cached_property
    def _rabbitmq_data_pvc_capacity_bytes(self) -> int:
        """Resolve the rabbitmq-data volume capacity from the mounted filesystem.

        Uses os.statvfs inside the workload container to read the total
        capacity of the filesystem mounted at RABBITMQ_DATA_DIR, avoiding
        Kubernetes API calls to the Pod and PersistentVolumeClaim objects.
        """
        try:
            container = self.unit.get_container(RABBITMQ_CONTAINER)
            connected = container.can_connect()
        except (ModelError, RuntimeError):
            connected = False
        if not connected:
            raise RabbitOperatorError(
                "Cannot determine storage capacity: "
                "container not yet available"
            )

        try:
            process = container.exec(
                [
                    "python3",
                    "-c",
                    (
                        "import os,json; "
                        f"s=os.statvfs({RABBITMQ_DATA_DIR!r}); "
                        "print(json.dumps({'total':s.f_frsize*s.f_blocks}))"
                    ),
                ],
                timeout=10,
            )
            stdout, _ = process.wait_output()
        except ExecError as e:
            raise RabbitOperatorError(
                f"Failed to stat {RABBITMQ_DATA_DIR}: {e}"
            ) from e

        try:
            data = json.loads(stdout.strip())
            total = int(data["total"])
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise RabbitOperatorError(
                f"Unexpected statvfs output for {RABBITMQ_DATA_DIR}: "
                f"{stdout.strip()!r}"
            ) from e

        if total <= 0:
            raise RabbitOperatorError(
                f"{RABBITMQ_DATA_DIR} reports zero capacity"
            )
        return total

    def _refresh_rabbitmq_version(self) -> None:
        """Refresh the cached RabbitMQ version when the management API is ready."""
        try:
            if self.peers.operator_user_created:
                api = self._get_admin_api()
            else:
                api = self._get_admin_api("guest", "guest")
            overview = api.overview()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ):
            logger.debug("Management API not ready, skipping version refresh")
            return
        self._stored.rabbitmq_version = overview.get("product_version")

    def _on_pebble_check_failed(self, event: PebbleCheckFailedEvent) -> None:
        """React to a Pebble health check failure."""
        if event.info.name != "ready":
            return
        self._reconcile_listener_protection(safe=False)

    def _on_pebble_check_recovered(
        self, event: PebbleCheckRecoveredEvent
    ) -> None:
        """React to a Pebble health check recovery."""
        if event.info.name != "ready":
            return
        self._reconcile_listener_protection(safe=True)

    def _read_safety_status(self) -> tuple[bool, str]:
        """Read broker safety from the Pebble ready check without shell-outs."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        try:
            check = container.get_check("ready")
        except ModelError:
            return True, "Safety check not yet available"

        # INACTIVE means the check hasn't run yet (e.g. just after layer apply).
        # Treat as safe — we don't want to block during startup before the
        # first check cycle completes.
        if check.status != ops.pebble.CheckStatus.DOWN:
            return True, "safe"

        try:
            reason = container.pull(RABBITMQ_SAFETY_REASON_FILE).read().strip()
        except PathError:
            reason = "Safety check failing"
        return False, reason

    def _reconcile_listener_protection(self, safe: bool) -> None:
        """Suspend or resume listeners based on safety state."""
        if not self._pebble_ready():
            return

        container = self.unit.get_container(RABBITMQ_CONTAINER)
        marker_exists = container.exists(RABBITMQ_PROTECTOR_MARKER)
        protect_members = self.protect_members

        if safe:
            if not marker_exists:
                return
            self._resume_listeners(container, "after safety recovered")
            return

        if not protect_members:
            if marker_exists:
                self._resume_listeners(container, "while disabling protection")
            return

        if marker_exists:
            return

        self._suspend_listeners(container)

    def _resume_listeners(
        self, container: ops.Container, context: str
    ) -> None:
        """Resume listeners and clear the charm marker."""
        try:
            container.exec(
                ["rabbitmqctl", "await_startup"], timeout=60
            ).wait_output()
            container.exec(
                ["rabbitmqctl", "resume_listeners"], timeout=30
            ).wait_output()
            container.remove_path(RABBITMQ_PROTECTOR_MARKER)
        except (ExecError, PathError):
            logger.warning(
                "Failed to resume listeners %s", context, exc_info=True
            )

    def _suspend_listeners(self, container: ops.Container) -> None:
        """Suspend listeners and write the charm marker."""
        try:
            container.exec(
                ["rabbitmqctl", "suspend_listeners"], timeout=30
            ).wait_output()
            container.push(
                RABBITMQ_PROTECTOR_MARKER,
                "suspended-by-charm\n",
                user=RABBITMQ_USER,
                group=RABBITMQ_GROUP,
                permissions=0o640,
                make_dirs=True,
            )
        except ExecError:
            logger.warning(
                "Failed to suspend listeners while entering protection mode",
                exc_info=True,
            )

    def _initialize_operator_user(self) -> None:
        """Initialize the operator administrative user.

        By default, the RabbitMQ admin interface has an administravie user
        'guest' with password 'guest'. We are exposing the admin interface
        so we must create a new administravie user and remove the guest
        user.

        Create the 'operator' administravie user, grant it permissions and
        tell the peer relation this is done.

        Burn the bridge behind us and remove the guest user.
        """
        logging.info("Initializing the operator user.")
        # Use guest to create operator user
        api = self._get_admin_api("guest", "guest")
        api.create_user(
            self._operator_user,
            self._operator_password,
            tags=["administrator"],
        )
        api.create_user_permission(self._operator_user, vhost="/")
        self.peers.set_operator_user_created(self._operator_user)
        # Burn the bridge behind us.
        # We do not want to leave a known user/pass available
        logging.warning("Deleting the guest user.")
        api.delete_user("guest")

    def _recreate_operator_user(self) -> None:
        """Recreate the operator administrative user from peer data."""
        password = self._operator_password
        if not password:
            raise RabbitOperatorError("Operator password not available")

        try:
            self._run_rabbitmqctl("add_user", self._operator_user, password)
        except ExecError as e:
            output = f"{e.stdout}\n{e.stderr}".lower()
            if "already exists" not in output:
                raise RabbitOperatorError(
                    f"Failed to add operator user: {e.stderr or e.stdout}"
                ) from e
            self._run_rabbitmqctl(
                "change_password", self._operator_user, password
            )

        self._run_rabbitmqctl(
            "set_user_tags", self._operator_user, "administrator"
        )
        self._run_rabbitmqctl(
            "set_permissions",
            "-p",
            "/",
            self._operator_user,
            ".*",
            ".*",
            ".*",
        )
        try:
            self._run_rabbitmqctl("delete_user", "guest")
        except ExecError as e:
            output = f"{e.stdout}\n{e.stderr}".lower()
            if "does not exist" not in output and "no_such_user" not in output:
                raise RabbitOperatorError(
                    f"Failed to delete guest user: {e.stderr or e.stdout}"
                ) from e

        self.peers.set_operator_user_created(self._operator_user)

    def _ensure_ownership(
        self, container: ops.Container, path: str, user: str, group: str
    ) -> bool:
        """Ensure ownership of a path.

        :param container: The container to check ownership in
        :param path: Path to check ownership of
        :param user: User to set ownership to
        :param group: Group to set ownership to
        :returns: True if ownership was set, False otherwise
        """
        try:
            paths = container.list_files(path, itself=True)
            if paths[0].user != user or paths[0].group != group:
                logger.info(
                    "Setting ownership of %s to %s:%s", path, user, group
                )
                container.exec(
                    ["chown", "-R", f"{user}:{group}", path]
                ).wait_output()
        except ExecError as e:
            logger.error(f"Failed to set ownership: {e.stderr}")
            return False
        return True

    def _set_ownership_on_data_dir(self) -> bool:
        """Set ownership on /var/lib/rabbitmq."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        try:
            container.exec(["mountpoint", RABBITMQ_DATA_DIR]).wait_output()
        except ExecError:
            logger.info(f"{RABBITMQ_DATA_DIR} is not mounted yet.")
            return False

        if not self._ensure_ownership(
            container,
            RABBITMQ_DATA_DIR,
            RABBITMQ_USER,
            RABBITMQ_GROUP,
        ):
            return False

        paths = container.list_files(RABBITMQ_DATA_DIR, pattern="mnesia")
        if len(paths) == 0:
            logger.info("Creating mnesia directory")
            container.make_dir(
                RABBITMQ_MNESIA_DIR,
                permissions=0o750,
                user=RABBITMQ_USER,
                group=RABBITMQ_GROUP,
            )
        else:
            return self._ensure_ownership(
                container,
                RABBITMQ_MNESIA_DIR,
                RABBITMQ_USER,
                RABBITMQ_GROUP,
            )

        return True

    def _ensure_erlang_cookie(self):
        """Ensure an erlang cookie file is present."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        if self.peers.erlang_cookie:
            container.push(
                RABBITMQ_COOKIE_PATH,
                self.peers.erlang_cookie,
                permissions=0o600,
                make_dirs=True,
                user=RABBITMQ_USER,
                group=RABBITMQ_GROUP,
            )
            return
        if (
            not container.exists(RABBITMQ_COOKIE_PATH)
            and self.unit.is_leader()
        ):
            logger.info("Creating erlang cookie file")
            cookie = secrets.token_hex(32)
            container.push(
                path=RABBITMQ_COOKIE_PATH,
                source=cookie,
                make_dirs=True,
                user=RABBITMQ_USER,
                group=RABBITMQ_GROUP,
                permissions=0o600,
            )
            # _ensure_erlang_cookie is called after checking the peer
            # relation is present.
            self.peers.set_erlang_cookie(cookie)

    def _render_and_push_config_files(self) -> set[str]:
        """Render and push configuration files."""
        changed_services = set()
        if self._render_and_push_enabled_plugins():
            changed_services.add(RABBITMQ_SERVICE)
        if self._render_and_push_rabbitmq_conf():
            changed_services.add(RABBITMQ_SERVICE)
        if self._render_and_push_rabbitmq_env():
            changed_services.add(RABBITMQ_SERVICE)
        return changed_services

    def _push_text_file(
        self,
        container: ops.Container,
        path: str,
        content: str,
        *,
        description: str,
        **kwargs,
    ) -> bool:
        """Push a workload file only when its content has changed."""
        try:
            with container.pull(path) as stream:
                current_content = stream.read()
        except PathError:
            current_content = None

        if current_content == content:
            logger.debug("%s unchanged, skipping push", description)
            return False

        logger.info("Pushing new %s", description)
        container.push(path, content, make_dirs=True, **kwargs)
        return True

    def _render_and_push_enabled_plugins(self) -> bool:
        """Render and push enabled plugins config."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        enabled_plugins = ",".join(self._stored.enabled_plugins)
        enabled_plugins_template = f"[{enabled_plugins}]."
        return self._push_text_file(
            container,
            "/etc/rabbitmq/enabled_plugins",
            enabled_plugins_template,
            description="enabled_plugins",
        )

    def _render_and_push_rabbitmq_conf(self) -> bool:
        """Render and push rabbitmq conf."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        rabbitmq_conf = self._render_template(
            "rabbitmq.conf.j2",
            loopback_users="none",
            app_name=self.app.name,
            cluster_partition_handling=self.cluster_partition_handling,
            disk_free_limit_bytes=self.resolved_disk_free_limit_bytes,
            wal_max_size_bytes=self.resolved_wal_max_size_bytes,
        )
        return self._push_text_file(
            container,
            "/etc/rabbitmq/rabbitmq.conf",
            rabbitmq_conf,
            description="rabbitmq.conf",
        )

    @property
    def _expected_cluster_size(self) -> int:
        """Return the expected number of cluster members from peer relation.

        During relation-departed hooks the departing unit is still present in
        relation.units, so we subtract any known departing units.
        """
        rel = self.peers.peers_rel
        if rel is None:
            return 1
        return max(1, len(rel.units) + 1 - self._departing_unit_count)

    def _render_and_push_safety_check(self) -> bool:
        """Render the workload safety-check script."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        script = self._render_template(
            "rabbitmq-safety-check.sh.j2",
            safety_reason_file=RABBITMQ_SAFETY_REASON_FILE,
            safety_reason_not_running=SAFETY_REASON_NOT_RUNNING,
            safety_reason_local_alarms=SAFETY_REASON_LOCAL_ALARMS,
            safety_reason_cluster_status=SAFETY_REASON_CLUSTER_STATUS,
            protect_members=str(self.protect_members).lower(),
            amqp_port=RABBITMQ_SERVICE_PORT,
            expected_cluster_size=self._expected_cluster_size,
        )
        return self._push_text_file(
            container,
            RABBITMQ_SAFETY_CHECK_PATH,
            script,
            description="safety-check script",
            permissions=0o755,
            user=RABBITMQ_USER,
            group=RABBITMQ_GROUP,
        )

    def _render_and_push_pebble_notifier(self) -> bool:
        """Render notifier script and push to workload container."""
        auto_ha_frequency = int(self.config["auto-ha-frequency"])
        if auto_ha_frequency < 1:
            msg = "auto-ha-frequency must be greater than 0"
            logger.error(msg)
            raise RabbitOperatorError(msg)
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        notifier = self._render_template(
            "notifier.sh.j2",
            auto_ha_frequency_minutes=auto_ha_frequency,
            auto_ha_frequency_seconds=auto_ha_frequency * 60,
            timer_notice=TIMER_NOTICE,
        )
        return self._push_text_file(
            container,
            "/usr/bin/notifier",
            notifier,
            description="notifier script",
            permissions=0o755,
        )

    def generate_nodename(self, unit_name) -> str:
        """K8S DNS nodename for local unit."""
        return (
            f"rabbit@{unit_name.replace('/', '-')}.{self.app.name}-endpoints"
        )

    @property
    def nodename(self) -> str:
        """K8S DNS nodename for local unit."""
        return self.generate_nodename(self.unit.name)

    def _render_and_push_rabbitmq_env(self) -> bool:
        """Render and push rabbitmq-env conf."""
        container = self.unit.get_container(RABBITMQ_CONTAINER)
        rabbitmq_env = self._render_template(
            "rabbitmq-env.conf.j2", nodename=self.nodename
        )
        return self._push_text_file(
            container,
            "/etc/rabbitmq/rabbitmq-env.conf",
            rabbitmq_env,
            description="rabbitmq-env.conf",
        )

    def _render_template(self, template_name: str, **context) -> str:
        """Render a Jinja template from src/templates."""
        template = TEMPLATE_ENV.get_template(template_name)
        return template.render(**context)

    def _on_get_operator_info_action(self, event) -> None:
        """Action to get operator user and password information.

        Set event results with operator user and password for accessing the
        administrative web interface.
        """
        data = {
            "operator-user": self._operator_user,
            "operator-password": self._operator_password,
        }
        event.set_results(data)

    def _on_recreate_operator_user_action(self, event: ActionEvent) -> None:
        """Action to recreate the operator user from peer application data."""
        if not self.unit.is_leader():
            event.fail("Not leader unit, unable to recreate operator user")
            return

        if not self._rabbitmq_running():
            event.fail(
                "RabbitMQ not running, unable to recreate operator user"
            )
            return

        if not self._operator_password:
            event.fail("Operator password not available")
            return

        try:
            self._recreate_operator_user()
        except RabbitOperatorError as e:
            event.fail(str(e))
            return
        except (ExecError, ModelError) as e:
            event.fail(str(e))
            return

        event.set_results({"operator-user": self._operator_user})

    def _manage_queues(self) -> bool:
        """Whether the charm should manage queue membership."""
        return bool(self.config.get("minimum-replicas"))

    def min_replicas(self) -> int | None:
        """The minimum number of replicas a queue should have."""
        return self.config.get("minimum-replicas")

    def get_undersized_queues(self) -> list[dict]:
        """Return a list of queues which have fewer members than minimum."""
        api = self._get_admin_api()
        undersized_queues = [
            q
            for q in api.list_quorum_queues()
            if len(q["members"]) < self.min_replicas()
        ]
        return undersized_queues

    def _on_update_status(self, _) -> None:
        """Periodic status hook triggers reconciliation without deferral."""
        self._reconcile(None)

    def _configuration_error(self) -> str | None:
        """Return a blocking configuration error, if any."""
        if self.unit.is_leader() and not self._annotations_valid:
            return "Invalid config value 'loadbalancer_annotations'"

        try:
            self.cluster_partition_handling
            self.resolved_disk_free_limit_bytes
            self.resolved_wal_max_size_bytes
        except RabbitOperatorError as e:
            return str(e)
        return None

    def _undersized_queue_count(self) -> int:
        """Return the number of undersized queues, or 0 if not applicable."""
        if (
            not self.unit.is_leader()
            or not self._manage_queues()
            or not self.peers.operator_user_created
            or not self._rabbitmq_running()
        ):
            return 0
        try:
            return len(self.get_undersized_queues())
        except requests.exceptions.ConnectionError as e:
            logger.debug(
                "RabbitMQ management API not ready for queue status, skipping: %s",
                e,
            )
            return 0

    def _pre_broker_status(self) -> ops.StatusBase | None:
        """Return a blocking or waiting status before local broker checks."""
        if error := self._configuration_error():
            return BlockedStatus(error)

        if not self.unit.is_leader() and not self.peers.erlang_cookie:
            return WaitingStatus("Waiting for leader to provide erlang cookie")

        if self.peers.operator_user_created:
            return None

        if self.unit.is_leader():
            return WaitingStatus("Waiting for RabbitMQ to start")

        return WaitingStatus("Waiting for leader to create operator user")

    def _running_broker_status(self) -> ops.StatusBase | None:
        """Return a unit status once RabbitMQ is running locally."""
        if self._operator_user_recovery_required():
            return BlockedStatus(OPERATOR_USER_RECOVERY_MESSAGE)

        if not self._health_checks_ready():
            return WaitingStatus(HEALTH_CHECK_WAITING_MESSAGE)

        safe, reason = self._read_safety_status()
        if self._stored.rabbitmq_version:
            self.unit.set_workload_version(self._stored.rabbitmq_version)

        if not safe:
            return self._unsafe_status(reason)

        undersized_queues = self._undersized_queue_count()
        if undersized_queues:
            return ActiveStatus(
                f"WARNING: {undersized_queues} Queue(s) with insufficient members"
            )

        return ActiveStatus()

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        """Collect unit status from current state rather than event deltas."""
        if status := self._pre_broker_status():
            event.add_status(status)
            return

        if not self._rabbitmq_running():
            event.add_status(BlockedStatus(SAFETY_REASON_NOT_RUNNING))
            return

        event.add_status(self._running_broker_status())

    def _validate_disk_free_limit(self) -> bool:
        """Validate that disk-free-limit-bytes resolves cleanly."""
        try:
            self.resolved_disk_free_limit_bytes
        except RabbitOperatorError as e:
            logger.debug("Disk free limit is not yet valid: %s", e)
            return False
        return True

    def _unsafe_status(self, reason: str):
        """Return unit status for an unsafe broker state."""
        if self.protect_members:
            return BlockedStatus(f"Protection mode: {reason}")
        return ActiveStatus(f"WARNING: protection disabled ({reason})")

    def create_amqp_credentials(
        self,
        event: ops.RelationEvent,
        username: str,
        vhost: str,
        external_connectivity: bool,
    ) -> None:
        """Set AMQP Credentials.

        :param event: The current event
        :param username: The requested username
        :param vhost: The requested vhost
        :param external_connectivity: Whether to use external connectivity
        """
        # TODO TLS Support. Existing interfaces set ssl_port and ssl_ca
        logging.debug("Setting amqp connection information.")

        # NOTE: fast exit if credentials are already on the relation
        if event.relation.data[self.app].get("password"):
            logging.debug(f"Credentials already provided for {username}")
            return

        if not self._ensure_broker_running(event):
            return
        try:
            self._ensure_relation_credentials(
                event.relation,
                username,
                vhost,
                external_connectivity,
            )
        except requests.exceptions.HTTPError as http_e:
            # If the peers binding address exists, but the operator has not
            # been set up yet, the command will fail with a http 401 error and
            # unauthorized in the message. Just check the status code for now.
            if (
                http_e.response is not None
                and http_e.response.status_code == 401
            ):
                logger.warning(
                    "RabbitMQ not fully configured yet, deferring. "
                    "Status: %s",
                    http_e.response.status_code,
                )
                event.defer()
            else:
                raise
        except requests.exceptions.ConnectionError as e:
            logging.warning(
                f"Rabbitmq is not ready, deferring. Errno: {e.errno}"
            )
            event.defer()

    def _publish_relation_data(self) -> None:
        """Update all relation data to latest state."""
        for relation in self.model.relations[AMQP_RELATION]:
            try:
                is_skippable = not (
                    self.model.unit.is_leader()
                    and relation.active
                    and relation.data[self.app].get("password")
                )
            except ops.ModelError:
                logger.debug(
                    "Fail to read relation data: rel=%s rel_id=%d, skipping",
                    relation.name,
                    relation.id,
                    exc_info=True,
                )
                is_skippable = True
            if is_skippable:
                # Skip if not leader or no password yet
                continue
            relation.data[self.app]["hostname"] = self.get_hostname(
                self.amqp_provider.external_connectivity(relation)
            )

    def _get_service_account(self, event: ActionEvent) -> None:
        """Get/create service account details for access to RabbitMQ.

        :param event: The current event
        """
        if not self.unit.is_leader():
            event.fail("Not leader unit")
            return

        if not self._rabbitmq_running():
            event.fail("RabbitMQ not running")
            return

        username = event.params["username"]
        vhost = event.params["vhost"]

        try:
            if not self.does_vhost_exist(vhost):
                self.create_vhost(vhost)
            if not self.does_user_exist(username):
                password = self.create_user(username)
                self.peers.store_password(username, password)
            password = self.peers.retrieve_password(username)
            self.set_user_permissions(username, vhost)

            event.set_results(
                {
                    "username": username,
                    "password": password,
                    "vhost": vhost,
                    "ingress-address": self.ingress_address,
                    "port": RABBITMQ_SERVICE_PORT,
                    "url": self.rabbitmq_url(username, password, vhost),
                }
            )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ) as e:
            msg = f"Rabbitmq is not ready. Errno: {e.errno}"
            logging.error(msg)
            event.fail(msg)

    def _add_members_to_undersized_queues(
        self,
        api: rabbit_extended_api.ExtendedAdminApi,
        nodes: list[str],
        undersized_queues: list[dict],
        replicas: int,
        dry_run: bool,
    ) -> list[str]:
        """Add members to undersized queues.

        Simple algorithm to select nodes with fewest queues to add replicas on.
        """
        queues = api.list_quorum_queues()

        nodes_queues_count = collections.Counter({node: 0 for node in nodes})
        # Get a count of how many queues each node is a member of
        for queue in queues:
            for member in queue["members"]:
                nodes_queues_count[member] += 1

        replicated_queues = []
        for queue in undersized_queues:
            needed_replicas = replicas - len(queue["members"])
            # select node with fewest queues
            sorted_count = nodes_queues_count.most_common()
            node_candidates = []
            for node, _ in reversed(sorted_count):
                if node not in queue["members"]:
                    node_candidates.append(node)
                if len(node_candidates) >= needed_replicas:
                    break
            if len(node_candidates) < needed_replicas:
                logger.warning(
                    "Not enough nodes found to replicate queue %s to HA,"
                    " availables nodes: %s, needed nodes %s",
                    queue["name"],
                    len(node_candidates),
                    needed_replicas,
                )
            logger.debug(
                "Replicating queue %r to nodes %s, dry_run=%s",
                queue["name"],
                ", ".join(node_candidates),
                dry_run,
            )
            if not dry_run:
                for node in node_candidates:
                    api.add_member(node, queue["vhost"], queue["name"])
                    nodes_queues_count[node] += 1
                replicated_queues.append(queue["name"])
        logger.info(
            "Replicated %s queues to ensure HA, dry_run=%s",
            len(replicated_queues),
            dry_run,
        )
        return replicated_queues

    def ensure_queue_ha(self, dry_run: bool = False) -> dict[str, int]:
        """Ensure queue has HA.

        The role of this function is to ensure that all queues are available on
        at least the minimum number of replicas. If there is not enough nodes to
        support the minimum number of replicas, then this function will early
        exit and not replicate any queues.

        Must be called on the leader.

        :param dry_run: Whether to perform a dry run
        """
        undersized_queues = self.get_undersized_queues()
        if len(undersized_queues) < 1:
            msg = "No undersized queues found"
            logger.debug(msg)
            return {
                "undersized-queues": 0,
                "replicated-queues": 0,
            }

        api = self._get_admin_api()
        nodes = [node["name"] for node in api.list_nodes()]
        min_replicas = self.min_replicas()

        if len(nodes) < min_replicas:
            msg = (
                "Not enough nodes to ensure queue HA, availables nodes:"
                f" {len(nodes)}, needed nodes {min_replicas}"
            )
            logger.debug(msg)
            raise RabbitOperatorError(msg)

        replicated_queues = self._add_members_to_undersized_queues(
            api, nodes, undersized_queues, min_replicas, dry_run
        )

        if len(replicated_queues) > 0:
            logger.debug("Rebalancing queues")
            api.rebalance_queues()

        return {
            "undersized-queues": len(undersized_queues),
            "replicated-queues": len(replicated_queues),
        }

    def _ensure_queue_ha_action(self, event: ActionEvent) -> None:
        """Ensure queue has HA action.

        :param event: The current event
        """
        if not self._require_queue_management_ready(event):
            return

        if not self._manage_queues():
            msg = "Queue management is disabled, unable to ensure queue HA"
            logger.error(msg)
            event.fail(msg)
            return

        dry_run = event.params.get("dry-run", False)
        try:
            result = self.ensure_queue_ha(dry_run=dry_run)
        except RabbitOperatorError as e:
            event.fail(str(e))
            return
        event.set_results(
            {
                **result,
                "dry-run": dry_run,
            }
        )

    @property
    def _service_ports(self) -> list[ServicePort]:
        """Kubernetes service ports to be opened for this workload.

        We cannot use ops unit.open_port here because Juju will provision a ClusterIP
        but we need LoadBalancer.
        """
        amqp = ServicePort(RABBITMQ_SERVICE_PORT, name="amqp")
        management = ServicePort(RABBITMQ_MANAGEMENT_PORT, name="management")
        return [amqp, management]

    @property
    def lightkube_client(self):
        """Returns a lightkube client configured for this charm."""
        if self._lightkube_client is None:
            self._lightkube_client = Client(
                namespace=self.model.name,
                field_manager=self._lightkube_field_manager,
            )
        return self._lightkube_client

    def _get_lb_resource_manager(self):
        return KubernetesResourceManager(
            labels=create_charm_default_labels(
                self.app.name, self.model.name, scope=LB_LABEL
            ),
            resource_types={Service},
            lightkube_client=self.lightkube_client,
            logger=logger,
        )

    def _construct_lb(self) -> Service:
        return Service(
            metadata=ObjectMeta(
                name=f"{self._lb_name}",
                namespace=self.model.name,
                labels={"app.kubernetes.io/name": self.app.name},
                annotations=self._loadbalancer_annotations,
            ),
            spec=ServiceSpec(
                ports=self._service_ports,
                selector={"app.kubernetes.io/name": self.app.name},
                type="LoadBalancer",
            ),
        )

    def _reconcile_lb(self, _):
        """Reconcile the LoadBalancer's state."""
        if not self.unit.is_leader():
            return

        klm = self._get_lb_resource_manager()
        resources_list = []
        if self._annotations_valid:
            resources_list.append(self._construct_lb())
            logger.info(
                f"Patching k8s loadbalancer service object {self._lb_name}"
            )
        klm.reconcile(resources_list)

    def _on_remove(self, _):
        if not self.unit.is_leader():
            return

        logger.info(
            f"Removing k8s loadbalancer service object {self._lb_name}"
        )
        klm = self._get_lb_resource_manager()
        klm.delete()

    @functools.cache
    def _get_loadbalancer_ip(self) -> str | None:
        """Helper to get loadbalancer IP.

        Result is cached for the whole duration of a hook.
        """
        try:
            rabbitmq_service = self.lightkube_client.get(
                Service, name=self._lb_name, namespace=self.model.name
            )
        except ApiError as e:
            logger.error(f"Failed to fetch LoadBalancer {self._lb_name}: {e}")
            return None

        if not (status := getattr(rabbitmq_service, "status", None)):
            return None
        if not (load_balancer_status := getattr(status, "loadBalancer", None)):
            return None
        if not (
            ingress_addresses := getattr(load_balancer_status, "ingress", None)
        ):
            return None
        if not (ingress_address := ingress_addresses[0]):
            return None

        return ingress_address.ip


def validate_annotation_key(key: str) -> bool:
    """Validate the annotation key."""
    if len(key) > 253:
        logger.error(
            f"Invalid annotation key: '{key}'. Key length exceeds 253 characters."
        )
        return False

    if not is_qualified_name(key.lower()):
        logger.error(
            f"Invalid annotation key: '{key}'. Must follow Kubernetes annotation syntax."
        )
        return False

    if key.startswith(("kubernetes.io/", "k8s.io/")):
        logger.error(
            f"Invalid annotation: Key '{key}' uses a reserved prefix."
        )
        return False

    return True


def validate_annotation_value(value: str) -> bool:
    """Validate the annotation value."""
    if not value:
        logger.error("Invalid annotation value: value must not be empty.")
        return False

    return True


def parse_annotations(annotations: str | None) -> dict[str, str] | None:
    """Parse and validate annotations from a string.

    logic is based on Kubernetes annotation validation as described here:
    https://github.com/kubernetes/apimachinery/blob/v0.31.3/pkg/api/validation/objectmeta.go#L44
    """
    if not annotations:
        return {}

    annotations = annotations.strip().rstrip(
        ","
    )  # Trim spaces and trailing commas

    try:
        parsed_annotations = {
            key.strip(): value.strip()
            for key, value in (
                pair.split("=", 1) for pair in annotations.split(",") if pair
            )
        }
    except ValueError:
        logger.error(
            "Invalid format for 'loadbalancer_annotations'. "
            "Expected format: key1=value1,key2=value2."
        )
        return None

    # Validate each key-value pair
    for key, value in parsed_annotations.items():
        if not validate_annotation_key(key) or not validate_annotation_value(
            value
        ):
            return None

    return parsed_annotations


def is_qualified_name(value: str) -> bool:
    """Check if a value is a valid Kubernetes qualified name."""
    parts = value.split("/")
    if len(parts) > 2:
        return False  # Invalid if more than one '/'

    if len(parts) == 2:  # If prefixed
        prefix, name = parts
        if not prefix or not DNS1123_SUBDOMAIN_PATTERN.match(prefix):
            return False
    else:
        name = parts[0]  # No prefix

    if not name or len(name) > 63 or not QUALIFIED_NAME_PATTERN.match(name):
        return False

    return True


if __name__ == "__main__":
    ops.main(RabbitMQOperatorCharm)
