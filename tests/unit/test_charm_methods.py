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

"""Mock-driven unit tests for RabbitMQOperatorCharm helpers."""

import json
from pathlib import (
    Path,
)
from types import (
    SimpleNamespace,
)
from unittest.mock import (
    Mock,
    call,
    patch,
)

import httpx
import ops.model
import ops.pebble
import pytest
import requests
import yaml

import charm


def test_grafana_dashboard_uses_cos_datasource_convention():
    """Bundled dashboards should not ship legacy Grafana datasource placeholders."""
    for dashboard_path in Path("src/grafana_dashboards").glob("*.json"):
        dashboard = dashboard_path.read_text()

        assert "${DS_PROMETHEUS}" not in dashboard
        assert '"__inputs"' not in dashboard
        assert "${prometheusds}" in dashboard


def test_prometheus_alert_rules_are_well_formed():
    """Bundled alert rules should be parseable and contain the expected alerts."""
    expected_alerts = {
        "RabbitMQNodeDown": "critical",
        "RabbitMQDiskAlarm": "critical",
        "RabbitMQDiskHeadroomLow": "warning",
        "RabbitMQMemoryAlarm": "critical",
        "RabbitMQMemoryHeadroomHigh": "warning",
        "RabbitMQFDAlarm": "critical",
        "RabbitMQRaftCommitLatencyHigh": "warning",
        "RabbitMQRaftUncommittedEntriesHigh": "warning",
    }

    found_alerts = {}
    for rule_path in Path("src/prometheus_alert_rules").glob("*.*"):
        document = yaml.safe_load(rule_path.read_text())
        assert "groups" in document
        for group in document["groups"]:
            assert group["name"] == "RabbitMQ"
            for rule in group["rules"]:
                assert rule["expr"]
                assert "%%juju_topology%%" not in rule["expr"]
                found_alerts[rule["alert"]] = rule["labels"]["severity"]

    assert found_alerts == expected_alerts


def _fake_charm(**kwargs):
    """Build a thin object that can exercise charm helper methods."""
    base = {
        "app": SimpleNamespace(name="rabbitmq-k8s"),
        "unit": Mock(),
        "peers": SimpleNamespace(
            erlang_cookie=None,
            set_erlang_cookie=Mock(),
            operator_user_created=None,
            operator_password="operator-password",
            set_operator_user_created=Mock(),
        ),
        "_get_admin_api": Mock(),
        "_operator_user": "operator",
        "_operator_password": "operator-password",
        "min_replicas": lambda: 3,
        "config": {
            "cluster-partition-handling": "pause_minority",
            "disk-free-limit-bytes": "auto",
            "protect-members": True,
            "minimum-replicas": 3,
        },
        "cluster_partition_handling": "pause_minority",
        "protect_members": True,
        "resolved_disk_free_limit_bytes": 536870912,
        "_rabbitmq_data_pvc_capacity_bytes": Mock(return_value=1073741824),
        "_rabbitmq_data_pvc_name": lambda pod: charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_name(  # noqa: E501
            SimpleNamespace(), pod
        ),
        "_push_text_file": lambda container, path, content, **kwargs: charm.RabbitMQOperatorCharm._push_text_file(  # noqa: E501
            SimpleNamespace(), container, path, content, **kwargs
        ),
        "_bytes_from_string": lambda value: charm.RabbitMQOperatorCharm._bytes_from_string(  # noqa: E501
            SimpleNamespace(), value
        ),
        "peers_bind_address": "10.10.1.1",
        "get_hostname": Mock(return_value="rabbitmq-k8s-endpoints"),
        "does_vhost_exist": Mock(return_value=True),
        "create_vhost": Mock(),
        "does_user_exist": Mock(return_value=True),
        "create_user": Mock(return_value="new-password"),
        "set_user_permissions": Mock(),
        "_rabbitmq_running": Mock(return_value=True),
        "_reconcile_workload": Mock(return_value=True),
        "_reconcile_running_broker_state": Mock(),
        "_reconcile": Mock(),
        "_ensure_broker_running": Mock(return_value=True),
        "_ensure_relation_credentials": Mock(),
        "_on_update_status": Mock(),
        "_operator_user_recovery_required": Mock(return_value=False),
        "_manage_queues": Mock(return_value=True),
        "generate_nodename": lambda unit_name: charm.RabbitMQOperatorCharm.generate_nodename(
            SimpleNamespace(app=SimpleNamespace(name="rabbitmq-k8s")),
            unit_name,
        ),
        "_pebble_ready": lambda: True,
        "_render_template": lambda template_name, **context: charm.RabbitMQOperatorCharm._render_template(
            SimpleNamespace(), template_name, **context
        ),
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


class FakeConnectionError(requests.exceptions.ConnectionError):
    """Connection error carrying an errno for charm logging paths."""

    def __init__(self, message: str, errno: int = 111):
        super().__init__(message)
        self.errno = errno


def test_get_queue_growth_selector():
    """The queue growth selector follows the existing sizing rules."""
    fake = _fake_charm()

    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 1, 1)
        == "all"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 1, 2)
        == "all"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 1, 3)
        == "individual"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 2, 2)
        == "all"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 2, 3)
        == "even"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 3, 3)
        == "even"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 3, 5)
        == "even"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 4, 4)
        == "even"
    )


def test_generate_nodename():
    """Unit names are converted to the k8s RabbitMQ node format."""
    fake = _fake_charm()

    assert (
        charm.RabbitMQOperatorCharm.generate_nodename(fake, "unit/1")
        == "rabbit@unit-1.rabbitmq-k8s-endpoints"
    )


def test_unit_in_cluster():
    """Cluster membership is determined from the admin API node list."""
    admin_api = Mock()
    admin_api.list_nodes.return_value = [
        {"name": "rabbit@unit-1.rabbitmq-k8s-endpoints"}
    ]
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    assert charm.RabbitMQOperatorCharm.unit_in_cluster(fake, "unit/1")
    assert not charm.RabbitMQOperatorCharm.unit_in_cluster(fake, "unit/2")


def test_grow_queues_onto_unit():
    """Growing queues chooses the correct API call for each selector."""
    queue_one_member = {
        "name": "queue1",
        "vhost": "openstack",
        "members": ["node1"],
    }
    queue_two_member = {
        "name": "queue2",
        "vhost": "openstack",
        "members": ["node1", "node2"],
    }
    queue_three_member = {
        "name": "queue3",
        "vhost": "openstack",
        "members": ["node1", "node2", "node3"],
    }
    admin_api = Mock()
    fake = _fake_charm(
        _get_admin_api=Mock(return_value=admin_api),
        get_queue_growth_selector=Mock(
            side_effect=["all", "even", "individual"]
        ),
        get_undersized_queues=Mock(
            return_value=[
                queue_one_member,
                queue_two_member,
                queue_three_member,
            ]
        ),
    )

    admin_api.list_quorum_queues.return_value = [queue_one_member]
    charm.RabbitMQOperatorCharm.grow_queues_onto_unit(fake, "unit/1")
    admin_api.grow_queue.assert_called_once_with(
        "rabbit@unit-1.rabbitmq-k8s-endpoints", "all"
    )

    admin_api.grow_queue.reset_mock()
    admin_api.list_quorum_queues.return_value = [
        queue_two_member,
        queue_three_member,
    ]
    charm.RabbitMQOperatorCharm.grow_queues_onto_unit(fake, "unit/1")
    admin_api.grow_queue.assert_called_once_with(
        "rabbit@unit-1.rabbitmq-k8s-endpoints", "even"
    )

    admin_api.grow_queue.reset_mock()
    admin_api.list_quorum_queues.return_value = [
        queue_one_member,
        queue_two_member,
        queue_three_member,
    ]
    charm.RabbitMQOperatorCharm.grow_queues_onto_unit(fake, "unit/1")
    admin_api.add_member.assert_has_calls(
        [
            call(
                "rabbit@unit-1.rabbitmq-k8s-endpoints",
                "openstack",
                "queue1",
            ),
            call(
                "rabbit@unit-1.rabbitmq-k8s-endpoints",
                "openstack",
                "queue2",
            ),
        ]
    )


def test_no_undersized_queues():
    """No queue changes are made when nothing is undersized."""
    admin_api = Mock()
    admin_api.list_quorum_queues.return_value = []

    result = charm.RabbitMQOperatorCharm._add_members_to_undersized_queues(
        SimpleNamespace(), admin_api, ["node1", "node2", "node3"], [], 3, False
    )

    assert result == []


def test_not_enough_nodes_to_replicate():
    """Queues are still replicated onto available nodes when possible."""
    queues = [{"name": "queue1", "members": ["node1"], "vhost": "/"}]
    admin_api = Mock()
    admin_api.list_quorum_queues.return_value = queues

    result = charm.RabbitMQOperatorCharm._add_members_to_undersized_queues(
        SimpleNamespace(),
        admin_api,
        ["node1", "node2"],
        queues,
        3,
        False,
    )

    assert result == ["queue1"]
    admin_api.add_member.assert_called_once_with("node2", "/", "queue1")


def test_exact_number_of_nodes_needed():
    """Queues are added to the nodes with the lowest current membership."""
    admin_api = Mock()
    admin_api.list_quorum_queues.return_value = [
        {"name": "queue1", "members": ["node1"], "vhost": "/"},
        {"name": "queue2", "members": ["node2"], "vhost": "/"},
        {
            "name": "queue3",
            "members": ["node1", "node2", "node3"],
            "vhost": "/",
        },
    ]
    undersized_queues = [
        {"name": "queue1", "members": ["node1"], "vhost": "/"},
        {"name": "queue2", "members": ["node2"], "vhost": "/"},
    ]

    result = charm.RabbitMQOperatorCharm._add_members_to_undersized_queues(
        SimpleNamespace(),
        admin_api,
        ["node1", "node2", "node3"],
        undersized_queues,
        3,
        False,
    )

    assert result == ["queue1", "queue2"]
    admin_api.add_member.assert_has_calls(
        [
            call("node3", "/", "queue1"),
            call("node2", "/", "queue1"),
            call("node3", "/", "queue2"),
            call("node1", "/", "queue2"),
        ]
    )


@pytest.mark.parametrize(
    ("annotations", "expected_result"),
    [
        ("key1=value1,key2=value2", {"key1": "value1", "key2": "value2"}),
        ("", {}),
        (
            "key1=value1,key_2=value2,key-3=value3,",
            {"key1": "value1", "key_2": "value2", "key-3": "value3"},
        ),
        (
            "key1=value1,key2=value2,key3=value3",
            {"key1": "value1", "key2": "value2", "key3": "value3"},
        ),
        ("example.com/key=value", {"example.com/key": "value"}),
        (
            "prefix1/key=value1,prefix2/another-key=value2",
            {"prefix1/key": "value1", "prefix2/another-key": "value2"},
        ),
        (
            "key=value,key.sub-key=value-with-hyphen",
            {"key": "value", "key.sub-key": "value-with-hyphen"},
        ),
        ("key1=value1,key2=value2,key=value3,key4=", None),
        ("kubernetes.io/description=this-is-valid,custom.io/key=value", None),
        ("key1=value1,key2", None),
        ("key1=value1,example..com/key2=value2", None),
        ("key1=value1,key=value2,key3=", None),
        ("key1=value1,=value2", None),
        ("key1=value1,key=val=ue2", None),
        ("a" * 256 + "=value", None),
        ("key@=value", None),
        ("key. =value", None),
        ("key,value", None),
        ("kubernetes/description=", None),
    ],
)
def test_lb_annotations(annotations, expected_result):
    """Load-balancer annotations are parsed and validated."""
    assert charm.parse_annotations(annotations) == expected_result


def test_ensure_ownership_same_ownership():
    """Ownership is unchanged when it already matches the desired values."""
    container = Mock()
    file_info = Mock(user="rabbitmq", group="rabbitmq")
    container.list_files.return_value = [file_info]

    result = charm.RabbitMQOperatorCharm._ensure_ownership(
        SimpleNamespace(), container, "/test/path", "rabbitmq", "rabbitmq"
    )

    assert result is True
    container.list_files.assert_called_once_with("/test/path", itself=True)
    container.exec.assert_not_called()


def test_ensure_ownership_different_ownership():
    """Ownership is corrected when the target path is owned by root."""
    container = Mock()
    file_info = Mock(user="root", group="root")
    container.list_files.return_value = [file_info]

    result = charm.RabbitMQOperatorCharm._ensure_ownership(
        SimpleNamespace(), container, "/test/path", "rabbitmq", "rabbitmq"
    )

    assert result is True
    container.exec.assert_called_once_with(
        ["chown", "-R", "rabbitmq:rabbitmq", "/test/path"]
    )


def test_ensure_ownership_exec_error():
    """Ownership changes fail gracefully when chown raises ExecError."""
    container = Mock()
    file_info = Mock(user="root", group="root")
    container.list_files.return_value = [file_info]
    container.exec.side_effect = ops.pebble.ExecError(
        ["chown"], 1, "stdout", "Permission denied"
    )

    result = charm.RabbitMQOperatorCharm._ensure_ownership(
        SimpleNamespace(), container, "/test/path", "rabbitmq", "rabbitmq"
    )

    assert result is False


def test_set_ownership_on_data_dir_not_mounted():
    """Data-dir ownership waits until the storage mount exists."""
    container = Mock()
    container.exec.side_effect = ops.pebble.ExecError(
        ["mountpoint"], 1, "", "not a mountpoint"
    )
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is False
    container.exec.assert_called_once_with(
        ["mountpoint", charm.RABBITMQ_DATA_DIR]
    )


def test_set_ownership_on_data_dir_ensure_ownership_fails():
    """Data-dir ownership stops on the first ownership failure."""
    container = Mock()
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )
    fake._ensure_ownership = Mock(return_value=False)

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is False
    fake._ensure_ownership.assert_called_once_with(
        container,
        charm.RABBITMQ_DATA_DIR,
        charm.RABBITMQ_USER,
        charm.RABBITMQ_GROUP,
    )


def test_set_ownership_on_data_dir_no_mnesia_dir():
    """The mnesia directory is created when it does not exist."""
    container = Mock()
    container.list_files.return_value = []
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )
    fake._ensure_ownership = Mock(return_value=True)

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is True
    container.make_dir.assert_called_once_with(
        charm.RABBITMQ_MNESIA_DIR,
        permissions=0o750,
        user=charm.RABBITMQ_USER,
        group=charm.RABBITMQ_GROUP,
    )


def test_set_ownership_on_data_dir_mnesia_exists():
    """Both data and mnesia ownership are enforced when mnesia exists."""
    container = Mock()
    container.list_files.return_value = [Mock()]
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )
    fake._ensure_ownership = Mock(return_value=True)

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is True
    fake._ensure_ownership.assert_has_calls(
        [
            call(
                container,
                charm.RABBITMQ_DATA_DIR,
                charm.RABBITMQ_USER,
                charm.RABBITMQ_GROUP,
            ),
            call(
                container,
                charm.RABBITMQ_MNESIA_DIR,
                charm.RABBITMQ_USER,
                charm.RABBITMQ_GROUP,
            ),
        ]
    )


def test_set_ownership_on_data_dir_mnesia_ownership_fails():
    """A mnesia ownership failure returns False."""
    container = Mock()
    container.list_files.return_value = [Mock()]
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )
    fake._ensure_ownership = Mock(side_effect=[True, False])

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is False


def test_ensure_erlang_cookie_with_existing_peer_cookie():
    """An existing peer cookie is written into the workload container."""
    container = Mock()
    peers = SimpleNamespace(
        erlang_cookie="existing-cookie-value",
        set_erlang_cookie=Mock(),
    )
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        peers=peers,
    )

    charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_called_once_with(
        charm.RABBITMQ_COOKIE_PATH,
        "existing-cookie-value",
        permissions=0o600,
        make_dirs=True,
        user=charm.RABBITMQ_USER,
        group=charm.RABBITMQ_GROUP,
    )


def test_ensure_erlang_cookie_generate_new_when_leader():
    """The leader generates and stores a cookie when none exists yet."""
    container = Mock()
    container.exists.return_value = False
    peers = SimpleNamespace(
        erlang_cookie="",
        set_erlang_cookie=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = True
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit, peers=peers)

    with patch(
        "charm.secrets.token_hex", return_value="generated-cookie-value"
    ):
        charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_called_once_with(
        path=charm.RABBITMQ_COOKIE_PATH,
        source="generated-cookie-value",
        make_dirs=True,
        user=charm.RABBITMQ_USER,
        group=charm.RABBITMQ_GROUP,
        permissions=0o600,
    )
    peers.set_erlang_cookie.assert_called_once_with("generated-cookie-value")


def test_ensure_erlang_cookie_no_action_when_not_leader():
    """Non-leaders do not generate a new cookie."""
    container = Mock()
    container.exists.return_value = False
    peers = SimpleNamespace(
        erlang_cookie=None,
        set_erlang_cookie=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = False
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit, peers=peers)

    charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_not_called()
    peers.set_erlang_cookie.assert_not_called()


def test_ensure_erlang_cookie_no_action_when_file_exists():
    """No new cookie is written when the file already exists."""
    container = Mock()
    container.exists.return_value = True
    peers = SimpleNamespace(
        erlang_cookie="",
        set_erlang_cookie=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = True
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit, peers=peers)

    charm.RabbitMQOperatorCharm._ensure_erlang_cookie(fake)

    container.push.assert_not_called()
    peers.set_erlang_cookie.assert_not_called()


def test_publish_relation_data_on_leader():
    """Leaders publish the hostname to active AMQP relations with credentials."""
    app = object()
    databag = {"password": "the_password"}
    relation = SimpleNamespace(
        active=True,
        data={app: databag},
        name="amqp",
        id=1,
    )
    amqp_provider = SimpleNamespace(
        external_connectivity=Mock(return_value=False)
    )
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        app=app,
        model=SimpleNamespace(
            relations={charm.AMQP_RELATION: [relation]}, unit=unit
        ),
        amqp_provider=amqp_provider,
        get_hostname=Mock(return_value="rabbitmq-k8s-endpoints"),
    )

    charm.RabbitMQOperatorCharm._publish_relation_data(fake)

    assert databag == {
        "password": "the_password",
        "hostname": "rabbitmq-k8s-endpoints",
    }


def test_publish_relation_data_on_non_leader():
    """Non-leaders do not publish AMQP relation data."""
    app = object()
    databag = {"password": "the_password"}
    relation = SimpleNamespace(
        active=True,
        data={app: databag},
        name="amqp",
        id=1,
    )
    unit = Mock()
    unit.is_leader.return_value = False
    fake = _fake_charm(
        app=app,
        model=SimpleNamespace(
            relations={charm.AMQP_RELATION: [relation]}, unit=unit
        ),
        amqp_provider=SimpleNamespace(external_connectivity=Mock()),
        get_hostname=Mock(),
    )

    charm.RabbitMQOperatorCharm._publish_relation_data(fake)

    fake.get_hostname.assert_not_called()
    assert databag == {"password": "the_password"}


def test_publish_relation_data_on_model_error():
    """Model errors while reading relation data are ignored."""
    app = object()
    databag = Mock()
    databag.get.side_effect = ops.model.ModelError(
        "ERROR permission denied (unauthorized access)"
    )
    relation = SimpleNamespace(
        active=True,
        data={app: databag},
        name="amqp",
        id=1,
    )
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        app=app,
        model=SimpleNamespace(
            relations={charm.AMQP_RELATION: [relation]}, unit=unit
        ),
        amqp_provider=SimpleNamespace(external_connectivity=Mock()),
        get_hostname=Mock(),
    )

    charm.RabbitMQOperatorCharm._publish_relation_data(fake)

    fake.get_hostname.assert_not_called()


def test_on_peer_relation_connected_attempts_recovery_before_deferring():
    """Peer relation setup delegates to the shared reconciler."""
    event = Mock(defer=Mock())
    reconcile = Mock()
    fake = _fake_charm(_reconcile=reconcile)

    charm.RabbitMQOperatorCharm._on_peer_relation_connected(fake, event)

    reconcile.assert_called_once_with(event)


def test_on_peer_relation_connected_sets_cookie_and_initializes_user():
    """Peer relation wrapper does not bypass the holistic reconciler."""
    event = Mock(defer=Mock())
    reconcile = Mock()
    fake = _fake_charm(_reconcile=reconcile)

    charm.RabbitMQOperatorCharm._on_peer_relation_connected(fake, event)

    reconcile.assert_called_once_with(event)


def test_on_peer_relation_ready_defers_until_unit_in_cluster():
    """Peer-ready delegates to the shared reconciler."""
    event = SimpleNamespace(nodename="rabbitmq-k8s/1", defer=Mock())
    reconcile = Mock()
    fake = _fake_charm(_reconcile=reconcile)

    charm.RabbitMQOperatorCharm._on_peer_relation_ready(fake, event)

    reconcile.assert_called_once_with(event)


def test_on_peer_relation_ready_attempts_recovery_before_deferring():
    """Peer-ready wrapper delegates to the shared reconciler."""
    event = SimpleNamespace(nodename="rabbitmq-k8s/1", defer=Mock())
    reconcile = Mock()
    fake = _fake_charm(_reconcile=reconcile)

    charm.RabbitMQOperatorCharm._on_peer_relation_ready(fake, event)

    reconcile.assert_called_once_with(event)


def test_on_peer_relation_ready_leader_rebalances_when_ready():
    """Peer-ready wrapper delegates to the shared reconciler."""
    event = SimpleNamespace(nodename="rabbitmq-k8s/1", defer=Mock())
    reconcile = Mock()
    fake = _fake_charm(_reconcile=reconcile)

    charm.RabbitMQOperatorCharm._on_peer_relation_ready(fake, event)

    reconcile.assert_called_once_with(event)


def test_on_gone_away_amqp_clients_non_leader_noop():
    """AMQP relation-broken delegates to the shared reconciler."""
    relation = Mock()
    event = SimpleNamespace(relation=relation)
    reconcile = Mock()
    fake = _fake_charm(_reconcile=reconcile)

    charm.RabbitMQOperatorCharm._on_gone_away_amqp_clients(fake, event)

    reconcile.assert_called_once_with(event)


def test_on_gone_away_amqp_clients_leader_deletes_user():
    """AMQP relation-broken wrapper delegates to reconciliation."""
    relation = Mock()
    event = SimpleNamespace(relation=relation)
    reconcile = Mock()
    fake = _fake_charm(_reconcile=reconcile)

    charm.RabbitMQOperatorCharm._on_gone_away_amqp_clients(fake, event)

    reconcile.assert_called_once_with(event)


def test_does_user_exist_false_on_404():
    """User existence returns False when the API reports a 404."""
    admin_api = Mock()
    error = requests.exceptions.HTTPError("missing user")
    error.response = SimpleNamespace(status_code=404)
    admin_api.get_user.side_effect = error
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    assert not charm.RabbitMQOperatorCharm.does_user_exist(fake, "svc-user")


def test_does_vhost_exist_false_on_404():
    """Vhost existence returns False when the API reports a 404."""
    admin_api = Mock()
    error = requests.exceptions.HTTPError("missing vhost")
    error.response = SimpleNamespace(status_code=404)
    admin_api.get_vhost.side_effect = error
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    assert not charm.RabbitMQOperatorCharm.does_vhost_exist(fake, "/")


def test_create_user_returns_generated_password():
    """User creation returns the generated password."""
    admin_api = Mock()
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    with patch("charm.pwgen.pwgen", return_value="generated-password"):
        password = charm.RabbitMQOperatorCharm.create_user(fake, "svc-user")

    assert password == "generated-password"
    admin_api.create_user.assert_called_once_with(
        "svc-user", "generated-password"
    )


def test_set_user_permissions_calls_api():
    """Permission updates are delegated to the admin API."""
    admin_api = Mock()
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    charm.RabbitMQOperatorCharm.set_user_permissions(fake, "svc-user", "/")

    admin_api.create_user_permission.assert_called_once_with(
        "svc-user", "/", configure=".*", write=".*", read=".*"
    )


def test_create_vhost_calls_api():
    """Vhost creation is delegated to the admin API."""
    admin_api = Mock()
    fake = _fake_charm(_get_admin_api=Mock(return_value=admin_api))

    charm.RabbitMQOperatorCharm.create_vhost(fake, "/svc")

    admin_api.create_vhost.assert_called_once_with("/svc")


def test_operator_password_sets_password_on_leader():
    """The leader generates and stores the operator password when missing."""
    peers = SimpleNamespace(
        operator_password=None,
        set_operator_password=Mock(),
    )
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(unit=unit, peers=peers)

    with patch("charm.pwgen.pwgen", return_value="generated-password"):
        password = charm.RabbitMQOperatorCharm._operator_password.fget(fake)

    assert password is None
    peers.set_operator_password.assert_called_once_with("generated-password")


def test_rabbitmq_running_uses_diagnostics_instead_of_pebble_state():
    """Broker liveness depends on rabbitmq-diagnostics, not Pebble service metadata."""
    process = Mock(wait_output=Mock(return_value=("", "")))
    container = Mock(exec=Mock(return_value=process))
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )

    assert charm.RabbitMQOperatorCharm._rabbitmq_running(fake)
    container.exec.assert_called_once_with(
        ["rabbitmq-diagnostics", "check_running"], timeout=30
    )


def test_rabbitmq_running_returns_false_when_diagnostics_fails():
    """Diagnostics failures report RabbitMQ as down even if the container is up."""
    container = Mock()
    container.exec.side_effect = ops.pebble.ExecError(
        ["rabbitmq-diagnostics", "check_running"], 1, "", "broker stopped"
    )
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )

    assert not charm.RabbitMQOperatorCharm._rabbitmq_running(fake)


def test_rabbit_running_caches_version_when_broker_is_running():
    """The public running property refreshes version metadata opportunistically."""
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=True),
        _refresh_rabbitmq_version=Mock(),
    )

    assert charm.RabbitMQOperatorCharm.rabbit_running.fget(fake)
    fake._refresh_rabbitmq_version.assert_called_once_with()


def test_rabbit_running_returns_false_when_diagnostics_reports_down():
    """The public running property follows broker diagnostics."""
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=False),
        _refresh_rabbitmq_version=Mock(),
    )

    assert not charm.RabbitMQOperatorCharm.rabbit_running.fget(fake)
    fake._refresh_rabbitmq_version.assert_not_called()


def test_ensure_broker_running_recovers_and_defers_when_broker_stays_down():
    """Startup recovery defers the current event if RabbitMQ is still down."""
    event = Mock(defer=Mock())
    fake = _fake_charm(
        _rabbitmq_running=Mock(side_effect=[False, False]),
        _reconcile_workload=Mock(return_value=True),
        _reconcile_running_broker_state=Mock(),
    )

    assert not charm.RabbitMQOperatorCharm._ensure_broker_running(fake, event)
    fake._reconcile_workload.assert_called_once_with(event)
    fake._reconcile_running_broker_state.assert_not_called()
    event.defer.assert_called_once_with()


def test_ensure_broker_running_reconciles_broker_state_after_recovery():
    """Startup recovery ensures the local broker is started."""
    event = Mock(defer=Mock())
    fake = _fake_charm(
        _rabbitmq_running=Mock(side_effect=[False, True]),
        _reconcile_workload=Mock(return_value=True),
        _reconcile_running_broker_state=Mock(),
    )

    assert charm.RabbitMQOperatorCharm._ensure_broker_running(fake, event)
    fake._reconcile_workload.assert_called_once_with(event)
    fake._reconcile_running_broker_state.assert_not_called()
    event.defer.assert_not_called()


def test_reconcile_running_broker_state_skips_api_before_operator_user_exists():
    """Post-start reconciliation must not hit the admin API before operator bootstrap."""
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=True),
        peers=SimpleNamespace(operator_user_created=None),
        _refresh_rabbitmq_version=Mock(),
        _ensure_cluster_name=Mock(),
    )

    charm.RabbitMQOperatorCharm._reconcile_running_broker_state(fake)

    fake._refresh_rabbitmq_version.assert_not_called()
    fake._ensure_cluster_name.assert_not_called()


def test_reconcile_running_broker_state_refreshes_api_state_after_operator_user():
    """Post-start reconciliation refreshes API-backed state once bootstrap is complete."""
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=True),
        peers=SimpleNamespace(operator_user_created="rmqadmin"),
        _refresh_rabbitmq_version=Mock(),
        _ensure_cluster_name=Mock(),
    )

    charm.RabbitMQOperatorCharm._reconcile_running_broker_state(fake)

    fake._refresh_rabbitmq_version.assert_called_once_with()
    fake._ensure_cluster_name.assert_called_once_with()


def test_ensure_workload_services_starts_only_required_services():
    """Workload startup should only manage RabbitMQ-owned Pebble services."""
    rabbitmq_service = Mock(is_running=Mock(return_value=False))
    notifier_service = Mock(is_running=Mock(return_value=False))
    container = Mock()
    container.get_service.side_effect = lambda name: {
        charm.RABBITMQ_SERVICE: rabbitmq_service,
        charm.NOTIFIER_SERVICE: notifier_service,
    }[name]

    charm.RabbitMQOperatorCharm._ensure_workload_services(
        _fake_charm(), container, set()
    )

    container.start.assert_has_calls(
        [
            call(charm.RABBITMQ_SERVICE),
            call(charm.NOTIFIER_SERVICE),
        ]
    )
    container.autostart.assert_not_called()


def test_reconcile_workload_layer_skips_replan_when_plan_matches():
    """Pebble should not be replanned when the current plan already matches."""
    desired_layer = charm.RabbitMQOperatorCharm._rabbitmq_layer(
        SimpleNamespace()
    )
    container = Mock()
    container.get_plan.return_value.to_dict.return_value = {
        "services": desired_layer["services"],
        "checks": desired_layer["checks"],
    }
    fake = _fake_charm(_rabbitmq_layer=lambda: desired_layer)

    changed = charm.RabbitMQOperatorCharm._reconcile_workload_layer(
        fake, container
    )

    assert not changed
    container.add_layer.assert_not_called()
    container.replan.assert_not_called()


def test_reconcile_workload_layer_replans_when_plan_drifts():
    """Pebble should be replanned when desired state differs from the plan."""
    desired_layer = charm.RabbitMQOperatorCharm._rabbitmq_layer(
        SimpleNamespace()
    )
    container = Mock()
    container.get_plan.return_value.to_dict.return_value = {
        "services": {},
        "checks": {},
    }
    fake = _fake_charm(_rabbitmq_layer=lambda: desired_layer)

    changed = charm.RabbitMQOperatorCharm._reconcile_workload_layer(
        fake, container
    )

    assert changed
    container.add_layer.assert_called_once_with(
        "rabbitmq", desired_layer, combine=True
    )
    container.replan.assert_called_once_with()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("auto", "auto"),
        ("12345", 12345),
        ("500M", 500_000_000),
        ("500MB", 500_000_000),
        ("5G", 5_000_000_000),
        ("5Gi", 5 * 1024**3),
    ],
)
def test_bytes_from_string(value, expected):
    """Disk size strings are parsed using raw bytes or human-readable units."""
    assert (
        charm.RabbitMQOperatorCharm._bytes_from_string(
            SimpleNamespace(), value
        )
        == expected
    )


def test_bytes_from_string_rejects_invalid_values():
    """Unsupported size strings raise ValueError."""
    with pytest.raises(ValueError):
        charm.RabbitMQOperatorCharm._bytes_from_string(
            SimpleNamespace(), "definitely-not-a-size"
        )


def test_render_rabbitmq_conf_uses_safer_defaults():
    """The rendered broker config includes the new fail-closed defaults."""
    container = Mock()
    push_text_file = Mock(return_value=True)
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
        resolved_disk_free_limit_bytes=107374182,
        _push_text_file=push_text_file,
    )

    charm.RabbitMQOperatorCharm._render_and_push_rabbitmq_conf(fake)

    rendered_conf = push_text_file.call_args.args[2]
    assert "cluster_partition_handling = pause_minority" in rendered_conf
    assert "disk_free_limit.absolute = 107374182" in rendered_conf


def test_render_rabbitmq_conf_skips_push_when_unchanged():
    """Broker config should not be rewritten when the content already matches."""
    fake = _fake_charm(
        resolved_disk_free_limit_bytes=107374182,
    )
    rabbitmq_conf = charm.RabbitMQOperatorCharm._render_template(
        fake,
        "rabbitmq.conf.j2",
        loopback_users="none",
        app_name=fake.app.name,
        cluster_partition_handling=fake.cluster_partition_handling,
        disk_free_limit_bytes=fake.resolved_disk_free_limit_bytes,
    )
    stream = Mock()
    stream.read.return_value = rabbitmq_conf
    stream.__enter__ = Mock(return_value=stream)
    stream.__exit__ = Mock(return_value=None)
    container = Mock()
    container.pull.return_value = stream
    fake.unit = Mock(get_container=Mock(return_value=container))

    changed = charm.RabbitMQOperatorCharm._render_and_push_rabbitmq_conf(fake)

    assert not changed
    container.push.assert_not_called()


def test_render_and_push_config_files_marks_rabbitmq_for_restart():
    """Broker config drift should be surfaced as a RabbitMQ service change."""
    fake = _fake_charm(
        _render_and_push_enabled_plugins=Mock(return_value=True),
        _render_and_push_rabbitmq_conf=Mock(return_value=False),
        _render_and_push_rabbitmq_env=Mock(return_value=True),
    )

    changed_services = (
        charm.RabbitMQOperatorCharm._render_and_push_config_files(fake)
    )

    assert changed_services == {charm.RABBITMQ_SERVICE}


def test_rabbitmq_data_pvc_capacity_bytes():
    """The rabbitmq-data PVC capacity is resolved from the current unit pod."""
    volume_name = "rabbitmq-k8s-rabbitmq-data-e1790691"
    volume_mount = SimpleNamespace(
        name=volume_name,
        mountPath=charm.RABBITMQ_DATA_DIR,
    )
    container = SimpleNamespace(
        name=charm.RABBITMQ_CONTAINER,
        volumeMounts=[volume_mount],
    )
    volume = SimpleNamespace(
        name=volume_name,
        persistentVolumeClaim=SimpleNamespace(claimName="pvc-rabbitmq-data"),
    )
    pod = SimpleNamespace(
        spec=SimpleNamespace(containers=[container], volumes=[volume])
    )
    pvc = SimpleNamespace(status=SimpleNamespace(capacity={"storage": "1Gi"}))
    lightkube_client = SimpleNamespace(get=Mock(side_effect=[pod, pvc]))
    fake = _fake_charm(
        unit=SimpleNamespace(name="rabbitmq-k8s/0"),
        model=SimpleNamespace(name="test-model"),
        lightkube_client=lightkube_client,
        _bytes_from_string=lambda value: charm.RabbitMQOperatorCharm._bytes_from_string(  # noqa: E501
            SimpleNamespace(), value
        ),
    )

    resolved = charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes(
        fake
    )

    assert resolved == 1024**3


def test_rabbitmq_data_pvc_capacity_bytes_missing_pod_raises():
    """PVC lookup errors are surfaced as charm errors."""
    request = httpx.Request("GET", "https://kubernetes.invalid")
    response = httpx.Response(
        404,
        request=request,
        json={
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": "not found",
            "reason": "NotFound",
            "code": 404,
        },
    )
    fake = _fake_charm(
        unit=SimpleNamespace(name="rabbitmq-k8s/0"),
        model=SimpleNamespace(name="test-model"),
        lightkube_client=SimpleNamespace(
            get=Mock(
                side_effect=charm.ApiError(request=request, response=response)
            )
        ),
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes(fake)


def test_rabbitmq_data_pvc_capacity_bytes_missing_volume_raises():
    """Missing rabbitmq-data volumes are surfaced as charm errors."""
    container = SimpleNamespace(
        name=charm.RABBITMQ_CONTAINER,
        volumeMounts=[
            SimpleNamespace(
                name="rabbitmq-k8s-rabbitmq-data-e1790691",
                mountPath=charm.RABBITMQ_DATA_DIR,
            )
        ],
    )
    pod = SimpleNamespace(
        spec=SimpleNamespace(containers=[container], volumes=[])
    )
    fake = _fake_charm(
        unit=SimpleNamespace(name="rabbitmq-k8s/0"),
        model=SimpleNamespace(name="test-model"),
        lightkube_client=SimpleNamespace(get=Mock(return_value=pod)),
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes(fake)


def test_rabbitmq_data_pvc_capacity_bytes_missing_pvc_raises():
    """Missing PVC lookups are surfaced as charm errors."""
    volume_name = "rabbitmq-k8s-rabbitmq-data-e1790691"
    container = SimpleNamespace(
        name=charm.RABBITMQ_CONTAINER,
        volumeMounts=[
            SimpleNamespace(
                name=volume_name,
                mountPath=charm.RABBITMQ_DATA_DIR,
            )
        ],
    )
    volume = SimpleNamespace(
        name=volume_name,
        persistentVolumeClaim=SimpleNamespace(claimName="pvc-rabbitmq-data"),
    )
    pod = SimpleNamespace(
        spec=SimpleNamespace(containers=[container], volumes=[volume])
    )
    request = httpx.Request("GET", "https://kubernetes.invalid")
    response = httpx.Response(
        404,
        request=request,
        json={
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": "not found",
            "reason": "NotFound",
            "code": 404,
        },
    )
    fake = _fake_charm(
        unit=SimpleNamespace(name="rabbitmq-k8s/0"),
        model=SimpleNamespace(name="test-model"),
        lightkube_client=SimpleNamespace(
            get=Mock(
                side_effect=[
                    pod,
                    charm.ApiError(request=request, response=response),
                ]
            )
        ),
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes(fake)


def test_rabbitmq_data_pvc_capacity_bytes_missing_mount_raises():
    """Missing broker data mounts are surfaced as charm errors."""
    container = SimpleNamespace(
        name=charm.RABBITMQ_CONTAINER,
        volumeMounts=[],
    )
    pod = SimpleNamespace(
        spec=SimpleNamespace(containers=[container], volumes=[])
    )
    fake = _fake_charm(
        unit=SimpleNamespace(name="rabbitmq-k8s/0"),
        model=SimpleNamespace(name="test-model"),
        lightkube_client=SimpleNamespace(get=Mock(return_value=pod)),
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes(fake)


def test_resolved_disk_free_limit_bytes_auto_uses_ten_percent_of_pvc():
    """The auto setting resolves to 10 percent of the PVC capacity."""
    fake = _fake_charm(
        config={
            "cluster-partition-handling": "pause_minority",
            "disk-free-limit-bytes": "auto",
            "protect-members": True,
            "minimum-replicas": 3,
        },
        _rabbitmq_data_pvc_capacity_bytes=Mock(return_value=1024**3),
    )

    assert (
        charm.RabbitMQOperatorCharm.resolved_disk_free_limit_bytes.fget(fake)
        == 107374182
    )


def test_resolved_disk_free_limit_bytes_explicit_value_is_parsed():
    """Explicit human-readable values are resolved to bytes."""
    fake = _fake_charm(
        config={
            "cluster-partition-handling": "pause_minority",
            "disk-free-limit-bytes": "5Gi",
            "protect-members": True,
            "minimum-replicas": 3,
        },
        _rabbitmq_data_pvc_capacity_bytes=Mock(return_value=10 * 1024**3),
    )

    assert (
        charm.RabbitMQOperatorCharm.resolved_disk_free_limit_bytes.fget(fake)
        == 5 * 1024**3
    )


def test_resolved_disk_free_limit_bytes_rejects_values_larger_than_pvc():
    """Explicit limits larger than the PVC capacity are rejected."""
    fake = _fake_charm(
        config={
            "cluster-partition-handling": "pause_minority",
            "disk-free-limit-bytes": "2Gi",
            "protect-members": True,
            "minimum-replicas": 3,
        },
        _rabbitmq_data_pvc_capacity_bytes=Mock(return_value=1024**3),
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm.resolved_disk_free_limit_bytes.fget(fake)


def test_render_safety_check_checks_listener_and_honours_protection_flag():
    """The safety script checks AMQP listeners and only enforces fail-closed logic when enabled."""
    fake = _fake_charm()

    script = charm.RabbitMQOperatorCharm._render_template(
        fake,
        "rabbitmq-safety-check.sh.j2",
        safety_reason_not_running=charm.SAFETY_REASON_NOT_RUNNING,
        safety_reason_local_alarms=charm.SAFETY_REASON_LOCAL_ALARMS,
        safety_reason_cluster_status=charm.SAFETY_REASON_CLUSTER_STATUS,
        protect_members="true",
        amqp_port=charm.RABBITMQ_SERVICE_PORT,
    )

    assert (
        "listeners 2>/dev/null | grep -Eq '(^|[^0-9])port: 5672([^0-9]|$)'"
        in script
    )
    assert 'if [ "true" = "true" ]; then' in script


def test_evaluate_broker_safety_detects_local_alarms():
    """Safety evaluation is unsafe when local alarms are active."""
    container = Mock()
    container.exec.side_effect = [
        ops.pebble.ExecError(
            ["rabbitmq-diagnostics", "check_local_alarms"],
            1,
            "",
            "memory alarm",
        ),
    ]
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )

    safe, reason = charm.RabbitMQOperatorCharm._evaluate_broker_safety(fake)

    assert not safe
    assert reason == charm.SAFETY_REASON_LOCAL_ALARMS


def test_evaluate_broker_safety_detects_cluster_status_failure():
    """Safety evaluation is unsafe when cluster status cannot be fetched."""
    container = Mock()
    container.exec.side_effect = [
        Mock(wait_output=Mock(return_value=("", ""))),
        Mock(wait_output=Mock(return_value=("", ""))),
        ops.pebble.ExecError(
            ["rabbitmq-diagnostics", "cluster_status"],
            1,
            "",
            "timeout",
        ),
    ]
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )

    safe, reason = charm.RabbitMQOperatorCharm._evaluate_broker_safety(fake)

    assert not safe
    assert reason == charm.SAFETY_REASON_CLUSTER_STATUS


def test_evaluate_broker_safety_detects_loss_of_majority():
    """Safety evaluation is unsafe when the cluster loses majority."""
    cluster_status = {
        "disk_nodes": ["n1", "n2", "n3"],
        "running_nodes": ["n1"],
    }
    container = Mock()
    container.exec.side_effect = [
        Mock(wait_output=Mock(return_value=("", ""))),
        Mock(wait_output=Mock(return_value=(json.dumps(cluster_status), ""))),
    ]
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _rabbitmq_running=Mock(return_value=True),
    )

    safe, reason = charm.RabbitMQOperatorCharm._evaluate_broker_safety(fake)

    assert not safe
    assert reason == "Cluster lost majority (1/3 running disk nodes)"


def test_reconcile_listener_protection_suspends_on_unsafe_transition():
    """Unsafe units suspend listeners and create the charm marker."""
    container = Mock()
    container.exists.return_value = False
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
    fake._suspend_listeners = lambda current: charm.RabbitMQOperatorCharm._suspend_listeners(  # noqa: E501
        fake, current
    )

    charm.RabbitMQOperatorCharm._reconcile_listener_protection(fake, False)

    container.exec.assert_called_once_with(
        ["rabbitmqctl", "suspend_listeners"], timeout=30
    )
    container.push.assert_called_once()


def test_reconcile_listener_protection_resumes_only_with_charm_marker():
    """Safe units only auto-resume listeners when the charm marker exists."""
    container = Mock()
    container.exists.return_value = False
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
    fake._resume_listeners = lambda current, context: charm.RabbitMQOperatorCharm._resume_listeners(  # noqa: E501
        fake, current, context
    )

    charm.RabbitMQOperatorCharm._reconcile_listener_protection(fake, True)

    container.exec.assert_not_called()

    container.reset_mock()
    container.exists.return_value = True
    charm.RabbitMQOperatorCharm._reconcile_listener_protection(fake, True)

    container.exec.assert_called_once_with(
        ["rabbitmqctl", "resume_listeners"], timeout=30
    )
    container.remove_path.assert_called_once_with(
        charm.RABBITMQ_PROTECTOR_MARKER
    )


def test_reconcile_listener_protection_skips_suspension_when_disabled():
    """Protection disablement should not suspend listeners on unsafe states."""
    container = Mock()
    container.exists.return_value = False
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        config={
            "cluster-partition-handling": "pause_minority",
            "disk-free-limit-bytes": 536870912,
            "protect-members": False,
            "minimum-replicas": 3,
        },
        protect_members=False,
    )

    charm.RabbitMQOperatorCharm._reconcile_listener_protection(fake, False)

    container.exec.assert_not_called()


def test_initialize_operator_user_creates_and_removes_guest():
    """The operator bootstrap creates the admin user and removes guest."""
    admin_api = Mock()
    peers = SimpleNamespace(set_operator_user_created=Mock())
    fake = _fake_charm(
        peers=peers,
        _get_admin_api=Mock(return_value=admin_api),
    )
    fake._operator_user = "operator"
    fake._operator_password = "operator-password"

    charm.RabbitMQOperatorCharm._initialize_operator_user(fake)

    admin_api.create_user.assert_called_once_with(
        "operator",
        "operator-password",
        tags=["administrator"],
    )
    admin_api.create_user_permission.assert_called_once_with(
        "operator", vhost="/"
    )
    peers.set_operator_user_created.assert_called_once_with("operator")
    admin_api.delete_user.assert_called_once_with("guest")


def test_operator_user_auth_valid_detects_stale_credentials():
    """Stale operator credentials are detected with rabbitmqctl."""
    fake = _fake_charm(
        _rabbitmq_running=Mock(return_value=True),
        _run_rabbitmqctl=Mock(
            side_effect=ops.pebble.ExecError(
                ["rabbitmqctl", "authenticate_user"],
                65,
                "",
                "invalid credentials",
            )
        ),
    )

    assert not charm.RabbitMQOperatorCharm._operator_user_auth_valid(fake)


def test_operator_user_recovery_required_leader_only():
    """Only the leader should report operator-user recovery drift."""
    fake = _fake_charm(
        unit=SimpleNamespace(is_leader=lambda: True),
        peers=SimpleNamespace(
            operator_user_created="operator",
            operator_password="operator-password",
            set_operator_user_created=Mock(),
        ),
        _operator_user_auth_valid=Mock(return_value=False),
    )

    assert charm.RabbitMQOperatorCharm._operator_user_recovery_required(fake)

    follower = _fake_charm(
        unit=SimpleNamespace(is_leader=lambda: False),
        peers=SimpleNamespace(
            operator_user_created="operator",
            operator_password="operator-password",
            set_operator_user_created=Mock(),
        ),
        _operator_user_auth_valid=Mock(return_value=False),
    )

    assert not charm.RabbitMQOperatorCharm._operator_user_recovery_required(
        follower
    )


def test_recreate_operator_user_changes_password_when_user_exists():
    """Recovery reuses the stored password when the user already exists."""
    error = ops.pebble.ExecError(
        ["rabbitmqctl", "add_user"],
        70,
        "",
        "user_already_exists: operator already exists",
    )
    peers = SimpleNamespace(set_operator_user_created=Mock())
    fake = _fake_charm(
        peers=peers,
        _run_rabbitmqctl=Mock(
            side_effect=[
                error,
                ("", ""),
                ("", ""),
                ("", ""),
                ("", ""),
            ]
        ),
    )

    charm.RabbitMQOperatorCharm._recreate_operator_user(fake)

    fake._run_rabbitmqctl.assert_has_calls(
        [
            call("add_user", "operator", "operator-password"),
            call("change_password", "operator", "operator-password"),
            call("set_user_tags", "operator", "administrator"),
            call(
                "set_permissions",
                "-p",
                "/",
                "operator",
                ".*",
                ".*",
                ".*",
            ),
            call("delete_user", "guest"),
        ]
    )
    peers.set_operator_user_created.assert_called_once_with("operator")


def test_reconcile_queue_membership_skips_when_operator_user_recovery_required():
    """Queue reconciliation should not crash while operator-user recovery is needed."""
    fake = _fake_charm(
        unit=SimpleNamespace(is_leader=lambda: True),
        peers=SimpleNamespace(
            operator_user_created="operator",
            operator_password="operator-password",
            set_operator_user_created=Mock(),
        ),
        _rabbitmq_running=Mock(return_value=True),
        _operator_user_recovery_required=Mock(return_value=True),
        ensure_queue_ha=Mock(),
    )

    assert charm.RabbitMQOperatorCharm._reconcile_queue_membership(fake)
    fake.ensure_queue_ha.assert_not_called()


def test_recreate_operator_user_action_blocks_when_not_leader():
    """The recovery action is leader-only."""
    event = Mock()
    fake = _fake_charm(unit=SimpleNamespace(is_leader=lambda: False))

    charm.RabbitMQOperatorCharm._on_recreate_operator_user_action(fake, event)

    event.fail.assert_called_once_with(
        "Not leader unit, unable to recreate operator user"
    )


def test_recreate_operator_user_action_succeeds():
    """The recovery action recreates the stored operator user."""
    event = Mock()
    fake = _fake_charm(
        unit=SimpleNamespace(is_leader=lambda: True),
        _rabbitmq_running=Mock(return_value=True),
        _recreate_operator_user=Mock(),
    )

    charm.RabbitMQOperatorCharm._on_recreate_operator_user_action(fake, event)

    fake._recreate_operator_user.assert_called_once_with()
    event.set_results.assert_called_once_with({"operator-user": "operator"})


def test_create_amqp_credentials_attempts_recovery_before_deferring():
    """AMQP credential creation should try startup recovery first."""
    app = object()
    relation = SimpleNamespace(data={app: {}})
    event = SimpleNamespace(relation=relation, defer=Mock())

    def recover(current_event):
        current_event.defer()
        return False

    recover = Mock(side_effect=recover)
    fake = _fake_charm(app=app, _ensure_broker_running=recover)

    charm.RabbitMQOperatorCharm.create_amqp_credentials(
        fake, event, "svc-user", "svc-vhost", False
    )

    recover.assert_called_once_with(event)
    event.defer.assert_called_once_with()


def test_create_amqp_credentials_fast_exit_skips_recovery():
    """Existing AMQP credentials should not trigger startup recovery."""
    app = object()
    relation = SimpleNamespace(data={app: {"password": "stored-password"}})
    event = SimpleNamespace(relation=relation, defer=Mock())
    fake = _fake_charm(app=app)

    charm.RabbitMQOperatorCharm.create_amqp_credentials(
        fake, event, "svc-user", "svc-vhost", False
    )

    fake._ensure_broker_running.assert_not_called()
    event.defer.assert_not_called()


def test_create_amqp_credentials_success():
    """AMQP credentials are created and published onto the relation."""
    app = object()
    relation = SimpleNamespace(data={app: {}})
    event = SimpleNamespace(relation=relation, defer=Mock())
    fake = _fake_charm(app=app)

    charm.RabbitMQOperatorCharm.create_amqp_credentials(
        fake, event, "svc-user", "svc-vhost", True
    )

    fake._ensure_broker_running.assert_called_once_with(event)
    fake._ensure_relation_credentials.assert_called_once_with(
        relation, "svc-user", "svc-vhost", True
    )


def test_create_amqp_credentials_defers_on_http_401():
    """Unauthorized AMQP credential creation is deferred for later retry."""
    app = object()
    relation = SimpleNamespace(data={app: {}})
    event = SimpleNamespace(relation=relation, defer=Mock())
    error = requests.exceptions.HTTPError("unauthorized")
    error.response = SimpleNamespace(status_code=401)
    fake = _fake_charm(
        app=app,
        _ensure_relation_credentials=Mock(side_effect=error),
    )

    charm.RabbitMQOperatorCharm.create_amqp_credentials(
        fake, event, "svc-user", "svc-vhost", False
    )

    fake._ensure_broker_running.assert_called_once_with(event)
    event.defer.assert_called_once_with()


def test_get_service_account_fails_when_rabbit_unavailable():
    """Service-account action fails if RabbitMQ and peer binding are both missing."""
    event = Mock()
    event.params = {"username": "svc-user", "vhost": "svc-vhost"}
    fake = _fake_charm(
        peers_bind_address=None,
        peers=SimpleNamespace(retrieve_password=Mock()),
    )
    fake.rabbit_running = False

    charm.RabbitMQOperatorCharm._get_service_account(fake, event)

    event.fail.assert_called_once_with(
        "RabbitMQ not running, unable to create account"
    )


def test_get_service_account_fails_on_non_leader():
    """Service-account action is leader-only."""
    event = Mock()
    event.params = {"username": "svc-user", "vhost": "svc-vhost"}
    fake = _fake_charm()
    fake.unit.is_leader.return_value = False

    charm.RabbitMQOperatorCharm._get_service_account(fake, event)

    event.fail.assert_called_once_with(
        "Not leader unit, unable to create service account"
    )


def test_get_service_account_success():
    """Service-account action returns connection details on success."""
    event = Mock()
    event.params = {"username": "svc-user", "vhost": "svc-vhost"}
    peers = SimpleNamespace(
        store_password=Mock(),
        retrieve_password=Mock(return_value="svc-password"),
    )
    fake = _fake_charm(peers=peers)
    fake.unit.is_leader.return_value = True
    fake.rabbit_running = True
    fake.ingress_address = "10.5.0.1"
    fake.rabbitmq_url = Mock(return_value="rabbit://svc-user:svc-password")
    fake.does_vhost_exist.return_value = False
    fake.does_user_exist.return_value = False

    charm.RabbitMQOperatorCharm._get_service_account(fake, event)

    event.set_results.assert_called_once_with(
        {
            "username": "svc-user",
            "password": "svc-password",
            "vhost": "svc-vhost",
            "ingress-address": "10.5.0.1",
            "port": 5672,
            "url": "rabbit://svc-user:svc-password",
        }
    )


def test_ensure_queue_ha_raises_without_enough_nodes():
    """Queue HA fails when the cluster is too small for the configured replicas."""
    admin_api = Mock()
    admin_api.list_nodes.return_value = [{"name": "node1"}, {"name": "node2"}]
    fake = _fake_charm(
        get_undersized_queues=Mock(return_value=[{"name": "q1"}]),
        _get_admin_api=Mock(return_value=admin_api),
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm.ensure_queue_ha(fake)


def test_ensure_queue_ha_rebalances_when_replicated():
    """Queue HA rebalances once replicas were added."""
    admin_api = Mock()
    admin_api.list_nodes.return_value = [
        {"name": "node1"},
        {"name": "node2"},
        {"name": "node3"},
    ]
    undersized_queues = [{"name": "q1", "members": ["node1"], "vhost": "/"}]
    fake = _fake_charm(
        get_undersized_queues=Mock(return_value=undersized_queues),
        _get_admin_api=Mock(return_value=admin_api),
        _add_members_to_undersized_queues=Mock(return_value=["q1"]),
    )

    result = charm.RabbitMQOperatorCharm.ensure_queue_ha(fake)

    assert result == {"undersized-queues": 1, "replicated-queues": 1}
    admin_api.rebalance_queues.assert_called_once_with()


@pytest.mark.parametrize(
    (
        "leader",
        "rabbit_running",
        "operator_user_created",
        "manage_queues",
        "message",
    ),
    [
        (
            False,
            True,
            "rmqadmin",
            True,
            "Not leader unit, unable to ensure queue HA",
        ),
        (
            True,
            False,
            "rmqadmin",
            True,
            "RabbitMQ not running, unable to ensure queue HA",
        ),
        (
            True,
            True,
            None,
            True,
            "Operator user not created, unable to ensure queue HA",
        ),
        (
            True,
            True,
            "rmqadmin",
            False,
            "Queue management is disabled, unable to ensure queue HA",
        ),
    ],
)
def test_ensure_queue_ha_action_gate_failures(
    leader, rabbit_running, operator_user_created, manage_queues, message
):
    """Queue HA action fails early for each gating condition."""
    event = Mock()
    event.params = {"dry-run": False}
    unit = Mock()
    unit.is_leader.return_value = leader
    fake = _fake_charm(
        unit=unit,
        peers=SimpleNamespace(operator_user_created=operator_user_created),
        _manage_queues=Mock(return_value=manage_queues),
    )
    fake.rabbit_running = rabbit_running

    charm.RabbitMQOperatorCharm._ensure_queue_ha_action(fake, event)

    event.fail.assert_called_once_with(message)


def test_ensure_queue_ha_action_success():
    """Queue HA action returns the replication summary and updates status."""
    event = Mock()
    event.params = {"dry-run": True}
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        unit=unit,
        peers=SimpleNamespace(operator_user_created="rmqadmin"),
        _manage_queues=Mock(return_value=True),
        ensure_queue_ha=Mock(
            return_value={
                "undersized-queues": 2,
                "replicated-queues": 1,
            }
        ),
        _on_update_status=Mock(),
    )
    fake.rabbit_running = True

    charm.RabbitMQOperatorCharm._ensure_queue_ha_action(fake, event)

    event.set_results.assert_called_once_with(
        {
            "undersized-queues": 2,
            "replicated-queues": 1,
            "dry-run": True,
        }
    )
    fake._on_update_status.assert_called_once_with(event)


def test_get_hostname_prefers_loadbalancer_when_requested():
    """External connectivity prefers the load-balancer IP when available."""
    fake = _fake_charm(
        hostname="rabbitmq-k8s-endpoints.model.svc.cluster.local",
        _get_loadbalancer_ip=Mock(return_value="203.0.113.10"),
    )

    hostname = charm.RabbitMQOperatorCharm.get_hostname(
        fake, external_connectivity=True
    )

    assert hostname == "203.0.113.10"


def test_get_loadbalancer_ip_returns_none_on_apierror():
    """Load-balancer lookup failures are handled gracefully."""

    class _HashableCharm(SimpleNamespace):
        __hash__ = object.__hash__

    request = httpx.Request("GET", "https://kubernetes.invalid")
    response = httpx.Response(
        404,
        request=request,
        json={
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": "not found",
            "reason": "NotFound",
            "code": 404,
        },
    )
    fake = _HashableCharm(
        **_fake_charm(
            lightkube_client=SimpleNamespace(
                get=Mock(
                    side_effect=charm.ApiError(
                        request=request, response=response
                    )
                )
            ),
            model=SimpleNamespace(name="test-model"),
            _lb_name="rabbitmq-k8s-lb",
        ).__dict__
    )

    result = charm.RabbitMQOperatorCharm._get_loadbalancer_ip(fake)

    assert result is None


def test_reconcile_lb_sets_blocked_on_invalid_annotations():
    """Invalid load-balancer annotations reconcile an empty desired state."""
    unit = Mock()
    unit.is_leader.return_value = True
    manager = SimpleNamespace(reconcile=Mock())
    fake = _fake_charm(
        unit=unit,
        _annotations_valid=False,
        _get_lb_resource_manager=Mock(return_value=manager),
    )

    charm.RabbitMQOperatorCharm._reconcile_lb(fake, None)

    manager.reconcile.assert_called_once_with([])


def test_reconcile_lb_reconciles_valid_service():
    """Valid annotations produce a load-balancer reconciliation request."""
    unit = Mock()
    unit.is_leader.return_value = True
    resource_manager = SimpleNamespace(reconcile=Mock())
    service = Mock()
    fake = _fake_charm(
        unit=unit,
        _annotations_valid=True,
        _construct_lb=Mock(return_value=service),
        _get_lb_resource_manager=Mock(return_value=resource_manager),
        _lb_name="rabbitmq-k8s-lb",
    )

    charm.RabbitMQOperatorCharm._reconcile_lb(fake, None)

    resource_manager.reconcile.assert_called_once_with([service])
