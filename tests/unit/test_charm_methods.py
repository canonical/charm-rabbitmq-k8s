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
        "resolved_wal_max_size_bytes": 483183821,
        "_rabbitmq_data_pvc_capacity_bytes": 1073741824,
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
        "_local_node_in_running_cluster": Mock(return_value=True),
        "_health_checks_ready": Mock(return_value=True),
        "_reconcile_workload": Mock(return_value=True),
        "_reconcile_running_broker_state": Mock(),
        "_reconcile_health_checks": Mock(),
        "_reconcile": Mock(),
        "_ensure_broker_running": Mock(return_value=True),
        "_auto_forget_stale_cluster_nodes": Mock(return_value=True),
        "_auto_forget_stale_cluster_nodes_ready": Mock(return_value=False),
        "_cluster_status": Mock(return_value={}),
        "_forget_cluster_node_if_present": Mock(return_value=True),
        "_scale_down_force_boot_allowed": Mock(return_value=False),
        "_force_boot_single_survivor": Mock(return_value=False),
        "_force_boot_single_survivor_after_scale_down": Mock(
            return_value=False
        ),
        "_defer_or_continue": Mock(return_value=True),
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
        ("key1=value1,key=val=ue2", {"key1": "value1", "key": "val=ue2"}),
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
    container.exec.return_value.wait_output.assert_called_once()


def test_ensure_ownership_exec_error():
    """Ownership changes fail gracefully when chown raises ExecError."""
    container = Mock()
    file_info = Mock(user="root", group="root")
    container.list_files.return_value = [file_info]
    process = Mock()
    process.wait_output.side_effect = ops.pebble.ExecError(
        ["chown"], 1, "stdout", "Permission denied"
    )
    container.exec.return_value = process

    result = charm.RabbitMQOperatorCharm._ensure_ownership(
        SimpleNamespace(), container, "/test/path", "rabbitmq", "rabbitmq"
    )

    assert result is False


def test_set_ownership_on_data_dir_not_mounted():
    """Data-dir ownership waits until the storage mount exists."""
    container = Mock()
    process = Mock()
    process.wait_output.side_effect = ops.pebble.ExecError(
        ["mountpoint"], 1, "", "not a mountpoint"
    )
    container.exec.return_value = process
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

    with patch(
        "charm.secrets.token_urlsafe", return_value="generated-password"
    ):
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

    with patch(
        "charm.secrets.token_urlsafe", return_value="generated-password"
    ):
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
        _operator_user_recovery_required=Mock(return_value=False),
        _refresh_rabbitmq_version=Mock(),
        _ensure_cluster_name=Mock(),
        _render_and_push_safety_check=Mock(),
    )

    charm.RabbitMQOperatorCharm._reconcile_running_broker_state(fake)

    fake._refresh_rabbitmq_version.assert_called_once_with()
    fake._ensure_cluster_name.assert_called_once_with()
    fake._render_and_push_safety_check.assert_called_once_with()


def test_reconcile_operator_user_recovers_missing_peer_flag():
    """Leader should republish operator-user-created if auth already works."""
    peers = SimpleNamespace(
        operator_user_created=None,
        set_operator_user_created=Mock(),
    )
    fake = _fake_charm(
        unit=SimpleNamespace(is_leader=lambda: True),
        peers=peers,
        _rabbitmq_running=Mock(return_value=True),
        _recover_operator_user_peer_flag=Mock(return_value=True),
        _initialize_operator_user=Mock(),
    )

    assert charm.RabbitMQOperatorCharm._reconcile_operator_user(fake)

    peers.set_operator_user_created.assert_called_once_with("operator")
    fake._initialize_operator_user.assert_not_called()


def test_forget_cluster_node_action_removes_absent_non_running_members():
    """Action should forget old disk nodes no longer in Juju peer data."""
    cluster_status = {
        "disk_nodes": [
            "rabbit@rabbitmq-k8s-0.rabbitmq-k8s-endpoints",
            "rabbit@rabbitmq-k8s-1.rabbitmq-k8s-endpoints",
            "rabbit@rabbitmq-k8s-2.rabbitmq-k8s-endpoints",
        ],
        "running_nodes": ["rabbit@rabbitmq-k8s-0.rabbitmq-k8s-endpoints"],
    }
    container = Mock()
    container.exec.side_effect = [
        Mock(wait_output=Mock(return_value=(json.dumps(cluster_status), ""))),
        Mock(wait_output=Mock(return_value=("", ""))),
        Mock(wait_output=Mock(return_value=("", ""))),
    ]
    event = Mock()
    fake = _fake_charm(
        _require_queue_management_ready=Mock(return_value=True),
        unit=SimpleNamespace(
            is_leader=lambda: True,
            name="rabbitmq-k8s/0",
            get_container=Mock(return_value=container),
        ),
        peers=SimpleNamespace(peers_rel=SimpleNamespace(units=[])),
        app=SimpleNamespace(
            name="rabbitmq-k8s",
            units=[SimpleNamespace(name="rabbitmq-k8s/0")],
        ),
        generate_nodename=Mock(
            side_effect=lambda unit: (
                f"rabbit@{unit.replace('/', '-')}.rabbitmq-k8s-endpoints"
            )
        ),
    )

    charm.RabbitMQOperatorCharm._on_forget_cluster_node_action(fake, event)

    container.exec.assert_has_calls(
        [
            call(
                ["rabbitmqctl", "cluster_status", "--formatter=json"],
                timeout=30,
            ),
            call(
                [
                    "rabbitmqctl",
                    "forget_cluster_node",
                    "rabbit@rabbitmq-k8s-1.rabbitmq-k8s-endpoints",
                ],
                timeout=5 * 60,
            ),
            call(
                [
                    "rabbitmqctl",
                    "forget_cluster_node",
                    "rabbit@rabbitmq-k8s-2.rabbitmq-k8s-endpoints",
                ],
                timeout=5 * 60,
            ),
        ]
    )
    event.set_results.assert_called_once()


def test_forget_cluster_node_action_fails_when_not_ready():
    """Action should fail when shared readiness guard fails."""
    event = Mock()
    fake = _fake_charm(
        _require_queue_management_ready=Mock(return_value=False),
    )

    charm.RabbitMQOperatorCharm._on_forget_cluster_node_action(fake, event)

    fake._require_queue_management_ready.assert_called_once_with(event)
    event.set_results.assert_not_called()


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
        SimpleNamespace(peers=SimpleNamespace(operator_user_created=None))
    )
    container = Mock()
    container.get_plan.return_value.to_dict.return_value = {
        "services": desired_layer["services"],
        "checks": desired_layer.get("checks", {}),
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
        SimpleNamespace(peers=SimpleNamespace(operator_user_created=None))
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


def test_rabbitmq_layer_includes_checks_before_operator_bootstrap():
    """Health checks are always in the layer (gated by start/stop at runtime)."""
    layer = charm.RabbitMQOperatorCharm._rabbitmq_layer(
        SimpleNamespace(peers=SimpleNamespace(operator_user_created=None))
    )

    assert "checks" in layer
    assert "alive" in layer["checks"]
    assert "ready" in layer["checks"]


def test_rabbitmq_layer_adds_checks_after_operator_bootstrap():
    """Health checks should be added once operator bootstrap completed."""
    layer = charm.RabbitMQOperatorCharm._rabbitmq_layer(
        SimpleNamespace(
            peers=SimpleNamespace(operator_user_created="operator")
        )
    )

    assert layer["checks"] == {
        "alive": {
            "override": "replace",
            "level": "alive",
            "startup": "disabled",
            "period": charm.HEALTH_CHECK_INTERVAL,
            "timeout": charm.ALIVE_CHECK_TIMEOUT,
            "threshold": charm.HEALTH_CHECK_THRESHOLD,
            "exec": {"command": charm.RABBITMQ_ALIVE_CHECK_PATH},
        },
        "ready": {
            "override": "replace",
            "level": "ready",
            "startup": "disabled",
            "period": charm.HEALTH_CHECK_INTERVAL,
            "timeout": charm.READY_CHECK_TIMEOUT,
            "threshold": charm.HEALTH_CHECK_THRESHOLD,
            "exec": {"command": charm.RABBITMQ_SAFETY_CHECK_PATH},
        },
    }


def test_render_and_push_workload_scripts_only_restarts_notifier_for_notifier_changes():
    """Only notifier script updates should mark the notifier service dirty."""
    fake = _fake_charm(
        _render_and_push_alive_check=Mock(return_value=True),
        _render_and_push_pebble_notifier=Mock(return_value=False),
        _render_and_push_safety_check=Mock(return_value=True),
    )

    changed_services = (
        charm.RabbitMQOperatorCharm._render_and_push_workload_scripts(fake)
    )

    assert changed_services == set()

    fake._render_and_push_alive_check.assert_called_once_with()
    fake._render_and_push_pebble_notifier.assert_called_once_with()
    fake._render_and_push_safety_check.assert_called_once_with()


def test_render_and_push_workload_scripts_marks_notifier_when_notifier_changes():
    """Notifier script drift should be surfaced as a notifier restart."""
    fake = _fake_charm(
        _render_and_push_alive_check=Mock(return_value=False),
        _render_and_push_pebble_notifier=Mock(return_value=True),
        _render_and_push_safety_check=Mock(return_value=False),
    )

    changed_services = (
        charm.RabbitMQOperatorCharm._render_and_push_workload_scripts(fake)
    )

    assert changed_services == {charm.NOTIFIER_SERVICE}


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
    assert (
        "cluster_formation.k8s.host = kubernetes.default.svc" in rendered_conf
    )


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
        wal_max_size_bytes=fake.resolved_wal_max_size_bytes,
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


def _make_container(can_connect=True, statvfs_total=1073741824):
    """Build a mock container for _rabbitmq_data_pvc_capacity_bytes tests."""
    process = Mock()
    process.wait_output.return_value = (
        json.dumps({"total": statvfs_total}),
        "",
    )
    container = Mock()
    container.can_connect.return_value = can_connect
    container.exec.return_value = process
    return container


def test_rabbitmq_data_pvc_capacity_bytes():
    """Storage capacity is resolved via statvfs inside the container."""
    container = _make_container(statvfs_total=1073741824)
    unit = Mock()
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit)

    resolved = (
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes.func(
            fake
        )
    )

    assert resolved == 1073741824
    container.exec.assert_called_once()


def test_rabbitmq_data_pvc_capacity_bytes_container_not_ready():
    """An unavailable container raises a clear transient error."""
    container = _make_container(can_connect=False)
    unit = Mock()
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit)

    with pytest.raises(
        charm.RabbitOperatorError,
        match="container not yet available",
    ):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes.func(
            fake
        )


def test_rabbitmq_data_pvc_capacity_bytes_exec_error_raises():
    """An exec failure inside the container raises a charm error."""
    container = Mock()
    container.can_connect.return_value = True
    container.exec.return_value.wait_output.side_effect = charm.ExecError(
        ["python3"], 1, "", "stat failed"
    )
    unit = Mock()
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit)

    with pytest.raises(
        charm.RabbitOperatorError,
        match="Failed to stat",
    ):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes.func(
            fake
        )


def test_rabbitmq_data_pvc_capacity_bytes_bad_output_raises():
    """Unexpected statvfs output raises a charm error."""
    process = Mock()
    process.wait_output.return_value = ("not-json", "")
    container = Mock()
    container.can_connect.return_value = True
    container.exec.return_value = process
    unit = Mock()
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit)

    with pytest.raises(
        charm.RabbitOperatorError,
        match="Unexpected statvfs output",
    ):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes.func(
            fake
        )


def test_rabbitmq_data_pvc_capacity_bytes_zero_capacity_raises():
    """A filesystem reporting zero capacity raises a charm error."""
    container = _make_container(statvfs_total=0)
    unit = Mock()
    unit.get_container.return_value = container
    fake = _fake_charm(unit=unit)

    with pytest.raises(
        charm.RabbitOperatorError,
        match="reports zero capacity",
    ):
        charm.RabbitMQOperatorCharm._rabbitmq_data_pvc_capacity_bytes.func(
            fake
        )


def test_resolved_disk_free_limit_bytes_auto_uses_ten_percent_of_pvc():
    """The auto setting resolves to 10 percent of the PVC capacity."""
    fake = _fake_charm(
        config={
            "cluster-partition-handling": "pause_minority",
            "disk-free-limit-bytes": "auto",
            "protect-members": True,
            "minimum-replicas": 3,
        },
        _rabbitmq_data_pvc_capacity_bytes=1024**3,
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
        _rabbitmq_data_pvc_capacity_bytes=10 * 1024**3,
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
        _rabbitmq_data_pvc_capacity_bytes=1024**3,
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm.resolved_disk_free_limit_bytes.fget(fake)


def test_resolved_wal_max_size_bytes_large_pvc_is_capped():
    """A 4Gi PVC produces a WAL size capped at 512Mi."""
    fake = _fake_charm(
        _rabbitmq_data_pvc_capacity_bytes=4 * 1024**3,
        resolved_disk_free_limit_bytes=429496729,
    )

    result = charm.RabbitMQOperatorCharm.resolved_wal_max_size_bytes.fget(fake)

    assert result == 536870912  # 512 MiB


def test_resolved_wal_max_size_bytes_small_pvc_uncapped():
    """A 1Gi PVC produces an uncapped WAL size of (pvc - disk_free) // 2."""
    fake = _fake_charm(
        _rabbitmq_data_pvc_capacity_bytes=1073741824,
        resolved_disk_free_limit_bytes=107374182,
    )

    result = charm.RabbitMQOperatorCharm.resolved_wal_max_size_bytes.fget(fake)

    assert result == 483183821  # (1073741824 - 107374182) // 2


def test_resolved_wal_max_size_bytes_tiny_pvc_raises():
    """A PVC where capacity equals the disk free limit raises RabbitOperatorError."""
    pvc_capacity = 1073741824
    fake = _fake_charm(
        _rabbitmq_data_pvc_capacity_bytes=pvc_capacity,
        resolved_disk_free_limit_bytes=pvc_capacity,
    )

    with pytest.raises(charm.RabbitOperatorError):
        charm.RabbitMQOperatorCharm.resolved_wal_max_size_bytes.fget(fake)


def test_configuration_error_returns_wal_size_resolution_message():
    """Status collection should block when resolved_wal_max_size_bytes raises."""

    class FakeCharm(SimpleNamespace):
        @property
        def cluster_partition_handling(self):
            return "pause_minority"

        @property
        def resolved_disk_free_limit_bytes(self):
            return 107374182

        @property
        def resolved_wal_max_size_bytes(self):
            raise charm.RabbitOperatorError(
                "PVC capacity too small for a safe Raft WAL size"
            )

    fake = FakeCharm(
        unit=SimpleNamespace(is_leader=lambda: False),
        _annotations_valid=True,
    )

    assert (
        charm.RabbitMQOperatorCharm._configuration_error(fake)
        == "PVC capacity too small for a safe Raft WAL size"
    )


def test_configuration_error_returns_disk_limit_resolution_message():
    """Status collection should block on disk-limit resolution errors, not crash."""

    class FakeCharm(SimpleNamespace):
        @property
        def cluster_partition_handling(self):
            return "pause_minority"

        @property
        def resolved_disk_free_limit_bytes(self):
            raise charm.RabbitOperatorError(
                "Failed to configure Kubernetes client for rabbitmq-data PVC lookup"
            )

        @property
        def resolved_wal_max_size_bytes(self):
            return 536870912

    fake = FakeCharm(
        unit=SimpleNamespace(is_leader=lambda: False),
        _annotations_valid=True,
    )

    assert (
        charm.RabbitMQOperatorCharm._configuration_error(fake)
        == "Failed to configure Kubernetes client for rabbitmq-data PVC lookup"
    )


def test_render_safety_check_checks_listener_and_honours_protection_flag():
    """The safety script checks AMQP listeners and only enforces fail-closed logic when enabled."""
    fake = _fake_charm()

    script = charm.RabbitMQOperatorCharm._render_template(
        fake,
        "rabbitmq-safety-check.sh.j2",
        safety_reason_file=charm.RABBITMQ_SAFETY_REASON_FILE,
        safety_reason_not_running=charm.SAFETY_REASON_NOT_RUNNING,
        safety_reason_local_alarms=charm.SAFETY_REASON_LOCAL_ALARMS,
        safety_reason_cluster_status=charm.SAFETY_REASON_CLUSTER_STATUS,
        protect_members="true",
        amqp_port=charm.RABBITMQ_SERVICE_PORT,
        expected_cluster_size=3,
    )

    assert "REASON_FILE=" in script
    assert (
        "listeners 2>/dev/null | grep -Eq '(^|[^0-9])port: 5672([^0-9]|$)'"
        in script
    )
    assert 'if [ "true" = "true" ]; then' in script


def test_render_alive_check_allows_bounded_startup_grace():
    """The alive script should tolerate broker bootstrap for a bounded period."""
    fake = _fake_charm()

    script = charm.RabbitMQOperatorCharm._render_template(
        fake,
        "rabbitmq-alive-check.sh.j2",
        startup_grace_seconds=charm.RABBITMQ_STARTUP_GRACE_SECONDS,
        safety_reason_not_running=charm.SAFETY_REASON_NOT_RUNNING,
    )

    assert "rabbitmq-diagnostics check_running" in script
    assert "pgrep -u rabbitmq -f 'beam.smp'" in script
    assert (
        f'[ "$beam_age" -lt "{charm.RABBITMQ_STARTUP_GRACE_SECONDS}" ]'
        in script
    )
    assert charm.SAFETY_REASON_NOT_RUNNING in script


def test_render_and_push_alive_check_delegates_to_push_text_file():
    """Alive check pushes through the shared drift-detection helper."""
    container = Mock()
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _push_text_file=Mock(return_value=True),
    )

    changed = charm.RabbitMQOperatorCharm._render_and_push_alive_check(fake)

    assert changed is True
    fake._push_text_file.assert_called_once()
    container.pull.assert_not_called()


def test_render_and_push_safety_check_delegates_to_push_text_file():
    """Safety check pushes through the shared drift-detection helper."""
    container = Mock()
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _push_text_file=Mock(return_value=True),
        _expected_cluster_size=3,
    )

    changed = charm.RabbitMQOperatorCharm._render_and_push_safety_check(fake)

    assert changed is True
    fake._push_text_file.assert_called_once()
    container.pull.assert_not_called()


def test_render_and_push_pebble_notifier_delegates_to_push_text_file():
    """Notifier pushes through the shared drift-detection helper."""
    container = Mock()
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _push_text_file=Mock(return_value=True),
        config={
            "cluster-partition-handling": "pause_minority",
            "disk-free-limit-bytes": "auto",
            "protect-members": True,
            "minimum-replicas": 3,
            "auto-ha-frequency": 5,
        },
    )

    changed = charm.RabbitMQOperatorCharm._render_and_push_pebble_notifier(
        fake
    )

    assert changed is True
    fake._push_text_file.assert_called_once()
    container.pull.assert_not_called()


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
    container.exec.side_effect = [Mock(), Mock()]
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
    fake._resume_listeners = lambda current, context: charm.RabbitMQOperatorCharm._resume_listeners(  # noqa: E501
        fake, current, context
    )

    charm.RabbitMQOperatorCharm._reconcile_listener_protection(fake, True)

    container.exec.assert_not_called()

    container.reset_mock()
    container.exists.return_value = True
    container.exec.side_effect = [Mock(), Mock()]
    charm.RabbitMQOperatorCharm._reconcile_listener_protection(fake, True)

    assert container.exec.call_args_list == [
        call(["rabbitmqctl", "await_startup"], timeout=60),
        call(["rabbitmqctl", "resume_listeners"], timeout=30),
    ]
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


def test_reconcile_queue_membership_defers_on_unauthorized_auth():
    """Queue reconciliation should defer instead of crashing on stale creds."""
    event = Mock(defer=Mock())
    response = Mock(status_code=401)
    error = requests.exceptions.HTTPError(response=response)
    fake = _fake_charm(
        unit=SimpleNamespace(is_leader=lambda: True),
        peers=SimpleNamespace(operator_user_created="operator"),
        _manage_queues=Mock(return_value=True),
        _rabbitmq_running=Mock(return_value=True),
        _operator_user_recovery_required=Mock(return_value=False),
        ensure_queue_ha=Mock(side_effect=error),
    )

    assert not charm.RabbitMQOperatorCharm._reconcile_queue_membership(
        fake, event
    )
    event.defer.assert_called_once_with()


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


def test_get_service_account_fails_when_rabbit_unavailable():
    """Service-account action fails if RabbitMQ is not reachable."""
    event = Mock()
    event.params = {"username": "svc-user", "vhost": "svc-vhost"}
    fake = _fake_charm(
        peers=SimpleNamespace(retrieve_password=Mock()),
    )
    fake._rabbitmq_running = Mock(return_value=False)

    charm.RabbitMQOperatorCharm._get_service_account(fake, event)

    event.fail.assert_called_once_with("RabbitMQ not running")


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
    fake._rabbitmq_running = Mock(return_value=True)
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


def test_ensure_queue_ha_action_fails_when_not_ready():
    """Queue HA action fails when shared readiness guard fails."""
    event = Mock()
    event.params = {"dry-run": False}
    fake = _fake_charm(
        _require_queue_management_ready=Mock(return_value=False),
    )

    charm.RabbitMQOperatorCharm._ensure_queue_ha_action(fake, event)

    fake._require_queue_management_ready.assert_called_once_with(event)
    event.set_results.assert_not_called()


def test_ensure_queue_ha_action_fails_when_manage_queues_disabled():
    """Queue HA action fails when queue management is disabled."""
    event = Mock()
    event.params = {"dry-run": False}
    fake = _fake_charm(
        _require_queue_management_ready=Mock(return_value=True),
        _manage_queues=Mock(return_value=False),
    )

    charm.RabbitMQOperatorCharm._ensure_queue_ha_action(fake, event)

    event.fail.assert_called_once_with(
        "Queue management is disabled, unable to ensure queue HA"
    )


def test_ensure_queue_ha_action_success():
    """Queue HA action returns the replication summary."""
    event = Mock()
    event.params = {"dry-run": True}
    fake = _fake_charm(
        _require_queue_management_ready=Mock(return_value=True),
        _manage_queues=Mock(return_value=True),
        ensure_queue_ha=Mock(
            return_value={
                "undersized-queues": 2,
                "replicated-queues": 1,
            }
        ),
    )

    charm.RabbitMQOperatorCharm._ensure_queue_ha_action(fake, event)

    event.set_results.assert_called_once_with(
        {
            "undersized-queues": 2,
            "replicated-queues": 1,
            "dry-run": True,
        }
    )


@pytest.mark.parametrize(
    (
        "leader",
        "rabbitmq_running",
        "operator_user_created",
        "recovery_required",
        "message",
    ),
    [
        (
            False,
            True,
            "rmqadmin",
            False,
            "Not leader unit",
        ),
        (
            True,
            False,
            "rmqadmin",
            False,
            "RabbitMQ not running",
        ),
        (
            True,
            True,
            None,
            False,
            "Operator user not created",
        ),
        (
            True,
            True,
            "rmqadmin",
            True,
            charm.OPERATOR_USER_RECOVERY_MESSAGE,
        ),
    ],
)
def test_require_queue_management_ready_failures(
    leader, rabbitmq_running, operator_user_created, recovery_required, message
):
    """Each precondition failure calls event.fail and returns False."""
    event = Mock()
    unit = Mock()
    unit.is_leader.return_value = leader
    fake = _fake_charm(
        unit=unit,
        peers=SimpleNamespace(operator_user_created=operator_user_created),
        _rabbitmq_running=Mock(return_value=rabbitmq_running),
        _operator_user_recovery_required=Mock(return_value=recovery_required),
    )

    result = charm.RabbitMQOperatorCharm._require_queue_management_ready(
        fake, event
    )

    assert result is False
    event.fail.assert_called_once_with(message)


def test_require_queue_management_ready_passes():
    """All preconditions met returns True without failing the event."""
    event = Mock()
    unit = Mock()
    unit.is_leader.return_value = True
    fake = _fake_charm(
        unit=unit,
        peers=SimpleNamespace(operator_user_created="rmqadmin"),
        _rabbitmq_running=Mock(return_value=True),
        _operator_user_recovery_required=Mock(return_value=False),
    )

    result = charm.RabbitMQOperatorCharm._require_queue_management_ready(
        fake, event
    )

    assert result is True
    event.fail.assert_not_called()


def test_on_update_status_reconciles_without_event():
    """Update-status should drive reconcile without a deferrable event object."""
    fake = _fake_charm(_reconcile=Mock())

    charm.RabbitMQOperatorCharm._on_update_status(fake, Mock())

    fake._reconcile.assert_called_once_with(None)


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


def test_get_queue_growth_selector_zero_members():
    """A queue with zero members needs all nodes as candidates."""
    fake = _fake_charm()

    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 0, 0)
        == "all"
    )
    assert (
        charm.RabbitMQOperatorCharm.get_queue_growth_selector(fake, 0, 3)
        == "all"
    )


def test_peer_relation_leaving_does_not_forget_cluster_node_by_default():
    """The default auto path must never forget a cluster node.

    forget_cluster_node permanently removes a node and deletes its quorum
    queue replicas.  A peer-relation-departed event can fire for transient
    reasons (juju agent loss, pod reschedule, controller hiccup), so automatic
    cleanup is opt-in.
    """
    container = Mock()
    event = SimpleNamespace(nodename="rabbitmq-k8s/1")
    fake = _fake_charm(
        unit=Mock(
            is_leader=Mock(return_value=True),
            get_container=Mock(return_value=container),
        ),
        _reconcile=Mock(),
    )

    charm.RabbitMQOperatorCharm._on_peer_relation_leaving(fake, event)

    container.exec.assert_not_called()


def test_peer_relation_leaving_marks_departing_unit_during_reconcile():
    """Reconcile during a leaving event must know the departing unit name."""
    captured: dict[str, str | None] = {}
    event = SimpleNamespace(nodename="rabbitmq-k8s/1")

    def capture(_event):
        captured["unit_name"] = fake._departing_unit_name

    fake = _fake_charm(
        unit=Mock(is_leader=Mock(return_value=True)),
        _reconcile=Mock(side_effect=capture),
    )
    fake._departing_unit_name = None

    charm.RabbitMQOperatorCharm._on_peer_relation_leaving(fake, event)

    assert captured["unit_name"] == "rabbitmq-k8s/1"
    assert fake._departing_unit_name is None
    fake._reconcile.assert_called_once_with(event)


def test_expected_cluster_size_excludes_named_departing_unit():
    """Expected cluster size should ignore the unit currently departing."""
    fake = _fake_charm(
        peers=SimpleNamespace(
            peers_rel=SimpleNamespace(
                units=[
                    SimpleNamespace(name="rabbitmq-k8s/1"),
                    SimpleNamespace(name="rabbitmq-k8s/2"),
                ]
            )
        )
    )
    fake._departing_unit_name = "rabbitmq-k8s/1"

    assert charm.RabbitMQOperatorCharm._expected_cluster_size.fget(fake) == 2


def test_auto_forget_stale_nodes_forgets_absent_non_running_nodes():
    """Opt-in stale-node cleanup forgets only absent, non-running disk nodes."""
    cluster_status = {
        "disk_nodes": [
            "rabbit@rabbitmq-k8s-0.rabbitmq-k8s-endpoints",
            "rabbit@rabbitmq-k8s-1.rabbitmq-k8s-endpoints",
            "rabbit@rabbitmq-k8s-2.rabbitmq-k8s-endpoints",
        ],
        "running_nodes": [
            "rabbit@rabbitmq-k8s-0.rabbitmq-k8s-endpoints",
            "rabbit@rabbitmq-k8s-2.rabbitmq-k8s-endpoints",
        ],
    }
    container = Mock()
    container.exec.side_effect = [
        Mock(wait_output=Mock(return_value=(json.dumps(cluster_status), ""))),
        Mock(wait_output=Mock(return_value=("", ""))),
    ]
    fake = _fake_charm(
        config={
            "auto-forget-stale-nodes": True,
        },
        unit=SimpleNamespace(
            is_leader=lambda: True,
            name="rabbitmq-k8s/0",
            get_container=Mock(return_value=container),
        ),
        peers=SimpleNamespace(
            operator_user_created="operator",
            peers_rel=SimpleNamespace(
                units=[SimpleNamespace(name="rabbitmq-k8s/2")]
            ),
        ),
        _departing_unit_name=None,
        generate_nodename=Mock(
            side_effect=lambda unit: (
                f"rabbit@{unit.replace('/', '-')}.rabbitmq-k8s-endpoints"
            )
        ),
    )
    fake._expected_cluster_nodes = (
        lambda: charm.RabbitMQOperatorCharm._expected_cluster_nodes(fake)
    )
    fake._run_rabbitmqctl = lambda *args, timeout=30: container.exec(
        ["rabbitmqctl", *args], timeout=timeout
    ).wait_output()
    fake._cluster_status = lambda: charm.RabbitMQOperatorCharm._cluster_status(
        fake
    )
    fake._auto_forget_stale_cluster_nodes_ready = lambda: charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes_ready(
        fake
    )
    fake._defer_or_continue = (
        lambda event: charm.RabbitMQOperatorCharm._defer_or_continue(
            fake, event
        )
    )
    fake._forget_cluster_node_if_present = lambda node: charm.RabbitMQOperatorCharm._forget_cluster_node_if_present(
        fake, node
    )
    event = Mock()

    assert charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes(
        fake, event
    )

    container.exec.assert_has_calls(
        [
            call(
                ["rabbitmqctl", "cluster_status", "--formatter=json"],
                timeout=30,
            ),
            call(
                [
                    "rabbitmqctl",
                    "forget_cluster_node",
                    "rabbit@rabbitmq-k8s-1.rabbitmq-k8s-endpoints",
                ],
                timeout=5 * 60,
            ),
        ]
    )
    event.defer.assert_not_called()


def test_auto_forget_stale_nodes_skips_during_peer_departure():
    """Peer-departed hooks must not run destructive stale-node cleanup."""
    container = Mock()
    fake = _fake_charm(
        config={
            "auto-forget-stale-nodes": True,
        },
        unit=SimpleNamespace(
            is_leader=lambda: True,
            name="rabbitmq-k8s/2",
            get_container=Mock(return_value=container),
        ),
        peers=SimpleNamespace(
            operator_user_created="operator",
            peers_rel=SimpleNamespace(
                units=[
                    SimpleNamespace(name="rabbitmq-k8s/0"),
                    SimpleNamespace(name="rabbitmq-k8s/1"),
                ]
            ),
        ),
        _departing_unit_name="rabbitmq-k8s/2",
    )
    fake._auto_forget_stale_cluster_nodes_ready = lambda: charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes_ready(
        fake
    )
    event = Mock()

    assert charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes(
        fake, event
    )

    container.exec.assert_not_called()
    event.defer.assert_not_called()


def test_auto_forget_stale_nodes_continues_when_unexpected_node_still_running():
    """Opt-in stale-node cleanup does not block hooks on running nodes."""
    departing_node = "rabbit@rabbitmq-k8s-1.rabbitmq-k8s-endpoints"
    cluster_status = {
        "disk_nodes": [
            "rabbit@rabbitmq-k8s-0.rabbitmq-k8s-endpoints",
            departing_node,
        ],
        "running_nodes": [
            "rabbit@rabbitmq-k8s-0.rabbitmq-k8s-endpoints",
            departing_node,
        ],
    }
    container = Mock()
    container.exec.return_value = Mock(
        wait_output=Mock(return_value=(json.dumps(cluster_status), ""))
    )
    fake = _fake_charm(
        config={
            "auto-forget-stale-nodes": True,
        },
        unit=SimpleNamespace(
            is_leader=lambda: True,
            name="rabbitmq-k8s/0",
            get_container=Mock(return_value=container),
        ),
        peers=SimpleNamespace(
            operator_user_created="operator",
            peers_rel=SimpleNamespace(
                units=[SimpleNamespace(name="rabbitmq-k8s/0")]
            ),
        ),
        _departing_unit_name=None,
        generate_nodename=Mock(
            side_effect=lambda unit: (
                f"rabbit@{unit.replace('/', '-')}.rabbitmq-k8s-endpoints"
            )
        ),
    )
    fake._expected_cluster_nodes = (
        lambda: charm.RabbitMQOperatorCharm._expected_cluster_nodes(fake)
    )
    fake._run_rabbitmqctl = lambda *args, timeout=30: container.exec(
        ["rabbitmqctl", *args], timeout=timeout
    ).wait_output()
    fake._cluster_status = lambda: charm.RabbitMQOperatorCharm._cluster_status(
        fake
    )
    fake._auto_forget_stale_cluster_nodes_ready = lambda: charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes_ready(
        fake
    )
    fake._defer_or_continue = (
        lambda event: charm.RabbitMQOperatorCharm._defer_or_continue(
            fake, event
        )
    )
    fake._forget_cluster_node_if_present = lambda node: charm.RabbitMQOperatorCharm._forget_cluster_node_if_present(
        fake, node
    )
    event = Mock()

    assert charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes(
        fake, event
    )

    container.exec.assert_called_once_with(
        ["rabbitmqctl", "cluster_status", "--formatter=json"],
        timeout=30,
    )
    event.defer.assert_not_called()


def test_auto_forget_stale_nodes_skips_when_local_node_is_not_expected():
    """A doomed unit must not perform stale-node cleanup for the survivor."""
    container = Mock()
    fake = _fake_charm(
        config={
            "auto-forget-stale-nodes": True,
        },
        unit=SimpleNamespace(
            is_leader=lambda: True,
            name="rabbitmq-k8s/2",
            get_container=Mock(return_value=container),
        ),
        peers=SimpleNamespace(
            operator_user_created="operator",
            peers_rel=SimpleNamespace(
                units=[SimpleNamespace(name="rabbitmq-k8s/0")]
            ),
        ),
        _departing_unit_name="rabbitmq-k8s/2",
        generate_nodename=Mock(
            side_effect=lambda unit: (
                f"rabbit@{unit.replace('/', '-')}.rabbitmq-k8s-endpoints"
            )
        ),
    )
    fake._expected_cluster_nodes = (
        lambda: charm.RabbitMQOperatorCharm._expected_cluster_nodes(fake)
    )
    fake._auto_forget_stale_cluster_nodes_ready = lambda: charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes_ready(
        fake
    )
    event = Mock()

    assert charm.RabbitMQOperatorCharm._auto_forget_stale_cluster_nodes(
        fake, event
    )

    container.exec.assert_not_called()
    event.defer.assert_not_called()


def test_ensure_broker_running_force_boots_single_survivor_after_scale_down():
    """Opt-in single survivor recovery force-boots pause_minority shutdown."""
    event = Mock()
    container = Mock()
    fake = _fake_charm(
        config={
            "auto-forget-stale-nodes": True,
        },
        unit=SimpleNamespace(
            is_leader=lambda: True,
            name="rabbitmq-k8s/0",
            get_container=Mock(return_value=container),
        ),
        peers=SimpleNamespace(
            operator_user_created="operator",
            peers_rel=SimpleNamespace(units=[]),
        ),
        _departing_unit_name=None,
        _rabbitmq_running=Mock(side_effect=[False, False, True]),
        _reconcile_workload=Mock(return_value=True),
    )
    fake._expected_cluster_size = 1
    fake._run_rabbitmqctl = Mock(return_value=("", ""))
    fake._scale_down_force_boot_allowed = lambda event: charm.RabbitMQOperatorCharm._scale_down_force_boot_allowed(
        fake, event
    )
    fake._force_boot_single_survivor = (
        lambda: charm.RabbitMQOperatorCharm._force_boot_single_survivor(fake)
    )
    fake._force_boot_single_survivor_after_scale_down = lambda event: charm.RabbitMQOperatorCharm._force_boot_single_survivor_after_scale_down(
        fake, event
    )

    assert charm.RabbitMQOperatorCharm._ensure_broker_running(fake, event)

    fake._run_rabbitmqctl.assert_has_calls(
        [
            call("force_boot", timeout=5 * 60),
            call(
                "await_startup",
                timeout=charm.RABBITMQ_STARTUP_GRACE_SECONDS,
            ),
        ]
    )
    container.restart.assert_called_once_with(charm.RABBITMQ_SERVICE)
    event.defer.assert_not_called()


def test_ensure_broker_running_does_not_force_boot_during_departure_hook():
    """Single survivor recovery must not run from peer-departed hooks."""
    event = Mock()
    container = Mock()
    fake = _fake_charm(
        config={
            "auto-forget-stale-nodes": True,
        },
        unit=SimpleNamespace(
            is_leader=lambda: True,
            name="rabbitmq-k8s/0",
            get_container=Mock(return_value=container),
        ),
        peers=SimpleNamespace(
            operator_user_created="operator",
            peers_rel=SimpleNamespace(units=[]),
        ),
        _departing_unit_name="rabbitmq-k8s/1",
        _rabbitmq_running=Mock(side_effect=[False, False]),
        _reconcile_workload=Mock(return_value=True),
    )
    fake._expected_cluster_size = 1
    fake._run_rabbitmqctl = Mock(return_value=("", ""))
    fake._scale_down_force_boot_allowed = lambda event: charm.RabbitMQOperatorCharm._scale_down_force_boot_allowed(
        fake, event
    )
    fake._force_boot_single_survivor = (
        lambda: charm.RabbitMQOperatorCharm._force_boot_single_survivor(fake)
    )
    fake._force_boot_single_survivor_after_scale_down = lambda event: charm.RabbitMQOperatorCharm._force_boot_single_survivor_after_scale_down(
        fake, event
    )

    assert not charm.RabbitMQOperatorCharm._ensure_broker_running(fake, event)

    fake._run_rabbitmqctl.assert_not_called()
    container.restart.assert_not_called()
    event.defer.assert_called_once_with()


def test_workload_reconcile_prerequisites_non_leader_no_operator_user():
    """Non-leader without operator_user_created defers and returns False."""
    event = Mock()
    fake = _fake_charm(
        unit=Mock(is_leader=Mock(return_value=False)),
        peers=SimpleNamespace(
            erlang_cookie="some-cookie",
            operator_user_created=None,
        ),
        peers_bind_address="10.10.1.1",
    )

    result = charm.RabbitMQOperatorCharm._workload_reconcile_prerequisites(
        fake, event
    )

    assert result is False
    event.defer.assert_called_once()


def test_annotation_values_with_special_characters_accepted():
    """K8s annotation values with colons, slashes, etc. are valid."""
    assert charm.validate_annotation_value("http://example.com") is True
    assert charm.validate_annotation_value("key:value") is True
    assert charm.validate_annotation_value("a@b=c d") is True
    assert charm.validate_annotation_value("") is False


def test_on_collect_unit_status_does_not_mutate_listener_state():
    """Status collection must not call _reconcile_listener_protection."""
    container = Mock()
    container.exists.return_value = False
    event = Mock()
    fake = _fake_charm(
        unit=Mock(
            is_leader=Mock(return_value=True),
            get_container=Mock(return_value=container),
            set_workload_version=Mock(),
        ),
        peers=SimpleNamespace(
            erlang_cookie="cookie",
            operator_user_created=True,
            operator_password="op-pass",
        ),
        _stored=SimpleNamespace(rabbitmq_version="3.12.0"),
        _read_safety_status=Mock(return_value=(True, "safe")),
        _undersized_queue_count=Mock(return_value=0),
        _operator_user_recovery_required=Mock(return_value=False),
        _configuration_error=Mock(return_value=None),
    )
    fake._pre_broker_status = (
        lambda: charm.RabbitMQOperatorCharm._pre_broker_status(fake)
    )
    fake._running_broker_status = (
        lambda: charm.RabbitMQOperatorCharm._running_broker_status(fake)
    )
    fake._reconcile_listener_protection = Mock()

    charm.RabbitMQOperatorCharm._on_collect_unit_status(fake, event)

    fake._reconcile_listener_protection.assert_not_called()


def test_on_collect_unit_status_uses_read_safety_status():
    """Status collection reads pebble check status via _read_safety_status."""
    event = Mock()
    fake = _fake_charm(
        unit=Mock(
            is_leader=Mock(return_value=True),
            set_workload_version=Mock(),
        ),
        peers=SimpleNamespace(
            erlang_cookie="cookie",
            operator_user_created=True,
            operator_password="op-pass",
        ),
        _stored=SimpleNamespace(rabbitmq_version="3.12.0"),
        _read_safety_status=Mock(return_value=(True, "safe")),
        _undersized_queue_count=Mock(return_value=0),
        _operator_user_recovery_required=Mock(return_value=False),
        _configuration_error=Mock(return_value=None),
    )
    fake._pre_broker_status = (
        lambda: charm.RabbitMQOperatorCharm._pre_broker_status(fake)
    )
    fake._running_broker_status = (
        lambda: charm.RabbitMQOperatorCharm._running_broker_status(fake)
    )
    charm.RabbitMQOperatorCharm._on_collect_unit_status(fake, event)

    fake._read_safety_status.assert_called_once()


def test_retrieve_password_returns_none_not_string_none():
    """Missing passwords must return None, not the string 'None'."""
    from interface_rabbitmq_peers import (
        RabbitMQOperatorPeers,
    )

    app_data = {}
    app_key = Mock()
    rel = Mock()
    rel.data = {app_key: app_data}
    rel.app = app_key

    peers = object.__new__(RabbitMQOperatorPeers)
    peers.relation_name = "peers"

    with patch.object(
        type(peers),
        "peers_rel",
        new_callable=lambda: property(lambda self: rel),
    ):
        result = peers.retrieve_password("nonexistent-user")

    assert result is None
    assert result != "None"


def test_on_pebble_check_failed_suspends_on_ready_check():
    """The ready check failure drives listener suspension."""
    container = Mock()
    container.exists.return_value = False
    info = SimpleNamespace(name="ready")
    event = SimpleNamespace(info=info)
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
    )
    fake._suspend_listeners = (
        lambda c: charm.RabbitMQOperatorCharm._suspend_listeners(fake, c)
    )
    fake._reconcile_listener_protection = lambda safe: charm.RabbitMQOperatorCharm._reconcile_listener_protection(
        fake, safe
    )

    charm.RabbitMQOperatorCharm._on_pebble_check_failed(fake, event)

    # Handler calls _reconcile_listener_protection(safe=False), which reads
    # container from self.unit.get_container() (not event.workload).
    container.exec.assert_called_once_with(
        ["rabbitmqctl", "suspend_listeners"], timeout=30
    )


def test_on_pebble_check_failed_ignores_alive_check():
    """The alive check failure does not drive listener protection."""
    info = SimpleNamespace(name="alive")
    event = SimpleNamespace(info=info)
    fake = _fake_charm()
    fake._reconcile_listener_protection = Mock()

    charm.RabbitMQOperatorCharm._on_pebble_check_failed(fake, event)

    fake._reconcile_listener_protection.assert_not_called()


def test_on_pebble_check_recovered_resumes_on_ready_check():
    """The ready check recovery drives listener resumption."""
    container = Mock()
    container.exists.return_value = True
    container.exec.side_effect = [Mock(), Mock()]
    info = SimpleNamespace(name="ready")
    event = SimpleNamespace(info=info)
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
    )
    fake._resume_listeners = (
        lambda c, ctx: charm.RabbitMQOperatorCharm._resume_listeners(
            fake, c, ctx
        )
    )
    fake._reconcile_listener_protection = lambda safe: charm.RabbitMQOperatorCharm._reconcile_listener_protection(
        fake, safe
    )

    charm.RabbitMQOperatorCharm._on_pebble_check_recovered(fake, event)

    assert container.exec.call_args_list == [
        call(["rabbitmqctl", "await_startup"], timeout=60),
        call(["rabbitmqctl", "resume_listeners"], timeout=30),
    ]


def test_undersized_queue_count_returns_zero_when_admin_api_unavailable():
    """Queue status should not fail if the management API is still starting."""
    fake = _fake_charm(
        unit=Mock(is_leader=Mock(return_value=True)),
        peers=SimpleNamespace(operator_user_created=True),
        _manage_queues=Mock(return_value=True),
        _rabbitmq_running=Mock(return_value=True),
        get_undersized_queues=Mock(
            side_effect=requests.exceptions.ConnectionError("not ready")
        ),
    )

    result = charm.RabbitMQOperatorCharm._undersized_queue_count(fake)

    assert result == 0


def test_reconcile_does_not_drive_listener_protection():
    """Reconcile delegates listener protection to pebble check events."""
    fake = _fake_charm(
        _ensure_broker_running=Mock(return_value=True),
        _reconcile_operator_user=Mock(return_value=True),
        _reconcile_amqp_relations=Mock(return_value=True),
        _cleanup_stale_amqp_users=Mock(),
        _reconcile_queue_membership=Mock(return_value=True),
        _publish_relation_data=Mock(),
        _reconcile_lb=Mock(),
    )
    fake._reconcile_listener_protection = Mock()

    charm.RabbitMQOperatorCharm._reconcile(fake, None)

    fake._reconcile_listener_protection.assert_not_called()


def test_reconcile_reconciles_health_checks_before_and_after_bootstrap():
    """Reconcile should quiesce checks before startup and re-enable them after."""
    order = []
    fake = _fake_charm(
        _reconcile_lb=Mock(side_effect=lambda *_: order.append("lb")),
        _reconcile_health_checks=Mock(
            side_effect=lambda: order.append("health")
        ),
        _ensure_broker_running=Mock(
            side_effect=lambda *_: order.append("ensure") or True
        ),
        _reconcile_operator_user=Mock(
            side_effect=lambda *_: order.append("operator") or True
        ),
        _reconcile_running_broker_state=Mock(
            side_effect=lambda: order.append("running") or True
        ),
        _reconcile_amqp_relations=Mock(
            side_effect=lambda *_: order.append("amqp") or True
        ),
        _cleanup_stale_amqp_users=Mock(
            side_effect=lambda: order.append("cleanup")
        ),
        _reconcile_queue_membership=Mock(
            side_effect=lambda *_: order.append("queues") or True
        ),
        _publish_relation_data=Mock(
            side_effect=lambda: order.append("publish")
        ),
    )

    charm.RabbitMQOperatorCharm._reconcile(fake, None)

    assert order == [
        "lb",
        "health",
        "ensure",
        "operator",
        "running",
        "health",
        "amqp",
        "cleanup",
        "queues",
        "publish",
    ]


def test_on_pebble_check_recovered_ignores_alive_check():
    """The alive check recovery does not drive listener protection."""
    info = SimpleNamespace(name="alive")
    event = SimpleNamespace(info=info)
    fake = _fake_charm()
    fake._reconcile_listener_protection = Mock()

    charm.RabbitMQOperatorCharm._on_pebble_check_recovered(fake, event)

    fake._reconcile_listener_protection.assert_not_called()


def test_local_node_in_running_cluster_returns_true_when_node_present():
    """Local cluster membership relies on running_nodes from diagnostics."""
    process = Mock(
        wait_output=Mock(
            return_value=(
                json.dumps(
                    {
                        "running_nodes": [
                            "rabbit@rabbitmq-k8s-0.rabbitmq-k8s-endpoints"
                        ]
                    }
                ),
                "",
            )
        )
    )
    container = Mock(exec=Mock(return_value=process))
    fake = _fake_charm(
        unit=SimpleNamespace(
            name="rabbitmq-k8s/0",
            get_container=Mock(return_value=container),
        )
    )

    assert charm.RabbitMQOperatorCharm._local_node_in_running_cluster(fake)
    container.exec.assert_called_once_with(
        [
            "rabbitmq-diagnostics",
            "cluster_status",
            "--formatter=json",
        ],
        timeout=30,
    )


def test_local_node_in_running_cluster_returns_false_on_exec_error():
    """Diagnostic failures mean the unit is not yet ready for health checks."""
    container = Mock()
    container.exec.side_effect = ops.pebble.ExecError(
        ["rabbitmq-diagnostics", "cluster_status", "--formatter=json"],
        1,
        "",
        "not ready",
    )
    fake = _fake_charm(
        unit=SimpleNamespace(
            name="rabbitmq-k8s/0",
            get_container=Mock(return_value=container),
        )
    )

    assert not charm.RabbitMQOperatorCharm._local_node_in_running_cluster(fake)


@pytest.mark.parametrize(
    "operator_user_created,rabbitmq_running,node_in_cluster,expected",
    [
        (None, True, True, False),
        ("operator", False, True, False),
        ("operator", True, False, False),
        ("operator", True, True, True),
    ],
)
def test_health_checks_ready_requires_local_bootstrap(
    operator_user_created, rabbitmq_running, node_in_cluster, expected
):
    """Health checks run only once local startup and cluster join completed."""
    fake = _fake_charm(
        peers=SimpleNamespace(operator_user_created=operator_user_created),
        _rabbitmq_running=Mock(return_value=rabbitmq_running),
        _local_node_in_running_cluster=Mock(return_value=node_in_cluster),
    )

    result = charm.RabbitMQOperatorCharm._health_checks_ready(fake)

    assert result is expected


def test_reconcile_health_checks_stops_inactive_checks_until_ready():
    """Checks stay stopped while the local broker is still converging."""
    container = Mock(stop_checks=Mock())
    container.get_check.return_value = SimpleNamespace(
        status=ops.pebble.CheckStatus.INACTIVE
    )
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _health_checks_ready=Mock(return_value=False),
    )

    charm.RabbitMQOperatorCharm._reconcile_health_checks(fake)

    container.stop_checks.assert_called_once_with("alive", "ready")


def test_reconcile_health_checks_starts_and_resumes_stale_protection():
    """Re-enabling checks should clear a stale protection marker first."""
    container = Mock(
        start_checks=Mock(),
        exists=Mock(return_value=True),
    )
    container.get_check.return_value = SimpleNamespace(
        status=ops.pebble.CheckStatus.INACTIVE
    )
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
        _health_checks_ready=Mock(return_value=True),
    )
    fake._resume_listeners = (
        lambda current, context: charm.RabbitMQOperatorCharm._resume_listeners(
            fake, current, context
        )
    )

    charm.RabbitMQOperatorCharm._reconcile_health_checks(fake)

    assert container.exec.call_args_list == [
        call(["rabbitmqctl", "await_startup"], timeout=60),
        call(["rabbitmqctl", "resume_listeners"], timeout=30),
    ]
    container.start_checks.assert_called_once_with("alive", "ready")


def test_read_safety_status_returns_safe_when_check_is_up():
    """A passing ready check means the broker is safe."""
    container = Mock()
    check_info = SimpleNamespace(status=ops.pebble.CheckStatus.UP)
    container.get_check.return_value = check_info
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
    )

    safe, reason = charm.RabbitMQOperatorCharm._read_safety_status(fake)

    assert safe is True
    assert reason == "safe"
    container.get_check.assert_called_once_with("ready")


def test_read_safety_status_returns_unsafe_with_reason_from_file():
    """A failing ready check reads the reason from the reason file."""
    container = Mock()
    check_info = SimpleNamespace(status=ops.pebble.CheckStatus.DOWN)
    container.get_check.return_value = check_info
    container.pull.return_value = SimpleNamespace(
        read=lambda: "Local alarms active\n"
    )
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
    )

    safe, reason = charm.RabbitMQOperatorCharm._read_safety_status(fake)

    assert safe is False
    assert reason == "Local alarms active"


def test_read_safety_status_falls_back_when_reason_file_missing():
    """A failing check without a reason file uses a generic message."""
    container = Mock()
    check_info = SimpleNamespace(status=ops.pebble.CheckStatus.DOWN)
    container.get_check.return_value = check_info
    container.pull.side_effect = ops.pebble.PathError("not-found", "not found")
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
    )

    safe, reason = charm.RabbitMQOperatorCharm._read_safety_status(fake)

    assert safe is False
    assert "safety check failing" in reason.lower()


def test_read_safety_status_treats_inactive_as_safe():
    """A check that hasn't run yet (INACTIVE) is treated as safe."""
    container = Mock()
    check_info = SimpleNamespace(status=ops.pebble.CheckStatus.INACTIVE)
    container.get_check.return_value = check_info
    fake = _fake_charm(
        unit=Mock(get_container=Mock(return_value=container)),
    )

    safe, reason = charm.RabbitMQOperatorCharm._read_safety_status(fake)

    assert safe is True
