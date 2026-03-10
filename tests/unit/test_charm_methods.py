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

from types import (
    SimpleNamespace,
)
from unittest.mock import (
    Mock,
    call,
    patch,
)

import ops.model
import ops.pebble
import pytest

import charm


def _fake_charm(**kwargs):
    """Build a thin object that can exercise charm helper methods."""
    base = {
        "app": SimpleNamespace(name="rabbitmq-k8s"),
        "unit": Mock(),
        "peers": SimpleNamespace(
            erlang_cookie=None,
            set_erlang_cookie=Mock(),
            operator_user_created=None,
        ),
        "_get_admin_api": Mock(),
        "min_replicas": lambda: 3,
        "generate_nodename": lambda unit_name: charm.RabbitMQOperatorCharm.generate_nodename(
            SimpleNamespace(app=SimpleNamespace(name="rabbitmq-k8s")),
            unit_name,
        ),
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


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
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))

    result = charm.RabbitMQOperatorCharm._set_ownership_on_data_dir(fake)

    assert result is False
    container.exec.assert_called_once_with(
        ["mountpoint", charm.RABBITMQ_DATA_DIR]
    )


def test_set_ownership_on_data_dir_ensure_ownership_fails():
    """Data-dir ownership stops on the first ownership failure."""
    container = Mock()
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
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
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
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
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
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
    fake = _fake_charm(unit=Mock(get_container=Mock(return_value=container)))
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
