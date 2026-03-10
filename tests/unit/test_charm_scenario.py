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

"""Scenario tests for RabbitMQOperatorCharm."""

from unittest.mock import (
    Mock,
)

import ops.model
import ops.pebble
from ops import (
    testing,
)

import charm


def _state(
    rabbitmq_container,
    networks,
    *,
    leader=False,
    relations=None,
):
    """Build a testing state for the charm."""
    return testing.State(
        leader=leader,
        relations=relations or [],
        containers=[rabbitmq_container],
        networks=networks,
    )


def _patch_config_changed_for_success(
    monkeypatch, charm_instance, admin_api=None
):
    """Patch config-changed dependencies so the event can complete."""
    monkeypatch.setattr(
        charm_instance,
        "_set_ownership_on_data_dir",
        lambda: True,
    )
    monkeypatch.setattr(
        charm_instance,
        "_get_admin_api",
        lambda *args, **kwargs: admin_api
        or Mock(list_quorum_queues=Mock(return_value=[])),
    )
    monkeypatch.setattr(
        type(charm_instance),
        "rabbit_running",
        property(lambda self: True),
    )


def test_get_operator_info_action(ctx, rabbitmq_container, networks):
    """The operator info action reports the operator credentials."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={"operator_password": "foobar"},
        peers_data={0: {}},
    )
    state = _state(rabbitmq_container, networks, leader=True, relations=[peer])

    ctx.run(ctx.on.action("get-operator-info"), state)

    assert ctx.action_results == {
        "operator-user": "operator",
        "operator-password": "foobar",
    }


def test_rabbitmq_pebble_ready(ctx, rabbitmq_container, networks, monkeypatch):
    """Pebble ready configures the expected services and starts rabbitmq."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={
            "operator_password": "foobar",
            "operator_user_created": "rmqadmin",
            "erlang_cookie": "magicsecurity",
        },
        peers_data={0: {}},
    )
    state = _state(rabbitmq_container, networks, leader=True, relations=[peer])

    with ctx(ctx.on.pebble_ready(rabbitmq_container), state) as manager:
        _patch_config_changed_for_success(monkeypatch, manager.charm)
        state_out = manager.run()

    container = state_out.get_container(charm.RABBITMQ_CONTAINER)
    assert set(container.plan.to_dict()["services"]) == {
        "rabbitmq",
        "epmd",
        "notifier",
    }
    assert (
        container.service_statuses[charm.RABBITMQ_SERVICE]
        == ops.pebble.ServiceStatus.ACTIVE
    )


def test_config_changed_defers_without_operator_user(
    ctx, rabbitmq_container, networks
):
    """A non-leader defers config-changed until the operator user exists."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={
            "operator_password": "foobar",
            "erlang_cookie": "magicsecurity",
        },
        peers_data={0: {}},
    )
    state = _state(
        rabbitmq_container, networks, leader=False, relations=[peer]
    )

    state_out = ctx.run(ctx.on.config_changed(), state)

    assert len(state_out.deferred) == 1
    assert state_out.deferred[0].observer == "_on_config_changed"


def test_config_changed_proceeds_for_leader_without_operator_user(
    ctx, rabbitmq_container, networks, monkeypatch
):
    """The leader should not defer once connectivity and peer data are ready."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={
            "operator_password": "foobar",
            "erlang_cookie": "magicsecurity",
        },
        peers_data={0: {}},
    )
    state = _state(rabbitmq_container, networks, leader=True, relations=[peer])

    with ctx(ctx.on.config_changed(), state) as manager:
        _patch_config_changed_for_success(monkeypatch, manager.charm)
        state_out = manager.run()

    assert state_out.deferred == []


def test_config_changed_proceeds_for_non_leader_with_operator_user(
    ctx, rabbitmq_container, networks, monkeypatch
):
    """A non-leader should not defer once the operator user is present."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={
            "operator_password": "foobar",
            "operator_user_created": "rmqadmin",
            "erlang_cookie": "magicsecurity",
        },
        peers_data={0: {}},
    )
    state = _state(
        rabbitmq_container, networks, leader=False, relations=[peer]
    )

    with ctx(ctx.on.config_changed(), state) as manager:
        _patch_config_changed_for_success(monkeypatch, manager.charm)
        state_out = manager.run()

    assert state_out.deferred == []


def test_update_status_waiting_without_operator_user(
    ctx, rabbitmq_container, networks
):
    """Update status waits until the leader bootstraps the operator user."""
    state = _state(rabbitmq_container, networks, leader=True)

    state_out = ctx.run(ctx.on.update_status(), state)

    assert state_out.unit_status == ops.model.WaitingStatus(
        "Waiting for leader to create operator user"
    )


def test_update_status_active_when_relations_ready(
    ctx, rabbitmq_container, networks, monkeypatch
):
    """Update status becomes active once the peer and AMQP data are ready."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={
            "operator_password": "foobar",
            "operator_user_created": "rmqadmin",
            "erlang_cookie": "magicsecurity",
        },
        peers_data={0: {}},
    )
    amqp_relation = testing.Relation(
        endpoint="amqp",
        remote_app_name="amqp-client-app",
        remote_app_data={
            "username": "client",
            "vhost": "client-vhost",
        },
        remote_units_data={0: {"ingress-address": "10.20.0.5"}},
    )
    state = _state(
        rabbitmq_container,
        networks,
        leader=True,
        relations=[peer, amqp_relation],
    )

    with ctx(ctx.on.update_status(), state) as manager:
        monkeypatch.setattr(
            type(manager.charm),
            "rabbit_running",
            property(lambda self: True),
        )
        monkeypatch.setattr(
            manager.charm,
            "_get_admin_api",
            lambda *args, **kwargs: Mock(
                list_quorum_queues=Mock(return_value=[])
            ),
        )
        state_out = manager.run()

    assert state_out.unit_status == ops.model.ActiveStatus()


def test_add_member_action_calls_admin_api(
    ctx, rabbitmq_container, networks, monkeypatch
):
    """The add-member action calls the admin API with the generated nodename."""
    state = _state(rabbitmq_container, networks, leader=True)
    admin_api = Mock()

    with ctx(
        ctx.on.action(
            "add-member",
            params={
                "unit-name": "unit/1",
                "vhost": "/",
                "queue-name": "test_queue",
            },
        ),
        state,
    ) as manager:
        monkeypatch.setattr(manager.charm, "_get_admin_api", lambda: admin_api)
        monkeypatch.setattr(
            manager.charm, "_on_update_status", lambda event: None
        )
        manager.run()

    admin_api.add_member.assert_called_once_with(
        "rabbit@unit-1.rabbitmq-k8s-endpoints", "/", "test_queue"
    )


def test_delete_member_action_calls_admin_api(
    ctx, rabbitmq_container, networks, monkeypatch
):
    """The delete-member action calls the admin API with the generated nodename."""
    state = _state(rabbitmq_container, networks, leader=True)
    admin_api = Mock()

    with ctx(
        ctx.on.action(
            "delete-member",
            params={
                "unit-name": "unit/1",
                "vhost": "/",
                "queue-name": "test_queue",
            },
        ),
        state,
    ) as manager:
        monkeypatch.setattr(manager.charm, "_get_admin_api", lambda: admin_api)
        monkeypatch.setattr(
            manager.charm, "_on_update_status", lambda event: None
        )
        manager.run()

    admin_api.delete_member.assert_called_once_with(
        "rabbit@unit-1.rabbitmq-k8s-endpoints", "/", "test_queue"
    )


def test_timer_notice_calls_ensure_queue_ha_for_leader(
    ctx, rabbitmq_container, networks, timer_notice, monkeypatch
):
    """The notifier notice only triggers queue HA on the leader."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={
            "operator_password": "foobar",
            "operator_user_created": "rmqadmin",
            "erlang_cookie": "magicsecurity",
        },
        peers_data={0: {}},
    )
    container = testing.Container(
        name=charm.RABBITMQ_CONTAINER,
        can_connect=True,
        notices=[timer_notice],
    )
    state = testing.State(
        leader=True,
        relations=[peer],
        containers=[container],
        networks=networks,
    )
    ensure_queue_ha = Mock()

    with ctx(
        ctx.on.pebble_custom_notice(container, timer_notice), state
    ) as manager:
        monkeypatch.setattr(
            type(manager.charm),
            "rabbit_running",
            property(lambda self: True),
        )
        monkeypatch.setattr(manager.charm, "ensure_queue_ha", ensure_queue_ha)
        monkeypatch.setattr(
            manager.charm, "_on_update_status", lambda event: None
        )
        manager.run()

    ensure_queue_ha.assert_called_once_with()


def test_timer_notice_skips_ensure_queue_ha_for_non_leader(
    ctx, rabbitmq_container, networks, timer_notice, monkeypatch
):
    """The notifier notice is ignored by non-leader units."""
    peer = testing.PeerRelation(
        endpoint="peers",
        local_app_data={
            "operator_password": "foobar",
            "operator_user_created": "rmqadmin",
            "erlang_cookie": "magicsecurity",
        },
        peers_data={0: {}},
    )
    container = testing.Container(
        name=charm.RABBITMQ_CONTAINER,
        can_connect=True,
        notices=[timer_notice],
    )
    state = testing.State(
        leader=False,
        relations=[peer],
        containers=[container],
        networks=networks,
    )
    ensure_queue_ha = Mock()

    with ctx(
        ctx.on.pebble_custom_notice(container, timer_notice), state
    ) as manager:
        monkeypatch.setattr(
            type(manager.charm),
            "rabbit_running",
            property(lambda self: True),
        )
        monkeypatch.setattr(manager.charm, "ensure_queue_ha", ensure_queue_ha)
        manager.run()

    ensure_queue_ha.assert_not_called()
