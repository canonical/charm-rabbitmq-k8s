#
# Copyright 2023 Canonical Ltd.
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
#
# Learn more at: https://juju.is/docs/sdk

"""Add missing functionality to rabbitmq_admin.AdminAPI."""

import json
import urllib

import rabbitmq_admin
import requests


class ExtendedAdminApi(rabbitmq_admin.AdminAPI):
    """Extend rabbitmq_admin.AdminAPI to cover missing endpoints the charm needs."""

    def get_cluster_name(self) -> str | None:
        """Return the RabbitMQ cluster name."""
        return self._api_get("/api/overview").get("cluster_name")

    def set_cluster_name(self, cluster_name: str) -> None:
        """Set the RabbitMQ cluster name."""
        self._api_put(
            "/api/global-parameters/cluster_name",
            data={"value": cluster_name},
        )

    def list_queues(self) -> list[dict]:
        """A list of queues."""
        return self._api_get("/api/queues")

    def list_quorum_queues(self) -> list[dict]:
        """A list of quorum queues."""
        return [
            q for q in self._api_get("/api/queues") if q["type"] == "quorum"
        ]

    def get_queue(self, vhost: str, queue: str) -> dict:
        """Return details for a specific queue."""
        return self._api_get(
            "/api/queues/{}/{}".format(
                urllib.parse.quote_plus(vhost), urllib.parse.quote_plus(queue)
            )
        )

    def rebalance_queues(self) -> None:
        """Rebalance the queues leaders."""
        self._api_post("/api/rebalance/queues")

    def grow_queue(
        self,
        node: str,
        selector: str,
        vhost_pattern: str | None = None,
        queue_pattern: str | None = None,
    ) -> None:
        """Add a member to queues.

        Which queues have the member added is decided by the selector,
        vhost_pattern and queue_pattern
        """
        if not vhost_pattern:
            vhost_pattern = ".*"
        if not queue_pattern:
            queue_pattern = ".*"
        data = {
            "strategy": selector,
            "queue_pattern": queue_pattern,
            "vhost_pattern": vhost_pattern,
        }
        self._api_post(
            "/api/queues/quorum/replicas/on/{}/grow".format(node), data=data
        )

    def shrink_queue(self, node):
        """Remove all quorum queue replicas from a node.

        Returns the API response dict with 'deleted' and 'errors' keys.
        Note: _api_delete does not return the response body, so we
        bypass it and call requests.delete() directly.
        """
        response = requests.delete(
            self.url + "/api/queues/quorum/replicas/on/{}/shrink".format(node),
            auth=self.auth,
            headers=self.headers,
        )
        response.raise_for_status()
        return response.json()

    def add_member(self, node: str, vhost: str, queue: str) -> None:
        """Add a member to a queue."""
        data = {"node": node}
        self._api_post(
            "/api/queues/quorum/{}/{}/replicas/add".format(
                urllib.parse.quote_plus(vhost), urllib.parse.quote_plus(queue)
            ),
            data=data,
        )

    def delete_member(self, node: str, vhost: str, queue: str) -> None:
        """Remove a member from a queue."""
        # rabbitmq_admin does not seem to handle json encoding for DELETE requests
        data = json.dumps({"node": node})
        self._api_delete(
            "/api/queues/quorum/{}/{}/replicas/delete".format(
                urllib.parse.quote_plus(vhost), urllib.parse.quote_plus(queue)
            ),
            data=data,
        )
