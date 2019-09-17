import logging

from pyrabbit2.api import Client
from pyrabbit2.http import HTTPError, NetworkError

import beer_garden
from beer_garden.requests import cancel_request
from beer_garden.bg_utils.mongo.parser import MongoParser
from brewtils.pika import TransientPikaClient
from brewtils.schema_parser import SchemaParser


def get_routing_key(*args, **kwargs):
    """Convenience method for getting the most specific routing key"""
    return get_routing_keys(*args, **kwargs)[-1]


def get_routing_keys(*args, **kwargs):
    """Get a list of routing keys for a plugin in order from least specific to most specific.

    Will return all possible routing keys to get a message to a particular system.

    args is used to specify routing words. The correct order is
       System, Version, Instance, Clone ID

    For instance:

        ['test_system'], is_admin=True:
            ['admin', 'admin.test_system']

        ['test_system', '1.0.0'], is_admin=True:
            ['admin', 'admin.test_system', 'admin.test_system.1-0-0']

        ['test_system', '1.0.0', 'default'], is_admin=True:
            ['admin', 'admin.test_system', 'admin.test_system.1-0-0',
                'admin.test_system.1-0-0.default']

        ['test_system', '1.0.0', 'default', 'random_text'], is_admin=True:
            ['admin', 'admin.test_system', 'admin.test_system.1-0-0',
                'admin.test_system.1-0-0.default', 'admin.test_system.1-0-0.default.random_text']

    NOTE: Because RabbitMQ uses '.' as the word delimiter all '.' in routing words will be
        replaced with '-'

    :param args: List of routing key words to include in the routing keys
    :param kwargs: is_admin: Will prepend 'admin' to all generated keys if True
    :return: List of routing keys, ordered from general to specific
    """
    routing_keys = ["admin"] if kwargs.get("is_admin", False) else []

    for arg in (y for y in args if y is not None):
        # Make sure we don't have any extra word delimiters
        new_key = arg.replace(".", "-")

        routing_keys.append(
            routing_keys[-1] + "." + new_key if len(routing_keys) else new_key
        )

    return routing_keys


class PikaClient(TransientPikaClient):
    """Pika client that exposes additional Bartender-specific operations"""

    def publish_request(self, request, **kwargs):
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        if request.id:
            kwargs["headers"]["request_id"] = str(request.id)

        if "routing_key" not in kwargs:
            kwargs["routing_key"] = get_routing_key(
                request.system, request.system_version, request.instance_name
            )

        return self.publish(SchemaParser.serialize_request(request), **kwargs)


class PyrabbitClient(object):
    """Class that implements a connection to RabbitMQ Management HTTP API"""

    def __init__(
        self,
        host="localhost",
        port=15672,
        user="guest",
        password="guest",
        virtual_host="/",
        ssl=None,
        admin_expires=None,
    ):
        self.logger = logging.getLogger(__name__)
        self._admin_expires = admin_expires

        # Pyrabbit won't infer the default virtual host ('/'). So we need to enforce it
        self._virtual_host = virtual_host or "/"

        ssl = ssl or {}
        verify = ssl.get("ca_cert", True) if ssl.get("ca_verify") else False

        # The client for doing Admin things over the HTTP API
        self._client = Client(
            "%s:%s" % (host, port),
            user,
            password,
            scheme="https" if ssl.get("enabled") else "http",
            verify=verify,
            cert=ssl.get("client_cert"),
        )

    def is_alive(self):
        try:
            return self._client.is_alive()
        except NetworkError:
            return False

    def verify_virtual_host(self):
        """Ensure the virtual host exists"""
        try:
            return self._client.get_vhost(self._virtual_host)
        except Exception:
            self.logger.error(
                "Error verifying virtual host %s, does it exist?", self._virtual_host
            )
            raise

    def ensure_admin_expiry(self):
        """Ensure that the admin queue expiration policy exists"""
        try:
            kwargs = {
                "pattern": "^admin.*",
                "definition": {"expires": self._admin_expires},
                "priority": 1,
                "apply-to": "queues",
            }
            self._client.create_policy(self._virtual_host, "admin_expiry", **kwargs)
        except Exception:
            self.logger.error("Error creating admin queue expiration policy")
            raise

    def get_queue_size(self, queue_name):
        """Get the number of messages in a queue.

        :param queue_name: The name of the queue
        :return: The number of messages in the queue
        """
        self.logger.debug("Getting queue Size for: %s", queue_name)
        try:
            return self._client.get_queue(self._virtual_host, queue_name).get(
                "messages", 0
            )
        except HTTPError as ex:
            if ex.status == 404:
                self.logger.error("Queue '%s' could not be found", queue_name)
            else:
                self.logger.error("Could not connect to queue '%s'", queue_name)
            raise ex

    def clear_queue(self, queue_name):
        """Remove all messages in a queue.

        :param queue_name: The name of the queue
        :return:None
        """
        self.logger.info("Clearing Queue: %s", queue_name)

        queue_dictionary = self._client.get_queue(self._virtual_host, queue_name)
        number_of_messages = queue_dictionary.get("messages_ready", 0)

        while number_of_messages > 0:
            self.logger.debug("Getting the Next Message")
            messages = self._client.get_messages(
                self._virtual_host, queue_name, count=1, requeue=False
            )
            if messages and len(messages) > 0:
                message = messages[0]
                try:
                    request = MongoParser.parse_request(
                        message["payload"], from_string=True
                    )

                    self.logger.debug(f"Canceling Request {request.id}")
                    cancel_request(request)
                except Exception as ex:
                    self.logger.exception(f"Error canceling message: {ex}")
            else:
                self.logger.debug(
                    "Race condition: The while loop thought there were more messages "
                    "to ingest but no more messages could be received."
                )
                break

            number_of_messages -= 1

    def delete_queue(self, queue_name):
        """Actually remove a queue.

        :param queue_name: The name of the queue
        :return:
        """
        self._client.delete_queue(self._virtual_host, queue_name)

    def destroy_queue(self, queue_name, force_disconnect=False):
        """Remove all remnants of a queue.

        Ignores exceptions and ensures all aspects of the queue are deleted.

        :param queue_name: The queue name
        :param force_disconnect: Attempt to forcefully disconnect consumers of this queue
        :return:
        """
        if queue_name is None:
            return

        if force_disconnect:
            try:
                self.disconnect_consumers(queue_name)
            except HTTPError as ex:
                if ex.status != 404:
                    self.logger.exception(ex)
            except Exception as ex:
                self.logger.exception(ex)

        try:
            self.clear_queue(queue_name)
        except HTTPError as ex:
            if ex.status != 404:
                self.logger.exception(ex)
        except Exception as ex:
            self.logger.exception(ex)

        try:
            self.delete_queue(queue_name)
        except HTTPError as ex:
            if ex.status != 404:
                self.logger.exception(ex)
        except Exception as ex:
            self.logger.exception(ex)

    def disconnect_consumers(self, queue_name):
        # If there are no channels, then there is nothing to do
        channels = self._client.get_channels() or []
        for channel in channels:

            # If the channel is already gone, just return an empty response
            channel_details = self._client.get_channel(channel["name"]) or {
                "consumer_details": []
            }

            for consumer_details in channel_details["consumer_details"]:
                if queue_name == consumer_details["queue"]["name"]:
                    self._client.delete_connection(
                        consumer_details["channel_details"]["connection_name"]
                    )
