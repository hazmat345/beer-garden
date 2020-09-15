# -*- coding: utf-8 -*-
import copy
import logging
from typing import List, Sequence

from brewtils.errors import BrewtilsException, ModelValidationError
from brewtils.models import Command, Event, Events, Instance, System
from brewtils.schemas import SystemSchema
from time import sleep

import beer_garden.config as config
import beer_garden.db.api as db
import beer_garden.queue.api as queue
from beer_garden.errors import NotFoundException
from beer_garden.events import publish_event
from beer_garden.plugin import stop

REQUEST_FIELDS = set(SystemSchema.get_attribute_names())

logger = logging.getLogger(__name__)


def get_system(system_id: str) -> System:
    """Retrieve an individual System

    Args:
        system_id: The System ID

    Returns:
        The System

    """
    return db.query_unique(System, id=system_id)


def get_systems(**kwargs) -> List[System]:
    """Search for Systems

    Keyword Args:
        Parameters to be passed to the DB query

    Returns:
        The list of Systems that matched the query

    """
    return db.query(System, **kwargs)


@publish_event(Events.SYSTEM_CREATED)
def create_system(system: System) -> System:
    """Create a new System

    Args:
        system: The System to create

    Returns:
        The created System

    """
    if system.namespace is None:
        system.namespace = config.get("garden.name")

    # Create in the database
    system = db.create(system)

    # Also need to let the routing module know
    from beer_garden.router import update_routing

    update_routing(update_system=system)

    return system


@publish_event(Events.SYSTEM_UPDATED)
def update_system(
    system_id: str = None,
    system: System = None,
    new_commands: Sequence[Command] = None,
    add_instances: Sequence[Instance] = None,
    description: str = None,
    display_name: str = None,
    icon_name: str = None,
    metadata: dict = None,
) -> System:
    """Update an already existing System

    Args:
        system_id: The ID of the System to be updated
        system: The System to be updated
        new_commands: List of commands to overwrite existing commands
        add_instances: List of new instances that will be added to the current list
        description: Replacement description
        display_name: Replacement display_name
        icon_name: Replacement icon_name
        metadata: Dictionary that will be incorporated into current metadata

    Returns:
        The updated System

    """
    updates = {}
    system = system or db.query_unique(System, id=system_id)

    if new_commands:
        # Convert these to DB form and back to make sure all defaults are correct
        mongo_commands = [db.from_brewtils(command) for command in new_commands]
        brew_commands = db.to_brewtils(mongo_commands)

        if (
            system.commands
            and "dev" not in system.version
            and system.has_different_commands(brew_commands)
        ):
            raise ModelValidationError(
                f"System {system} already exists with different commands"
            )

        updates["commands"] = mongo_commands

    # If we set an attribute to None mongoengine marks that attribute for deletion
    # That's why we explicitly test each of these
    if description:
        updates["description"] = description

    if display_name:
        updates["display_name"] = display_name

    if icon_name:
        updates["icon_name"] = icon_name

    if metadata:
        metadata_update = copy.deepcopy(system.metadata)
        metadata_update.update(metadata)

        updates["metadata"] = metadata_update

    if add_instances:
        if -1 < system.max_instances < len(system.instances) + len(add_instances):
            raise ModelValidationError(
                f"Unable to add instance(s) to {system} - would exceed "
                f"the system instance limit of {system.max_instances}"
            )

        updates["push_all__instances"] = [db.from_brewtils(i) for i in add_instances]

    system = db.modify(system, **updates)

    # Also need to let the routing module know
    from beer_garden.router import update_routing

    update_routing(existing_id=system.id, update_system=system)

    return system


@publish_event(Events.SYSTEM_RELOAD_REQUESTED)
def reload_system(system_id: str = None, system: System = None) -> None:
    """Reload a system configuration

    NOTE: All we do here is grab the system from the database and return it. That's
    because all the work here needs to be done by the local PluginManager, and that
    only exists in the main thread. So we publish an event requesting that the
    appropriate action be taken.

    Args:
        system_id: The System ID
        system: The System

    Returns:
        None
    """
    # TODO - It'd be nice to have a check here to make sure system is managed

    return system or db.query_unique(System, id=system_id)


@publish_event(Events.SYSTEM_REMOVED)
def remove_system(system_id: str = None, system: System = None) -> System:
    """Remove a system

    Args:
        system_id: The System ID
        system: The System

    Returns:
        The removed System

    """
    system = system or db.query_unique(System, id=system_id)

    db.delete(system)

    # Also need to let the routing module know
    from beer_garden.router import update_routing

    update_routing(existing_id=system.id)

    return system


def purge_system(system_id: str = None, system: System = None) -> System:
    """Convenience method for *completely* removing a system

    This will:
    - Stop all instances of the system
    - Remove all message queues associated with the system
    - Remove the system from the database

    Args:
        system_id: The System ID
        system: The System

    Returns:
        The purged system

    """
    system = system or db.query_unique(System, id=system_id)

    # Attempt to stop the plugins
    for instance in system.instances:
        try:
            stop(instance=instance, system=system)
        except Exception as ex:
            logger.warning(
                f"Error while attempting to stop instance {instance.id}: {ex}"
            )

    # TODO - This is not great
    sleep(5)

    system = db.reload(system)

    # Now clean up the message queues. It's possible for the request or admin queue to
    # be none if we are stopping an instance that was not properly started.
    for instance in system.instances:
        force_disconnect = instance.status != "STOPPED"

        request_queue = instance.queue_info.get("request", {}).get("name")
        if request_queue:
            queue.remove(
                request_queue, force_disconnect=force_disconnect, clear_queue=True
            )

        admin_queue = instance.queue_info.get("admin", {}).get("name")
        if admin_queue:
            queue.remove(
                admin_queue, force_disconnect=force_disconnect, clear_queue=False
            )

    # Finally, actually delete the system
    return remove_system(system=system)


@publish_event(Events.SYSTEM_RESCAN_REQUESTED)
def rescan_system_directory() -> None:
    """Scans plugin directory and starts any new Systems"""
    pass


def get_instance(
    instance_id: str = None,
    system_id: str = None,
    instance_name: str = None,
    instance: Instance = None,
    **_,
) -> Instance:
    """Retrieve an individual Instance

    Args:
        instance_id: The Instance ID
        system_id: The System ID
        instance_name: The Instance name
        instance: The Instance

    Returns:
        The Instance

    """
    if instance:
        return instance

    if system_id and instance_name:
        system = db.query_unique(System, raise_missing=True, id=system_id)

        try:
            return system.get_instance_by_name(instance_name, raise_missing=True)
        except BrewtilsException:
            raise NotFoundException(
                f"System {system} does not have an instance with name '{instance_name}'"
            ) from None

    elif instance_id:
        system = db.query_unique(System, raise_missing=True, instances__id=instance_id)

        try:
            return system.get_instance_by_id(instance_id, raise_missing=True)
        except BrewtilsException:
            raise NotFoundException(
                f"System {system} does not have an instance with id '{instance_id}'"
            ) from None

    raise NotFoundException()


def remove_instance(
    *_, system: System = None, instance: Instance = None, **__
) -> Instance:
    """Removes an Instance

    Args:
        system: The System
        instance: The Instance

    Returns:
        The deleted Instance
    """
    db.modify(system, pull__instances=instance)

    return instance


def handle_event(event: Event) -> None:
    """Handle SYSTEM events

    When creating or updating a system, make sure to mark as non-local first.

    It's possible that we see SYSTEM_UPDATED events for systems that we don't currently
    know about. This will happen if a new system is created on the child while the child
    is operating in standalone mode. To handle that, just create the system.

    Args:
        event: The event to handle
    """
    if event.garden != config.get("garden.name"):

        if event.name in (Events.SYSTEM_CREATED.name, Events.SYSTEM_UPDATED.name):
            event.payload.local = False

            if db.count(System, id=event.payload.id):
                db.update(event.payload)
            else:
                db.create(event.payload)

        elif event.name == Events.SYSTEM_REMOVED.name:
            db.delete(event.payload)

        elif event.name == Events.INSTANCE_UPDATED.name:
            if not event.payload_type:
                logger.error(f"{event.name} error: no payload type ({event!r})")
                return

            record = db.query_unique(Instance, id=event.payload.id)

            if record:
                db.update(event.payload)
            else:
                logger.error(f"{event.name} error: object does not exist ({event!r})")
