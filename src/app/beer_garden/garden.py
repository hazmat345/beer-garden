# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from typing import Dict, List

from brewtils.errors import PluginError
from brewtils.models import Events, Garden, System

import beer_garden.config as config
import beer_garden.db.api as db
from beer_garden.events import publish_event
from beer_garden.namespace import get_namespaces
from beer_garden.systems import get_systems

logger = logging.getLogger(__name__)

# Dict of garden_name -> garden
garden_cache: Dict[str, Garden] = {}


def get_garden(garden_name: str) -> Garden:
    """Retrieve an individual Garden

    Args:
        garden_name: The name of Garden

    Returns:
        The Garden

    """
    if garden_name == config.get("garden.name"):
        return local_garden()

    return db.query_unique(Garden, name=garden_name)


def get_gardens(include_local: bool = True) -> List[Garden]:
    """Retrieve list of all Gardens

    Returns:
        All known gardens

    """
    gardens = db.query(Garden)

    if include_local:
        gardens.append(local_garden())

    return gardens


def local_garden() -> Garden:
    """Get the local garden definition

    Returns:
        The local Garden

    """
    return Garden(
        name=config.get("garden.name"),
        connection_type="LOCAL",
        status="RUNNING",
        systems=get_systems(filter_params={"local": True}),
        namespaces=get_namespaces(),
    )


def update_garden_config(garden: Garden):
    db_garden = db.query_unique(Garden, id=garden.id)
    db_garden.connection_params = garden.connection_params
    db_garden.connection_type = garden.connection_type
    db_garden = db.update(db_garden)

    return update_cache(db_garden)


def update_garden_status(garden_name: str, new_status: str) -> Garden:
    """Update an Garden status.

    Will also update the status_info heartbeat.

    Args:
        garden_name: The Garden Name
        new_status: The new status

    Returns:
        The updated Garden
    """
    garden = db.query_unique(Garden, name=garden_name)
    garden.status = new_status
    garden.status_info["heartbeat"] = datetime.utcnow()
    garden = db.update(garden)

    return update_cache(garden)


@publish_event(Events.GARDEN_REMOVED)
def remove_garden(garden_name: str) -> None:
    """Remove a garden

        Args:
            garden_name: The Garden name

        Returns:
            None

        """
    garden = db.query_unique(Garden, name=garden_name)
    db.delete(garden)

    del garden_cache[garden_name]

    return garden


@publish_event(Events.GARDEN_CREATED)
def create_garden(garden: Garden) -> Garden:
    """Create a new Garden

    Args:
        garden: The Garden to create

    Returns:
        The created Garden

    """
    garden.status_info["heartbeat"] = datetime.utcnow()

    return db.create(garden)


# def add_system(system: System, garden_name: str):
#     garden = get_garden(garden_name)
#
#     if garden is None:
#         raise PluginError(
#             f"Garden '{garden_name}' does not exist, unable to map '{str(system)}"
#         )
#
#     if system.namespace not in garden.namespaces:
#         garden.namespaces.append(system.namespace)
#
#     if str(system) not in garden.systems:
#         garden.systems.append(str(system))
#
#     return update_garden(garden)
#
#
# def update_system(system: System, garden_name: str):
#     garden = get_garden(garden_name)
#
#     if garden is None:
#         raise PluginError(
#             f"Garden '{garden_name}' does not exist, unable to map '{str(system)}"
#         )
#
#     if system.namespace not in garden.namespaces:
#         garden.namespaces.append(system.namespace)
#
#     if str(system) not in garden.systems:
#         garden.systems.append(str(system))
#
#     return update_garden(garden)
#
#
# def remove_system(system: System, garden_name: str):
#     garden = get_garden(garden_name)
#
#     if garden is None:
#         raise PluginError(
#             f"Garden '{garden_name}' does not exist, unable to map '{str(system)}"
#         )
#
#     if system.namespace not in garden.namespaces:
#         garden.namespaces.append(system.namespace)
#
#     if str(system) not in garden.systems:
#         garden.systems.append(str(system))
#
#     return update_garden(garden)


@publish_event(Events.GARDEN_UPDATED)
def update_cache(garden: Garden):
    garden_cache[garden.name] = garden
    return garden

    # return db.update(garden)


def setup_garden_cache():
    """Initialize the routing subsystem

    This will load the cached child garden definitions and use them to populate the
    two dictionaries that matter, garden_lookup and garden_connections.

    It will then query the database for all local systems and add those to the
    dictionaries as well.
    """
    local_garden_name = config.get("garden.name")

    # We do NOT want to load local garden information from the database as the local
    # name could have changed
    for garden in get_gardens():
        if garden.name != local_garden_name:
            if (
                garden.connection_type is not None
                and garden.connection_type.casefold() != "local"
            ):
                garden_cache[garden.name] = garden
            else:
                logger.warning(f"Garden with invalid connection info: {garden!r}")

    # Now add the "local" garden
    garden_cache[local_garden_name] = local_garden()

    logger.debug("Garden cache setup complete")


def handle_event(event):
    """Handle garden-related events

    For GARDEN events we only care about events originating from downstream. We also
    only care about immediate children, not grandchildren.

    Whenever a garden event is detected we should update that garden's database
    representation.

    This method should NOT update the routing module. Let its handler worry about that!
    """
    # Here we want to handle system changes from THIS garden
    # The system module will handle updating the system collection in the DB
    # So this only needs to update the garden cache for the local garden
    if event.garden == config.get("garden.name"):
        if event.name in (
            Events.SYSTEM_CREATED.name,
            Events.SYSTEM_UPDATED.name,
            Events.SYSTEM_REMOVED.name,
        ):
            existing_garden = garden_cache.get(event.garden)
            if not existing_garden:
                raise Exception("Uhh, looks like the local garden isn't in the cache")

            index = None
            for i, system in enumerate(garden_cache[event.garden].systems):
                if system.id == event.payload.id:
                    index = i
                    break

            if index is not None:
                garden_cache[event.garden].systems.pop(index)

            if event.name in (
                Events.SYSTEM_CREATED.name,
                Events.SYSTEM_UPDATED.name,
            ):
                garden_cache[event.garden].systems.append(event.payload)

    if event.garden != config.get("garden.name"):
        if event.name in (
            Events.GARDEN_STARTED.name,
            Events.GARDEN_UPDATED.name,
            Events.GARDEN_STOPPED.name,
        ):
            # Only do stuff for direct children
            if event.payload.name == event.garden:
                existing_garden = get_garden(event.garden)

                if existing_garden is None:
                    event.payload.connection_type = None
                    event.payload.connection_params = {}

                    for system in event.payload.systems:
                        system.local = False

                    created = create_garden(event.payload)

                    update_cache(created)
                else:
                    for attr in ("status", "status_info", "namespaces", "systems"):
                        setattr(existing_garden, attr, getattr(event.payload, attr))

                    for system in existing_garden.systems:
                        system.local = False

                    updated = db.update(existing_garden)

                    update_cache(updated)
                    # update_garden(existing_garden)

        # # Handle system changes gardens
        # if event.name in (
        #     Events.SYSTEM_CREATED.name,
        #     Events.SYSTEM_UPDATED.name,
        #     Events.SYSTEM_REMOVED.name,
        # ):
        #     if event.garden in garden_cache:
        #         index = None
        #         for i, system in enumerate(garden_cache[event.garden].systems):
        #             if system.id == event.payload.id:
        #                 index = i
        #                 break
        #
        #         if index is not None:
        #             garden_cache[event.garden].systems.pop(index)
        #
        #         if event.name in (
        #             Events.SYSTEM_CREATED.name,
        #             Events.SYSTEM_UPDATED.name,
        #         ):
        #             garden_cache[event.garden].systems.append(event.payload)
