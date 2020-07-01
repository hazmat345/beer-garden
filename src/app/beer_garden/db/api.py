# -*- coding: utf-8 -*-
import beer_garden

api = None

try:
    database_type = beer_garden.config.get("db.connection.type")
    if database_type:
        if database_type.lower() == "mongo":
            import beer_garden.db.mongo.api as api
        else:
            import beer_garden.db.sql.api as api
except TypeError:
    raise

if api:
    check_connection = api.check_connection
    create_connection = api.create_connection
    initial_setup = api.initial_setup

    get_pruner = api.get_pruner
    prune_tasks = api.prune_tasks

    get_job_store = api.get_job_store

    count = api.count
    query_unique = api.query_unique
    query = api.query
    reload = api.reload
    replace_commands = api.replace_commands
    distinct = api.distinct

    create = api.create
    update = api.update
    delete = api.delete
