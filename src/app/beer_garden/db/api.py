# -*- coding: utf-8 -*-
import beer_garden.db.mongo.api
import beer_garden.db.sql.api

# check_connection = None
# create_connection = None
# initial_setup = None
#
# get_pruner = None
# prune_tasks = None
#
# get_job_store = None
#
# count = None
# query_unique = None
# query = None
# reload = None
# replace_commands = None
# distinct = None
#
# create = None
# update = None
# delete = None

check_connection = beer_garden.db.sql.api.check_connection
create_connection = beer_garden.db.sql.api.create_connection
initial_setup = beer_garden.db.sql.api.initial_setup

get_pruner = beer_garden.db.sql.api.get_pruner
prune_tasks = beer_garden.db.sql.api.prune_tasks

get_job_store = beer_garden.db.sql.api.get_job_store

count = beer_garden.db.sql.api.count
query_unique = beer_garden.db.sql.api.query_unique
query = beer_garden.db.sql.api.query
reload = beer_garden.db.sql.api.reload
replace_commands = beer_garden.db.sql.api.replace_commands
distinct = beer_garden.db.sql.api.distinct

create = beer_garden.db.sql.api.create
update = beer_garden.db.sql.api.update
delete = beer_garden.db.sql.api.delete

# check_connection = beer_garden.db.mongo.api.check_connection
# create_connection = beer_garden.db.mongo.api.create_connection
# initial_setup = beer_garden.db.mongo.api.initial_setup
#
# get_pruner = beer_garden.db.mongo.api.get_pruner
# prune_tasks = beer_garden.db.mongo.api.prune_tasks
#
# get_job_store = beer_garden.db.mongo.api.get_job_store
#
# count = beer_garden.db.mongo.api.count
# query_unique = beer_garden.db.mongo.api.query_unique
# query = beer_garden.db.mongo.api.query
# reload = beer_garden.db.mongo.api.reload
# replace_commands = beer_garden.db.mongo.api.replace_commands
# distinct = beer_garden.db.mongo.api.distinct
#
# create = beer_garden.db.mongo.api.create
# update = beer_garden.db.mongo.api.update
# delete = beer_garden.db.mongo.api.delete
