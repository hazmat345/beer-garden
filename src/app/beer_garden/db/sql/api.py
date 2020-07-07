from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import MultipleResultsFound

import brewtils.models
import logging
from box import Box

from sqlalchemy import create_engine, inspect

from beer_garden.db.sql.parser import SqlParser
from beer_garden.db.sql.pruner import SqlPruner
from beer_garden.db.sql.util import ensure_roles, ensure_users
from brewtils.models import BaseModel
from brewtils.schema_parser import SchemaParser
import beer_garden.db.sql.models as sqlModels

from typing import List, Optional, Type, Union, Tuple

import beer_garden.db.sql.models

logger = logging.getLogger(__name__)

ModelType = Union[
    Type[brewtils.models.Command],
    Type[brewtils.models.Instance],
    Type[brewtils.models.Job],
    Type[brewtils.models.Request],
    Type[brewtils.models.RequestTemplate],
    Type[brewtils.models.System],
    Type[brewtils.models.Garden],
]

ModelItem = Union[
    brewtils.models.Command,
    brewtils.models.Instance,
    brewtils.models.Job,
    brewtils.models.Request,
    brewtils.models.RequestTemplate,
    brewtils.models.System,
    brewtils.models.Garden,
]

engine = None
Session = None

_model_map = {}
for model_name in beer_garden.db.sql.models.__all__:
    sql_class = getattr(beer_garden.db.sql.models, model_name)
    _model_map[sql_class.brewtils_model] = sql_class


def create_session():
    return Session()


def to_brewtils_fields(obj):

    if not isinstance(obj, dict):
        table = inspect(obj)
        obj = inspect(obj).dict

        columns = list()

        for column in table.mapper.columns:
            columns.append(column.key)

        for column in table.mapper.relationships:
            columns.append(column.key)

        remove_keys = list()

        for key in obj:
            if key not in columns:
                remove_keys.append(key)
        for key in remove_keys:
            obj.pop(key)

    for key in beer_garden.db.sql.models.restricted_field_mapping:
        if key in obj:
            obj[beer_garden.db.sql.models.restricted_field_mapping[key]] = obj[key]
            obj.pop(key, None)

    if "id" in obj and isinstance(obj["id"], int):
        obj["id"] = str(obj["id"])

    new_obj = dict()
    for key in obj:
        if isinstance(obj[key], list):
            items = list()

            for item in obj[key]:
                items.append(to_brewtils_fields(item))

            new_obj[key] = items

        elif isinstance(obj[key], dict):
            new_obj[key] = to_brewtils_fields(obj[key])
        else:
            new_obj[key] = obj[key]

    return new_obj


def from_brewtils_fields(obj, SqlModel):
    for key in beer_garden.db.sql.models.restricted_field_mapping:
        if beer_garden.db.sql.models.restricted_field_mapping[key] in obj:
            obj[key] = obj[beer_garden.db.sql.models.restricted_field_mapping[key]]
            obj.pop(beer_garden.db.sql.models.restricted_field_mapping[key], None)

    if "id" in obj and isinstance(obj["id"], str):
        obj["id"] = int(obj["id"])

    newObj = dict()
    for key in obj:

        if key in SqlModel.__mapper__.relationships.keys():
            if obj[key] is not None:
                column = SqlModel.__mapper__.relationships[key]
                if isinstance(obj[key], list):
                    if len(obj[key]) == 0:
                        newObj[key] = []
                    else:
                        obj_list = list()
                        for item in obj[key]:
                            obj_list.append(
                                from_brewtils_fields(item, column.entity.class_)
                            )
                        newObj[key] = obj_list
                else:
                    obj[key] = from_brewtils_fields(obj[key], column.entity.class_)
            elif SqlModel.__mapper__.relationships[key].uselist:
                newObj[key] = []

        elif key in SqlModel.__mapper__.columns.keys():
            newObj[key] = obj[key]
        # else:
        # obj.pop(key, None)
    try:
        return SqlModel(**newObj)
    except Exception as e:
        raise


def from_brewtils(obj: ModelItem):
    """Convert an item from its Brewtils model to its  one

    Args:
        obj: The Brewtils model item

    Returns:
        The Mongo model item

    """
    model_dict = SchemaParser.serialize(obj, to_string=False)

    sql_obj = from_brewtils_fields(model_dict, _model_map[type(obj)])
    return sql_obj


def object_as_dict(obj):
    return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}


def to_brewtils(obj) -> Union[ModelItem, List[ModelItem]]:
    """Convert an item from its Mongo model to its Brewtils one

    Args:
        obj: The Mongo model item

    Returns:
        The Brewtils model item

    """
    if obj is None or (isinstance(obj, list) and len(obj) == 0):
        return obj

    if isinstance(obj, list):
        results = list()

        for item in obj:
            results.append(to_brewtils_fields(item))

        if len(results) == 0:
            serialized = None
        else:
            serialized = results
    else:
        serialized = to_brewtils_fields(obj)

    many = True if isinstance(serialized, list) else False
    if many:
        model_class = obj[0].brewtils_model
    else:
        model_class = obj.brewtils_model

    return SchemaParser.parse(serialized, model_class, from_string=False, many=many)


def check_connection(db_config: Box):
    """Check connectivity to a SQL database

    Args:
        db_config: Yapconf-generated configuration object

    Returns:
        bool: True if successful, False otherwise (unable to connect)

    Raises:

    """
    # check_engine = create_engine(
    #     "{dialect}{driver}://{username}{password}{host}{port}{database}".
    #         format(dialect=db_config["connection"]["dialect"],
    #                driver=db_config["connection"]["driver"] if "+" + db_config["connection"]["driver"] else "",
    #                username=db_config["connection"]["username"] if db_config["connection"]["username"] else "",
    #                password=db_config["connection"]["password"] if ":" + db_config["connection"]["password"] else "",
    #                host=db_config["connection"]["host"] if "@" + db_config["connection"]["host"] else "",
    #                port=db_config["connection"]["port"] if ":" + db_config["connection"]["port"] else "",
    #                database=db_config["connection"]["database"] if "/" + db_config["connection"]["database"] else "", ))

    if engine:
        return not engine.closed
    else:
        return True


def create_connection(connection_alias: str = "default", db_config: Box = None) -> None:
    """Register a database connection

    Args:
        connection_alias: Alias for this connection
        db_config: Yapconf-generated configuration object

    Returns:
        None
    """
    # If the name of the database is memory, then we will assume it in an in memory only db
    # if db_config["name"] == "memory":
    #     db_config["name"] = ":memory:"

    global engine

    engine = create_engine(
        "{dialect}{driver}://{username}{password}{host}{port}{database}".format(
            dialect=str(db_config["type"]),
            driver="+" + str(db_config["driver"]) if db_config["driver"] else "",
            username=str(db_config["connection"]["username"])
            if db_config["connection"]["username"]
            else "",
            password=":" + str(db_config["connection"]["password"])
            if db_config["connection"]["password"]
            else "",
            host="@" + str(db_config["connection"]["host"])
            if db_config["connection"]["host"]
            else "",
            port=":" + str(db_config["connection"]["port"])
            if db_config["connection"]["port"]
            else "",
            database="/" + str(db_config["name"]) if db_config["name"] else "",
        ),
        pool_pre_ping=True,
        echo=False,
    )

    global Session

    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    sqlModels.Base.metadata.create_all(engine)


def initial_setup(guest_login_enabled):
    """Do everything necessary to ensure the database is in a 'good' state"""

    ensure_roles()
    ensure_users(guest_login_enabled)


def get_pruner():
    return SqlPruner


def prune_tasks(**kwargs) -> Tuple[List[dict], int]:
    return SqlPruner.determine_tasks(**kwargs)


def get_job_store():
    from beer_garden.db.sql.jobstore import SqlJobStore

    return SqlJobStore()


def count(model_class: ModelType, **kwargs) -> int:
    """Count the number of items matching a query

    Args:
        model_class: The Brewtils model class to query for
        **kwargs: Arguments to control the query. Equivalent to 'filter_params' from the
            'query' function.

    Returns:
        The number of items

    """
    session = Session()
    query_set = session.query(_model_map[model_class])

    for k, v in kwargs.items():
        column = getattr(_model_map[model_class], k, None)
        if column:
            if isinstance(v, BaseModel):
                query_set = query_set.filter(
                    column.has(_model_map[v.schema].id == v.id)
                )
            else:
                query_set = query_set.filter(column == v)

    return query_set.count()


def query_unique(
    model_class: ModelType, raise_missing=False, **kwargs
) -> Optional[ModelItem]:
    """Query a collection for a unique item

    This will search a collection for a single specific item.

    If no item matching the kwarg parameters is found:
    - Will return None if raise_missing=False
    - Will raise a DoesNotExist exception if raise_missing=True

    If more than one item matching is found a MultipleObjectsReturned will be raised.

    Args:
        model_class: The Brewtils model class to query for
        raise_missing: If True, raise an exception if an item matching the query is not
            found. If False, will return None in that case.
        **kwargs: Arguments to control the query. Equivalent to 'filter_params' from the
            'query' function.

    Returns:
        A single Brewtils model

    Raises:


    """
    try:
        session = Session()
        query_set = session.query(_model_map[model_class])

        for k, v in kwargs.items():
            column = getattr(_model_map[model_class], k, None)
            if column:
                if isinstance(v, BaseModel):
                    query_set = query_set.filter(
                        column.has(_model_map[v.schema].id == v.id)
                    )
                else:
                    query_set = query_set.filter(column == v)
            elif "__" in k:
                # instances__contains
                field, operations = k.split("__")
                column = getattr(_model_map[model_class], field, None)

                if column:
                    if isinstance(v, BaseModel):
                        query_set = query_set.filter(
                            column.has(_model_map[v.schema].id == v.id)
                        )
                    else:
                        query_set = query_set.filter(column == v)

        return to_brewtils(query_set.one_or_none())
    except MultipleResultsFound:
        if raise_missing:
            raise
        return None


def query(model_class: ModelType, **kwargs) -> List[ModelItem]:
    """Query a collection

    It's possible to specify `include_fields` _and_ `exclude_fields`. This doesn't make
    a lot of sense, but you can do it. If the same field is in both `exclude_fields`
    takes priority (the field will NOT be included in the response).

    Args:
        model_class: The Brewtils model class to query for
        **kwargs: Arguments to control the query. Valid options are:
            filter_params: Dict of filtering parameters
            order_by: Field that will be used to order the result list
            include_fields: Model fields to include
            exclude_fields: Model fields to exclude
            dereference_nested: Flag specifying if related models should be fetched
            text_search: A text search parameter
            hint: A hint specifying the index to use (cannot be used with text_search)
            start: Slicing start
            length: Slicing count

    Returns:
        A list of Brewtils models

    """
    session = Session()
    query_set = session.query(_model_map[model_class])

    if kwargs.get("filter_params"):
        filter_params = kwargs["filter_params"]

        # If any values are brewtils models those need to be converted
        for key in filter_params:
            column = getattr(_model_map[model_class], key, None)
            if column:
                if isinstance(filter_params[key], BaseModel):
                    query_set = query_set.filter(
                        column.has(
                            _model_map[filter_params[key].schema].id
                            == filter_params[key].id
                        )
                    )
                else:
                    query_set = query_set.filter(column == filter_params[key])

    # Bad things happen if you try to use a hint with a text search.
    # if kwargs.get("text_search"):
    #     query_set = query_set.search_text(kwargs.get("text_search"))
    # elif kwargs.get("hint"):
    #     # Sanity check - if index is 'bad' just let mongo deal with it
    #     if kwargs.get("hint") in _model_map[model_class].index_names():
    #         query_set = query_set.hint(kwargs.get("hint"))

    # Done
    if kwargs.get("order_by"):
        column = getattr(_model_map[model_class], kwargs.get("order_by"), None)
        query_set = query_set.order_by(column)

    # Done
    if kwargs.get("include_fields"):
        include_fields = list()
        for field in kwargs.get("include_fields"):
            column = getattr(_model_map[model_class], field, None)
            include_fields.append(column)
        query_set = query_set.with_entities(*include_fields)

    # Done
    if kwargs.get("exclude_fields"):
        include_fields = list()
        for column in _model_map[model_class].__table__.columns:
            if column.key not in kwargs.get("exclude_fields"):
                include_fields.append(column)
        query_set = query_set.with_entities(*include_fields)

    # if not kwargs.get("dereference_nested", True):
    #    query_set = query_set.no_dereference()

    # Done
    if kwargs.get("start"):
        query_set = query_set.offset(int(kwargs.get("start")))

    # Done
    if kwargs.get("length"):
        query_set = query_set.limit(int(kwargs.get("length")))

    return [] if query_set.count() == 0 else to_brewtils(query_set.all())


def create(obj: ModelItem) -> ModelItem:
    """Save a new item to the database

    Args:
        obj: The Brewtils model to save

    Returns:
        The saved Brewtils model

    """
    sql_obj = from_brewtils(obj)

    session = Session()
    session.add(sql_obj)
    session.commit()

    # Refreshing Object to ensure it is the latest
    session.refresh(sql_obj)

    brewtils_obj = to_brewtils(sql_obj)

    return brewtils_obj


def update(obj: ModelItem) -> ModelItem:
    """Save changes to an item to the database

    This is identical to the "create" function

    Args:
        obj: The Brewtils model to save

    Returns:
        The saved Brewtils model

    """
    return create(obj)


def delete(obj: ModelItem) -> None:
    """Delete an item from the database

    If the Mongo model corresponding to the Brewtils model has a "deep_delete" method
    then that will be called. Otherwise the normal "delete" will be used.

    Args:
        obj: The Brewtils model to delete

    Returns:
        None

    """
    sql_obj = from_brewtils(obj)

    session = Session()
    session.delete(sql_obj)
    session.commit()


def reload(obj: ModelItem) -> ModelItem:
    """Reload an item from the database

    Args:
        obj: The Brewtils model to reload

    Returns:
        The updated Brewtils model

    """

    session = Session()
    existing_obj = session.query(_model_map[type(obj)]).get(int(obj.id)).first()

    return to_brewtils(existing_obj)


def replace_commands(
    system: brewtils.models.System, new_commands: List[brewtils.models.Command]
) -> brewtils.models.System:
    """Replaces a System's Commands

    Assumes the commands passed in are more important than what currently exists in the
    database. It will delete commands that are not part of `new_commands`.

    This calls the Mongo object methods directly to avoid problems with translating the
    Command.system field.

    Args:
        system: System to update
        new_commands: List of new commands

    Returns:
        The updated Brewtils System
    """
    return update(system)


def distinct(brewtils_class: ModelItem, field: str) -> List:
    session = Session()

    query_set = (
        session.query(_model_map[brewtils_class])
        .with_entities(getattr(_model_map[brewtils_class], field, None))
        .distinct()
    )

    results = []

    for result in query_set:
        results.append(getattr(result, field))

    return results
