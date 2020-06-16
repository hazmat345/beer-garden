# -*- coding: utf-8 -*-
from copy import copy

from brewtils.schema_parser import SchemaParser

import beer_garden.db.sql.models


class SqlParser(SchemaParser):
    """Class responsible for converting JSON into SQL-backed objects."""

    _models = copy(SchemaParser._models)
    _models.update(beer_garden.db.sql.models.schema_mapping)

    @classmethod
    def _get_schema_name(cls, model):
        if isinstance(model, beer_garden.db.sql.models.Base):
            return model.brewtils_model.schema
        else:
            return super(SqlParser, cls)._get_schema_name(model)
