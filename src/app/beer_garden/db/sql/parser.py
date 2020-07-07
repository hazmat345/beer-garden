# -*- coding: utf-8 -*-
from copy import copy

from sqlalchemy import inspect

from brewtils.schema_parser import SchemaParser

import beer_garden.db.sql.models


class SqlParser(SchemaParser):
    _models = copy(SchemaParser._models)
    _models.update(
        {
            "SystemSchema": beer_garden.db.sql.models.System,
            "InstanceSchema": beer_garden.db.sql.models.Instance,
            "CommandSchema": beer_garden.db.sql.models.Command,
            "ParameterSchema": beer_garden.db.sql.models.Parameter,
            "RequestSchema": beer_garden.db.sql.models.Request,
            "RequestTemplateSchema": beer_garden.db.sql.models.RequestTemplate,
            "ChoicesSchema": beer_garden.db.sql.models.Choices,
            "EventSchema": beer_garden.db.sql.models.Event,
            "PrincipalSchema": beer_garden.db.sql.models.Principal,
            "RoleSchema": beer_garden.db.sql.models.Role,
            "RefreshTokenSchema": beer_garden.db.sql.models.RefreshToken,
            "JobSchema": beer_garden.db.sql.models.Job,
            "GardenSchema": beer_garden.db.sql.models.Garden,
        }
    )

    def object_as_dict(self, obj):
        return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

    def serialize(self, query):
        results = list()

        for obj in query:
            model_dict = self.object_as_dict(obj)

            for key in beer_garden.db.sql.restricted_field_mapping:
                if key in model_dict:
                    model_dict[
                        beer_garden.db.sql.restricted_field_mapping[key]
                    ] = model_dict[key]
                    model_dict.pop(key, None)

            results.append(model_dict)

        if len(results) == 0:
            return None
        elif len(results) == 1:
            return results.pop()
        else:
            return results
