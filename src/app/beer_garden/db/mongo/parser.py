# -*- coding: utf-8 -*-
from copy import copy

from brewtils.schema_parser import SchemaParser

#import beer_garden.db.mongo.models as bd_models
import beer_garden.db.mongo.new_models as bd_models


class MongoParser(SchemaParser):
    """Class responsible for converting JSON into Mongo-backed objects."""

    _models = copy(SchemaParser._models)
    _models.update(
        {
            "SystemSchema": bd_models.System,
            "InstanceSchema": bd_models.Instance,
            "CommandSchema": bd_models.Command,
            "ParameterSchema": bd_models.Parameter,
            "RequestSchema": bd_models.Request,
            "RequestTemplateSchema": bd_models.RequestTemplate,
            "ChoicesSchema": bd_models.Choices,
            "EventSchema": bd_models.Event,
            "PrincipalSchema": bd_models.Principal,
            "RoleSchema": bd_models.Role,
            "RefreshTokenSchema": bd_models.RefreshToken,
            "JobSchema": bd_models.Job,
            "DateTriggerSchema": bd_models.DateTrigger,
            "IntervalTriggerSchema": bd_models.IntervalTrigger,
            "CronTriggerSchema": bd_models.CronTrigger,
            "GardenSchema": bd_models.Garden,
        }
    )

    @classmethod
    def _get_schema_name(cls, model):
        if isinstance(model, bd_models.MongoModel):
            return model.brewtils_model.schema
        else:
            return super(MongoParser, cls)._get_schema_name(model)
