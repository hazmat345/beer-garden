# -*- coding: utf-8 -*-
import datetime
import logging

from .. import db_models

try:
    from lark import ParseError
    from lark.exceptions import LarkError
except ImportError:
    from lark.common import ParseError

    LarkError = ParseError
from mongoengine import (
    BooleanField,
    DateTimeField,
    DictField,
    Document,
    DynamicField,
    EmbeddedDocument,
    EmbeddedDocumentField,
    GenericEmbeddedDocumentField,
    IntField,
    ListField,
    ReferenceField,
    StringField,
    CASCADE,
    PULL,
    DO_NOTHING,
)
from mongoengine.errors import DoesNotExist

import brewtils.models
from .fields import DummyField

__all__ = [
    "System",
    "Instance",
    "Command",
    "Parameter",
    "Request",
    "Choices",
    "Event",
    "Principal",
    "Role",
    "RefreshToken",
    "Job",
    "RequestTemplate",
    "DateTrigger",
    "CronTrigger",
    "IntervalTrigger",
    "Garden",
]

base_type_mapper = {
    "STRING": StringField,
    "BOOLEAN": BooleanField,
    "INT": IntField,
    "DATE": DateTimeField,
    "JSON": DictField,
    "DICT": DictField,
    "PLACE_HOLDER": DummyField,
    "BLOB": DynamicField,
}

schema_mapping = dict()


class MongoModel:
    brewtils_model = None

    def __str__(self):
        return self.brewtils_model.__str__(self)

    def __repr__(self):
        return self.brewtils_model.__repr__(self)

    @classmethod
    def index_names(cls):
        return [index["name"] for index in cls._meta["indexes"]]

    def save(self, *args, **kwargs):
        kwargs.setdefault("write_concern", {"w": "majority"})
        return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        # Sigh. In delete (but not save!) write_concern things ARE the kwargs!
        kwargs.setdefault("w", "majority")
        return super().delete(*args, **kwargs)

    def clean_update(self):
        pass


def field_mapper(base_field):
    if base_field.choices:
        for key, value in enumerate(base_field.choices):
            if isinstance(value, brewtils.models.BaseModel):
                base_field.choices[key] = schema_mapping[value]

    if base_field.is_list:
        if base_field.field_type not in base_type_mapper:
            if isinstance(brewtils.models.BaseModel, base_field.field_type):
                return ListField(
                    EmbeddedDocumentField(
                        base_field.field_type.__name__,
                        required=base_field.required,
                        default=base_field.default,
                        choices=base_field.choices,
                    )
                )
            else:
                return ListField(
                    GenericEmbeddedDocumentField(
                        required=base_field.required,
                        default=base_field.default,
                        choices=base_field.choices,
                    )
                )
        else:
            return ListField(
                base_type_mapper[base_field.field_type](
                    required=base_field.required,
                    default=base_field.default,
                    choices=base_field.choices,
                )
            )
    else:
        if base_field.field_type not in base_type_mapper:
            if isinstance(brewtils.models.BaseModel, base_field.field_type):
                return EmbeddedDocumentField(
                    base_field.field_type.__name__,
                    required=base_field.required,
                    default=base_field.default,
                    choices=base_field.choices,
                )
            else:
                return GenericEmbeddedDocumentField(
                    required=base_field.required,
                    default=base_field.default,
                    choices=base_field.choices,
                )
        else:
            return base_type_mapper[base_field.field_type](
                required=base_field.required,
                default=base_field.default,
                choices=base_field.choices,
            )


def class_mapper(mongo_class, model):

    new_attributes = dict()
    new_attributes["brewtils_model"] = getattr(model, "brewtils_model", None)

    if hasattr(model, "clean"):
        new_attributes["clean"] = getattr(model, "clean")
    if hasattr(model, "clean_update"):
        new_attributes["clean_update"] = getattr(model, "clean_update")

    # Set it just in case the object references itself
    for field_label in [
        attr
        for attr in dir(model)
        if isinstance(getattr(model, attr), db_models.FieldBase)
    ]:

        field = getattr(model, field_label)

        if field.is_ref:
            reverse_delete_rule = DO_NOTHING
            if field.reverse_delete_rule == "CASCADE":
                reverse_delete_rule = CASCADE
            elif field.reverse_delete_rule == "PULL":
                reverse_delete_rule = PULL

            if field.is_list:
                new_attributes[field_label] = ReferenceField(
                    field.field_type.__name__,
                    required=field.required,
                    reverse_delete_rule=reverse_delete_rule,
                )
            else:
                new_attributes[field_label] = ListField(
                    ReferenceField(
                        field.field_type.__name__,
                        required=field.required,
                        reverse_delete_rule=reverse_delete_rule,
                    )
                )

        else:

            new_attributes[field_label] = field_mapper(field)
            if field.unique_with:
                unique_args = [field_label]
                if isinstance(field.unique_with, list):
                    for unique in field.unique_with:
                        if unique not in unique_args:
                            unique_args.append(unique)
                else:
                    if field.unique_with not in unique_args:
                        unique_args.append(field.unique_with)

                if hasattr(mongo_class, "meta"):
                    meta = getattr(mongo_class, "meta")
                    if "indexes" in meta:
                        unique_set = False
                        for key, value in enumerate(meta["indexes"]):
                            if value["name"] == "unique_index":
                                for unique_field in unique_args:
                                    if (
                                        unique_field
                                        not in meta["indexes"][key]["fields"]
                                    ):
                                        meta["indexes"][key]["fields"].append(
                                            unique_field
                                        )
                                unique_set = True
                                break
                        if not unique_set:
                            meta["indexes"].append(
                                {
                                    "name": "unique_index",
                                    "fields": unique_args,
                                    "unique": True,
                                }
                            )
                    else:
                        meta["indexes"] = [
                            {
                                "name": "unique_index",
                                "fields": unique_args,
                                "unique": True,
                            }
                        ]

                    new_attributes["meta"] = meta
                else:
                    meta = {
                        "auto_create_index": False,  # We need to manage this ourselves
                        "index_background": True,
                        "indexes": [
                            {
                                "name": "unique_index",
                                "fields": unique_args,
                                "unique": True,
                            }
                        ],
                    }

                    new_attributes["meta"] = meta

    # Creates new class objects with the attributes injected into them
    if hasattr(mongo_class, "embedded") and mongo_class.embedded:
        new_class = type(
            mongo_class.__name__, (MongoModel, EmbeddedDocument), new_attributes
        )
    else:
        new_class = type(mongo_class.__name__, (MongoModel, Document), new_attributes)

    # Sets it for global usage
    schema_mapping[model.brewtils_model] = new_class
    return new_class


class Choices:
    embedded = True


class Parameter:
    embedded = True
    # If no display name was set, it will default it to the same thing as the key
    def __init__(self, *args, **kwargs):
        if not kwargs.get("display_name", None):
            kwargs["display_name"] = kwargs.get("key", None)

        EmbeddedDocument.__init__(self, *args, **kwargs)


class Command:
    pass


class Command:
    pass


class Instance:
    pass


class Request:
    meta = {
        "auto_create_index": False,  # We need to manage this ourselves
        "index_background": True,
        "indexes": [
            # These are used for sorting all requests
            {"name": "command_index", "fields": ["command"]},
            {"name": "command_type_index", "fields": ["command_type"]},
            {"name": "system_index", "fields": ["system"]},
            {"name": "instance_name_index", "fields": ["instance_name"]},
            {"name": "status_index", "fields": ["status"]},
            {"name": "created_at_index", "fields": ["created_at"]},
            {"name": "updated_at_index", "fields": ["updated_at"]},
            {"name": "comment_index", "fields": ["comment"]},
            {"name": "parent_ref_index", "fields": ["parent"]},
            {"name": "parent_index", "fields": ["has_parent"]},
            # These are for sorting parent requests
            {"name": "parent_command_index", "fields": ["has_parent", "command"]},
            {"name": "parent_system_index", "fields": ["has_parent", "system"]},
            {
                "name": "parent_instance_name_index",
                "fields": ["has_parent", "instance_name"],
            },
            {"name": "parent_status_index", "fields": ["has_parent", "status"]},
            {"name": "parent_created_at_index", "fields": ["has_parent", "created_at"]},
            {"name": "parent_comment_index", "fields": ["has_parent", "comment"]},
            # These are used for filtering all requests while sorting on created time
            {"name": "created_at_command_index", "fields": ["-created_at", "command"]},
            {"name": "created_at_system_index", "fields": ["-created_at", "system"]},
            {
                "name": "created_at_instance_name_index",
                "fields": ["-created_at", "instance_name"],
            },
            {"name": "created_at_status_index", "fields": ["-created_at", "status"]},
            # These are used for filtering parent while sorting on created time
            {
                "name": "parent_created_at_command_index",
                "fields": ["has_parent", "-created_at", "command"],
            },
            {
                "name": "parent_created_at_system_index",
                "fields": ["has_parent", "-created_at", "system"],
            },
            {
                "name": "parent_created_at_instance_name_index",
                "fields": ["has_parent", "-created_at", "instance_name"],
            },
            {
                "name": "parent_created_at_status_index",
                "fields": ["has_parent", "-created_at", "status"],
            },
            # This is used for text searching
            {
                "name": "text_index",
                "fields": [
                    "$system",
                    "$command",
                    "$command_type",
                    "$comment",
                    "$status",
                    "$instance_name",
                ],
            },
        ],
    }

    logger = logging.getLogger(__name__)

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.utcnow()
        super(Request, self).save(*args, **kwargs)


class System(MongoModel, Document):
    brewtils_model = brewtils.models.System

    def deep_save(self):
        """Deep save. Saves Commands, Instances, and the System

        Mongoengine cannot save bidirectional references in one shot because
        'You can only reference documents once they have been saved to the database'
        So we must mangle the System to have no Commands, save it, save the individual
        Commands with the System reference, update the System with the Command list, and
        then save the System again
        """

        # Note if this system is already saved
        delete_on_error = self.id is None

        # Save these off here so we can 'revert' in case of an exception
        temp_commands = self.commands
        temp_instances = self.instances

        try:
            # Before we start saving things try to make sure everything will validate
            # correctly. This means multiple passes through the collections, but we want
            # to minimize the chances of having to bail out after saving something since
            # we don't have transactions

            # However, we have to start by saving the System. We need it in the database
            # so the Commands will validate against it correctly (the ability to undo
            # this is why we saved off delete_on_error earlier) The reference lists must
            # be empty or else we encounter the bidirectional reference issue
            self.commands = []
            self.instances = []
            self.save()

            # Make sure all commands have the correct System reference
            for command in temp_commands:
                command.system = self

            # Now validate
            for command in temp_commands:
                command.validate()
            for instance in temp_instances:
                instance.validate()

            # All validated, now save everything
            for command in temp_commands:
                command.save(validate=False)
            for instance in temp_instances:
                instance.save(validate=False)
            self.commands = temp_commands
            self.instances = temp_instances
            self.save()

        # Since we don't have actual transactions we are not in a good position here,
        # so try our best to 'roll back'
        except Exception:
            self.commands = temp_commands
            self.instances = temp_instances
            if delete_on_error and self.id:
                self.delete()
            raise

    def deep_delete(self):
        """Completely delete a system"""
        self.delete_commands()
        self.delete_instances()
        return self.delete()

    def delete_commands(self):
        """Delete all commands associated with this system"""
        for command in self.commands:
            command.delete()

    def delete_instances(self):
        """Delete all instances associated with this system"""
        for instance in self.instances:
            instance.delete()


class Event:
    pass


class Role:
    pass


class Principal:
    pass


class RefreshToken(Document):
    brewtils_model = brewtils.models.RefreshToken

    def get_principal(self):
        principal_id = self.payload.get("sub")
        if not principal_id:
            return None

        try:
            return Principal.objects.get(id=principal_id)
        except DoesNotExist:
            return None


class RequestTemplate:
    embedded = True


class DateTrigger:
    embedded = True


class IntervalTrigger:
    embedded = True


class CronTrigger:
    embedded = True


class Job:
    meta = {
        "auto_create_index": False,
        "index_background": True,
        "indexes": [
            {"name": "next_run_time_index", "fields": ["next_run_time"], "sparse": True}
        ],
    }


class Garden:
    pass


class StatusInfo:
    embedded = True


class SystemGardenMapping(MongoModel, Document):
    system = ReferenceField("System")
    garden = ReferenceField("Garden")


# Remap the classes to include data and build mongodb objects

Choices = class_mapper(Choices, db_models.Choices)
Parameter = class_mapper(Parameter, db_models.Parameter)
Command = class_mapper(Command, db_models.Command)
StatusInfo = class_mapper(StatusInfo, db_models.StatusInfo)
Instance = class_mapper(Instance, db_models.Instance)
RequestTemplate = class_mapper(RequestTemplate, db_models.RequestTemplate)
Request = class_mapper(Request, db_models.Request)
System = class_mapper(System, db_models.System)
Event = class_mapper(Event, db_models.Event)
Role = class_mapper(Role, db_models.Role)
Principal = class_mapper(Principal, db_models.Principal)
RefreshToken = class_mapper(RefreshToken, db_models.RefreshToken)
DateTrigger = class_mapper(DateTrigger, db_models.DateTrigger)
IntervalTrigger = class_mapper(IntervalTrigger, db_models.IntervalTrigger)
CronTrigger = class_mapper(CronTrigger, db_models.CronTrigger)
Job = class_mapper(Job, db_models.Job)
Garden = class_mapper(Garden, db_models.Garden)

# Update the Command field now that all models are defined
# System.register_delete_rule(Command, "system", CASCADE)
