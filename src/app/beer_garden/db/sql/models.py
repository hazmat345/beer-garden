from sqlalchemy import ARRAY, Column, ForeignKey, UniqueConstraint
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import relationship

import sqlalchemy

import brewtils.models
from beer_garden.db import db_models

# Base = declarative_base()
Base = automap_base()

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
    "STRING": sqlalchemy.String,
    "BOOLEAN": sqlalchemy.Boolean,
    "INT": sqlalchemy.Integer,
    "DATE": sqlalchemy.Date,
    "JSON": sqlalchemy.JSON,
    "DICT": sqlalchemy.JSON,
}


def field_mapper(base_field):

    if base_field.is_list:
        if base_field.field_type not in base_type_mapper:
            return Column(
                ARRAY(base_type_mapper["JSON"]),
                default=base_field.default,
                nullable=base_field.required,
            )
        else:
            return Column(
                ARRAY(base_type_mapper[base_field.field_type]),
                default=base_field.default,
                nullable=base_field.required,
            )
    else:
        if base_field.field_type not in base_type_mapper:
            return Column(
                base_type_mapper["JSON"],
                default=base_field.default,
                nullable=base_field.required,
            )
        else:
            return Column(
                base_type_mapper[base_field.field_type],
                default=base_field.default,
                nullable=base_field.required,
            )


def class_mapper(sql_class, model):
    setattr(sql_class, "brewtils_model", getattr(model, "brewtils_model", None))
    setattr(sql_class, "clean", getattr(model, "clean", None))

    schema_mapping[model.brewtils_model.schema] = sql_class
    for field_label in [
        attr
        for attr in dir(model)
        if isinstance(getattr(model, attr), db_models.FieldBase)
    ]:

        field = getattr(model, field_label)

        if field.is_ref:
            setattr(
                sql_class,
                field_label,
                relationship(
                    field.field_type.schema, back_populates=sql_class.__tablename__
                ),
            )

            ref_cls = schema_mapping[field.field_type.schema]
            setattr(
                ref_cls,
                f"{sql_class.__tablename__}_id",
                Column(sqlalchemy.Integer, ForeignKey(f"{sql_class.__tablename__}.id")),
            )
            setattr(
                ref_cls, sql_class.__tablename__, relationship(type(sql_class).__name__)
            )

        else:
            setattr(sql_class, field_label, field_mapper(field))
            if field.unique_with:
                unique_args = [field_label]
                if isinstance(field.unique_with, list):
                    for unique in field.unique_with:
                        if unique not in unique_args:
                            unique_args.append(unique)
                else:
                    if field.unique_with not in unique_args:
                        unique_args.append(field.unique_with)

                unique_name = "_uc"
                for arg in unique_args:
                    unique_name = f"{arg}_{unique_name}"

                if getattr(sql_class, "__table_args__"):
                    setattr(
                        sql_class,
                        "__table_args__",
                        getattr(sql_class, "__table_args__")
                        + (UniqueConstraint(*unique_args, name="unique_name")),
                    )
                else:
                    setattr(
                        sql_class,
                        "__table_args__",
                        (UniqueConstraint(*unique_args, name="unique_name")),
                    )


class System(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.System.schema


class Instance(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Instance.schema


class Command(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Command.schema


class Parameter(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Parameter.schema


class Request(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Request.schema


class Choices(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Choices.schema


class Event(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Event.schema


class Principal(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Principal.schema


class Role(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Role.schema


class RefreshToken(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.RefreshToken.schema


class Job(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Job.schema


class RequestTemplate(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.RequestTemplate.schema


class DateTrigger(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.DateTrigger.schema


class CronTrigger(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.CronTrigger.schema


class IntervalTrigger(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.IntervalTrigger.schema


class Garden(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = brewtils.models.Garden.schema


class StatusInfo(Base):
    id = Column(sqlalchemy.Integer, primary_key=True)
    __tablename__ = "StatusInfo"


schema_mapping = dict()

class_mapper(Choices, db_models.Choices)
class_mapper(Parameter, db_models.Parameter)
class_mapper(Command, db_models.Command)
class_mapper(StatusInfo, db_models.StatusInfo)
class_mapper(Instance, db_models.Instance)
class_mapper(RequestTemplate, db_models.RequestTemplate)
class_mapper(Request, db_models.Request)
class_mapper(System, db_models.System)
class_mapper(Event, db_models.Event)
class_mapper(Role, db_models.Role)
class_mapper(Principal, db_models.Principal)
class_mapper(RefreshToken, db_models.RefreshToken)
class_mapper(DateTrigger, db_models.DateTrigger)
class_mapper(IntervalTrigger, db_models.IntervalTrigger)
class_mapper(CronTrigger, db_models.CronTrigger)
class_mapper(Job, db_models.Job)
class_mapper(Garden, db_models.Garden)
