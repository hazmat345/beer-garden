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


base_type_mapper = {
    "STRING": sqlalchemy.String,
    "BOOLEAN": sqlalchemy.Boolean,
    "INT": sqlalchemy.Integer,
    "DATE": sqlalchemy.Date,
    "JSON": sqlalchemy.JSON,
}


def field_mapper(base_field):
    base_field.required
    base_field.default
    base_field.choices
    base_field.field_type
    base_field.is_list
    base_field.is_ref

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


# schema_mapping = {
#     brewtils.models.Choices.schema: Choices,
#     brewtils.models.Parameter.schema: Parameter,
#     brewtils.models.Command.schema: Command,
#     "StatusInfo" : StatusInfo
# }
schema_mapping = dict()

class_mapper(Choices, db_models.Choices)
class_mapper(Parameter, db_models.Parameter)
class_mapper(Command, db_models.Command)
class_mapper(StatusInfo, db_models.StatusInfo)
class_mapper(Instance, db_models.Instance)
