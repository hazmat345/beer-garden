import datetime

import pytz
import six

try:
    from lark import ParseError
    from lark.exceptions import LarkError
except ImportError:
    from lark.common import ParseError

    LarkError = ParseError

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, PickleType, ForeignKey, UniqueConstraint, DateTime
from sqlalchemy_utils import ChoiceType
from sqlalchemy.orm import validates, relationship
from sqlalchemy import event

import brewtils
from brewtils.choices import parse
from brewtils.errors import ModelValidationError, RequestStatusTransitionError
from brewtils.models import (
    Command as BrewtilsCommand,
    Instance as BrewtilsInstance,
    Parameter as BrewtilsParameter,
    Request as BrewtilsRequest,
    Job as BrewtilsJob,
)

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
    # "DateTrigger",
    # "CronTrigger",
    # "IntervalTrigger",
    "Garden",
]

Base = declarative_base()

restricted_field_mapping = {
    "model_metadata": "metadata",
}


class BaseModel:
    pass


class Choices(BaseModel, Base):
    __tablename__ = brewtils.models.Choices.schema
    brewtils_model = brewtils.models.Choices

    id = Column(Integer, primary_key=True)
    display = Column(String, nullable=False)
    strict = Column(Boolean, default=True)
    type = Column(String, nullable=False)
    value = Column(PickleType, nullable=False)
    details = Column(PickleType)

    parameter_id = Column(Integer, ForeignKey(f'{brewtils.models.Parameter.schema}.id'))

    @validates('display')
    def validate_display(self, key, value):
        if value not in brewtils.models.Choices.DISPLAYS:
            raise ModelValidationError(
                f"Can not save choices '{self}': '{value}' is not a valid input"
            )
        return value

    @validates('type')
    def validate_type(self, key, value):
        if value not in brewtils.models.Choices.TYPES:
            raise ModelValidationError(
                f"Can not save choices '{self}': '{value}' is not a valid input"
            )
        return value

    @validates('value')
    def validate_value(self, key, value):
        if self.type == "static" and not isinstance(self.value, (list, dict)):
            raise ModelValidationError(
                f"Can not save choices '{self}': type is 'static' but the value is "
                f"not a list or dictionary"
            )
        elif self.type == "url" and not isinstance(self.value, six.string_types):
            raise ModelValidationError(
                f"Can not save choices '{self}': type is 'url' but the value is "
                f"not a string"
            )
        elif self.type == "command" and not isinstance(
                self.value, (six.string_types, dict)
        ):
            raise ModelValidationError(
                f"Can not save choices '{self}': type is 'command' but the value is "
                f"not a string or dict"
            )

        if self.type == "command" and isinstance(self.value, dict):
            value_keys = self.value.keys()
            for required_key in ("command", "system", "version"):
                if required_key not in value_keys:
                    raise ModelValidationError(
                        f"Can not save choices '{self}': specifying value as a "
                        f"dictionary requires a '{required_key}' item"
                    )

        try:
            if self.details == {}:
                if isinstance(self.value, six.string_types):
                    self.details = parse(self.value)
                elif isinstance(self.value, dict):
                    self.details = parse(self.value["command"])
        except (LarkError, ParseError):
            raise ModelValidationError(
                f"Can not save choices '{self}': Unable to parse"
            )
        return value


class Parameter(BaseModel, Base):
    __tablename__ = brewtils.models.Parameter.schema
    brewtils_model = brewtils.models.Parameter

    id = Column(Integer, primary_key=True)
    key = Column(String, nullable=False)
    type = Column(String, default="Any")
    multi = Column(Boolean, nullable=False, default=False)
    display_name = Column(String)
    optional = Column(Boolean, nullable=False, default=True)
    default = Column(PickleType, default=None)
    description = Column(String)
    nullable = Column(Boolean, default=False)
    maximum = Column(Integer)
    minimum = Column(Integer)
    regex = Column(String)
    form_input_type = Column(String)
    type_info = Column(PickleType)

    parameter_id = Column(Integer, ForeignKey(f'{brewtils.models.Parameter.schema}.id'))
    command_id = Column(Integer, ForeignKey(f'{brewtils.models.Command.schema}.id'))

    choices = relationship("Choices", cascade="all, save-update, merge, delete, delete-orphan")
    parameters = relationship("Parameter", cascade="all, save-update, merge, delete")

    @validates('type')
    def validate_type(self, key, value):
        if value not in BrewtilsParameter.TYPES:
            raise ModelValidationError(
                f"Can not save Parameter '{self}': '{value}' is not a valid input"
            )
        return value

    @validates('form_input_type')
    def validate_form_input_type(self, key, value):
        if value not in BrewtilsParameter.FORM_INPUT_TYPES:
            raise ModelValidationError(
                f"Can not save Parameter '{self}': '{value}' is not a valid input"
            )
        return value

    @validates('display_name')
    def validate_display_name(self, key, value):
        if value is None:
            return self.key
        return value

    @validates('parameters')
    def validate_parameters(self, key, value):
        if len(self.parameters) != len(
                set(parameter.key for parameter in self.parameters)
        ):
            raise ModelValidationError(
                f"Can not save Parameter {self}: Contains Parameters with duplicate keys"
            )

        return value

    @validates('nullable')
    def validate_nullable(self, key, value):
        if not self.nullable and self.optional and self.default is None:
            raise ModelValidationError(
                f"Can not save Parameter {self}: For this Parameter nulls are not "
                f"allowed, but the parameter is optional with no default defined."
            )

        return value


class Command(BaseModel, Base):
    __tablename__ = brewtils.models.Command.schema
    brewtils_model = brewtils.models.Command

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String)
    command_type = Column(String, default="ACTION")
    output_type = Column(String, default="STRING")
    schema = Column(PickleType)
    form = Column(PickleType)
    template = Column(String)
    hidden = Column(Boolean)
    icon_name = Column(String)

    parameters = relationship("Parameter", cascade="all, save-update, merge, delete, delete-orphan")

    system_id = Column(Integer, ForeignKey(f'{brewtils.models.System.schema}.id'))
    system = relationship("System", back_populates="commands")

    UniqueConstraint('name', 'system')

    @validates('name')
    def validate_name(self, key, value):
        if not value:
            raise ModelValidationError(
                f"Can not save Command"
                f"{' for system ' + self.system.name if self.system else ''}"
                f": Missing name"
            )
        return value

    @validates('command_type')
    def validate_command_type(self, key, value):
        if value not in BrewtilsCommand.COMMAND_TYPES:
            raise ModelValidationError(
                f"Can not save Command {self}: Invalid command type '{value}'"
            )
        return value

    @validates('output_type')
    def validate_output_type(self, key, value):
        if value not in BrewtilsCommand.OUTPUT_TYPES:
            raise ModelValidationError(
                f"Can not save Command {self}: Invalid output type '{value}'"
            )
        return value

    @validates('parameters')
    def validate_parameters(self, key, value):
        if len(value) != len(
                set(parameter.key for parameter in value)
        ):
            raise ModelValidationError(
                f"Can not save Command {self}: Contains Parameters with duplicate keys"
            )
        return value


class StatusInfo(BaseModel, Base):
    __tablename__ = brewtils.models.StatusInfo.schema
    brewtils_model = brewtils.models.StatusInfo

    id = Column(Integer, primary_key=True)
    heartbeat = Column(DateTime)

    instance_id = Column(Integer, ForeignKey(f'{brewtils.models.Instance.schema}.id'))
    garden_id = Column(Integer, ForeignKey(f'{brewtils.models.Garden.schema}.id'))


class Instance(BaseModel, Base):
    __tablename__ = brewtils.models.Instance.schema
    brewtils_model = brewtils.models.Instance

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, default="default")
    description = Column(String)
    status = Column(String, default="INITIALIZING")
    queue_type = Column(String)
    queue_info = Column(PickleType)
    icon_name = Column(String)
    model_metadata = Column(PickleType)

    system_id = Column(Integer, ForeignKey(f'{brewtils.models.System.schema}.id'))
    status_info = relationship("StatusInfo", cascade="all, save-update, merge, delete")

    @validates('status')
    def validate_parameters(self, key, value):
        if value not in BrewtilsInstance.INSTANCE_STATUSES:
            raise ModelValidationError(
                f"Can not save Instance {self}: Invalid status 'value'"
            )
        return value


class RequestTemplate(BaseModel, Base):
    __tablename__ = brewtils.models.RequestTemplate.schema
    brewtils_model = brewtils.models.RequestTemplate

    id = Column(Integer, primary_key=True)

    # These fields are duplicated for Request, changes to this field
    # necessitate a change to the RequestTemplateSchema in brewtils.
    system = Column(String, nullable=False)
    system_name = Column(String, nullable=False)
    instance_name = Column(String, nullable=False)
    namespace = Column(String, nullable=False)
    command = Column(String, nullable=False)
    command_type = Column(String)
    parameters = Column(PickleType)
    comment = Column(String)
    model_metadata = Column(PickleType)
    output_type = Column(PickleType)

    # Creating Relationship with Job
    job_id = Column(Integer, ForeignKey(f'{brewtils.models.Job.schema}.id'))


class Request(BaseModel, Base):
    __tablename__ = brewtils.models.Request.schema
    brewtils_model = brewtils.models.Request

    id = Column(Integer, primary_key=True)

    # These fields are duplicated for RequestTemplate, changes to this field
    # necessitate a change to the RequestTemplateSchema in brewtils.
    system = Column(String, nullable=False)
    system_name = Column(String, nullable=False)
    instance_name = Column(String, nullable=False)
    namespace = Column(String, nullable=False)
    command = Column(String, nullable=False)
    command_type = Column(String)
    parameters = Column(PickleType)
    comment = Column(String)
    model_metadata = Column(PickleType)
    output_type = Column(PickleType)

    # Request Specific fields
    output = Column(String)
    output_type = Column(String)
    status = Column(String, default="CREATED")
    output = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=None)
    error_class = Column(String)
    has_parent = Column(Boolean)
    requester = Column(String)

    parent_id = Column(Integer, ForeignKey(f'{brewtils.models.Request.schema}.id'))
    #request_id = Column(Integer, ForeignKey(f'{brewtils.models.Request.schema}.id'))
    #children = relationship("Request", cascade="all, save-update, merge, delete", back_populates="parent")
    children = relationship("Request", cascade="all, save-update, merge, delete")
    #parent = relationship("Request", back_populates="children")

    @validates('updated_at')
    def validate_updated_at(self, key, value):
        return datetime.datetime.utcnow()

    @validates('status')
    def validate_status(self, key, value):
        if value not in BrewtilsRequest.STATUS_LIST:
            raise ModelValidationError(
                f"Can not save Request {self}: Invalid status '{value}'"
            )
        return value

    @validates('command_type')
    def validate_command_type(self, key, value):
        if (
                value is not None
                and value not in BrewtilsRequest.COMMAND_TYPES
        ):
            raise ModelValidationError(
                f"Can not save Request {self}: Invalid command type '{value}'"
            )
        return value

    @validates('output_type')
    def validate_output_type(self, key, value):
        if (
                value is not None
                and value not in BrewtilsRequest.OUTPUT_TYPES
        ):
            raise ModelValidationError(
                f"Can not save Request {self}: Invalid output type '{value}'"
            )
        return value


#@event.listens_for(Request.status, 'set', active_history=True)
def request_status_change(target, value, old_status, initiator):
    if value != old_status:
        if old_status in BrewtilsRequest.COMPLETED_STATUSES:
            raise RequestStatusTransitionError(
                f"Status for a request cannot be updated once it has been "
                f"completed. Current: {old_status}, Requested: {value}"
            )

        if (
                old_status == "IN_PROGRESS"
                and value not in BrewtilsRequest.COMPLETED_STATUSES
        ):
            raise RequestStatusTransitionError(
                f"Request status can only transition from IN_PROGRESS to a "
                f"completed status. Requested: {value}, completed statuses "
                f"are {BrewtilsRequest.COMPLETED_STATUSES}."
            )


class System(BaseModel, Base):
    __tablename__ = brewtils.models.System.schema
    brewtils_model = brewtils.models.System

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String)
    version = Column(String, nullable=False)
    namespace = Column(String, nullable=False)
    max_instances = Column(Integer, default=1)
    icon_name = Column(String)
    display_name = Column(String)
    model_metadata = Column(PickleType)
    local = Column(Boolean, default=True)

    instances = relationship("Instance", cascade="all, save-update, merge, delete, delete-orphan")
    commands = relationship("Command", cascade="all, save-update, merge, delete, delete-orphan")
    garden_id = Column(Integer, ForeignKey(f'{brewtils.models.Garden.schema}.id'))

    UniqueConstraint('namespace', 'name', 'version')

    @validates('instances')
    def validate_output_type(self, key, value):
        if len(value) > self.max_instances:
            raise ModelValidationError(
                "Can not save System %s: Number of instances (%s) "
                "exceeds system limit (%s)"
                % (str(self), len(value), self.max_instances)
            )

        if len(value) != len(
                set(instance.name for instance in value)
        ):
            raise ModelValidationError(
                "Can not save System %s: Duplicate instance names" % str(self)
            )

        return value


class Event(BaseModel, Base):
    __tablename__ = brewtils.models.Event.schema
    brewtils_model = brewtils.models.Event

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    namespace = Column(String, nullable=False)
    garden = Column(String)
    payload = Column(PickleType)
    error = Column(Boolean)
    model_metadata = Column(PickleType)
    timestamp = Column(DateTime)


class Role(BaseModel, Base):
    __tablename__ = brewtils.models.Role.schema
    brewtils_model = brewtils.models.Role

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String)
    permissions = Column(PickleType)

    role_id = Column(Integer, ForeignKey(f'{brewtils.models.Role.schema}.id'))
    principal_id = Column(Integer, ForeignKey(f'{brewtils.models.Principal.schema}.id'))

    roles = relationship("Role", cascade="all, save-update, merge, delete")

    UniqueConstraint('name')


class Principal(BaseModel, Base):
    __tablename__ = brewtils.models.Principal.schema
    brewtils_model = brewtils.models.Principal

    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    hash = Column(String)
    preferences = Column(PickleType)
    model_metadata = Column(PickleType)

    roles = relationship("Role", cascade="all, save-update, merge, delete")

    UniqueConstraint('username')


class RefreshToken(BaseModel, Base):
    __tablename__ = brewtils.models.RefreshToken.schema
    brewtils_model = brewtils.models.RefreshToken

    id = Column(Integer, primary_key=True)
    issued = Column(DateTime, nullable=False)
    expires = Column(DateTime, nullable=False)
    payload = Column(PickleType, nullable=False)


# class DateTrigger(BaseModel):
#     __tablename__ = brewtils.models.RefreshToken.schema
#     brewtils_model = brewtils.models.DateTrigger
#
#     id = Column(Integer, primary_key=True)
#     run_date = Column(DateTime, nullable=False)
#     timezone = Column(ChoiceType(pytz.all_timezones), default="utc")
#
#
# class IntervalTrigger(BaseModel):
#     __tablename__ = brewtils.models.IntervalTrigger.schema
#     brewtils_model = brewtils.models.IntervalTrigger
#
#     id = Column(Integer, primary_key=True)
#     weeks = Column(Integer, default=0)
#     days = Column(Integer, default=0)
#     hours = Column(Integer, default=0)
#     minutes = Column(Integer, default=0)
#     seconds = Column(Integer, default=0)
#     start_date = Column(DateTime, nullable=False)
#     end_date = Column(DateTime, nullable=False)
#     timezone = Column(ChoiceType(pytz.all_timezones), default="utc")
#     jitter = Column(Integer)
#     reschedule_on_finish = Column(Boolean, default=False)
#
#
# class CronTrigger(BaseModel):
#     __tablename__ = brewtils.models.CronTrigger.schema
#     brewtils_model = brewtils.models.CronTrigger
#
#     id = Column(Integer, primary_key=True)
#
#     year = Column(String, default="*")
#     month = Column(String, default="1")
#     day = Column(String, default="1")
#     week = Column(String, default="*")
#     day_of_week = Column(String, default="*")
#     hour = Column(String, default="0")
#     minute = Column(String, default="0")
#     second = Column(String, default="0")
#     start_date = Column(DateTime)
#     end_date = Column(DateTime)
#     timezone = Column(ChoiceType(pytz.all_timezones), default="utc")
#     jitter = Column(Integer)


class Job(BaseModel, Base):
    __tablename__ = brewtils.models.Job.schema
    brewtils_model = brewtils.models.Job

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    trigger_type = Column(String, nullable=False)
    trigger = Column(PickleType, nullable=False)
    misfire_grace_time = Column(Integer)
    coalesce = Column(Boolean, default=True)
    next_run_time = Column(DateTime)
    success_count = Column(Integer, default=0, nullable=False)
    error_count = Column(Integer, default=0, nullable=False)
    max_instances = Column(Integer, default=3)
    status = Column(String, nullable=False,
                          default="RUNNING")

    request_template = relationship("RequestTemplate", cascade="all, save-update, merge, delete")

    @validates('trigger_type')
    def validate_trigger_type(self, key, value):
        if value not in BrewtilsJob.TRIGGER_TYPES:
            raise ModelValidationError(
                f"Can not save Job '{self}': '{value}' is not a valid input"
            )
        return value

    @validates('status')
    def validate_status(self, key, value):
        if value not in BrewtilsJob.STATUS_TYPES:
            raise ModelValidationError(
                f"Can not save Job '{self}': '{value}' is not a valid input"
            )
        return value

    @validates('success_count', 'error_count')
    def validate_counts(self, key, value):
        if value < 0:
            raise ModelValidationError(
                "Can not save JOB %s: Number of %s "
                "is below zero)"
                % (self.name, key)
            )
        return value

    @validates('max_instances')
    def validate_max_instances(self, key, value):
        if value < 1:
            raise ModelValidationError(
                "Can not save JOB %s: Number of %s "
                "is below one)"
                % (self.name, key)
            )
        return value


class Garden(BaseModel, Base):
    __tablename__ = brewtils.models.Garden.schema
    brewtils_model = brewtils.models.Garden

    id = Column(Integer, primary_key=True)
    name = Column(String, default="default", nullable=False)
    status = Column(String, default="INITIALIZING")
    namespaces = Column(PickleType)
    connection_type = Column(String)
    connection_params = Column(PickleType)

    systems = relationship("System", cascade="all, save-update, merge, delete")
    status_info = relationship("StatusInfo", cascade="all, save-update, merge, delete")

    UniqueConstraint('name')
