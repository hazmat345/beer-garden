import six
import datetime
import pytz

try:
    from lark import ParseError
    from lark.exceptions import LarkError
except ImportError:
    from lark.common import ParseError

    LarkError = ParseError

import brewtils

# from brewtils.errors import ModelValidationError
from brewtils.choices import parse
from brewtils.errors import ModelValidationError, RequestStatusTransitionError
from brewtils.models import (
    Command as BrewtilsCommand,
    Instance as BrewtilsInstance,
    Parameter as BrewtilsParameter,
    Request as BrewtilsRequest,
    Job as BrewtilsJob,
)


class FieldBase:
    def __init__(
        self,
        required=False,
        default=None,
        choices=None,
        field_type=None,
        is_list=False,
        is_ref=False,
        unique_with=None,
        reverse_delete_rule="DO_NOTHING",
        min_value=None,
    ):
        """

        Args:
            required: Is the field required to save
            default: Default value of the field
            choices: Limit value to set amount of choices
            field_type: Approved mapping type for databases
            is_list: If this is a list
            is_ref: Is this stored in another table
            unique_with: List or Str to define columns to unique against

        Returns:

        """
        self.required = required
        self.default = default
        self.choices = choices
        self.field_type = field_type
        self.is_list = is_list
        self.is_ref = is_ref
        self.unique_with = unique_with
        self.reverse_delete_rule = reverse_delete_rule
        self.min_value = min_value

    def clean_update(self):
        pass

    def clean(self):
        pass


class Choices:
    brewtils_model = brewtils.models.Choices

    display = FieldBase(
        required=True, choices=brewtils.models.Choices.DISPLAYS, field_type="STRING"
    )
    strict = FieldBase(required=True, default=True, field_type="BOOLEAN")
    type = FieldBase(
        required=True,
        default="static",
        choices=brewtils.models.Choices.TYPES,
        field_type="STRING",
    )

    value = FieldBase(required=True, field_type="BLOB")
    details = FieldBase(field_type="DICT")

    def clean(self):
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


class Parameter:
    brewtils_model = brewtils.models.Parameter

    key = FieldBase(field_type="STRING", required=True)
    type = FieldBase(
        field_type="STRING",
        required=True,
        default="Any",
        choices=BrewtilsParameter.TYPES,
    )
    multi = FieldBase(field_type="BOOLEAN", required=True, default=False)
    display_name = FieldBase(field_type="STRING", required=False)
    optional = FieldBase(field_type="BOOLEAN", required=True, default=True)
    default = FieldBase(field_type="DICT", required=False, default=None)
    description = FieldBase(field_type="STRING", required=False)
    choices = FieldBase(field_type=brewtils.models.Choices, default=None)
    nullable = FieldBase(field_type="BOOLEAN", required=False, default=False)
    maximum = FieldBase(field_type="INT", required=False)
    minimum = FieldBase(field_type="INT", required=False)
    regex = FieldBase(field_type="STRING", required=False)
    form_input_type = FieldBase(
        field_type="STRING", required=False, choices=BrewtilsParameter.FORM_INPUT_TYPES
    )
    type_info = FieldBase(field_type="DICT", required=False)
    parameters = FieldBase(
        field_type=brewtils.models.Parameter, default=None, is_list=True
    )

    def clean(self):
        """Validate before saving to the database"""

        if not self.nullable and self.optional and self.default is None:
            raise ModelValidationError(
                f"Can not save Parameter {self}: For this Parameter nulls are not "
                f"allowed, but the parameter is optional with no default defined."
            )

        if len(self.parameters) != len(
            set(parameter.key for parameter in self.parameters)
        ):
            raise ModelValidationError(
                f"Can not save Parameter {self}: Contains Parameters with duplicate keys"
            )


class Command:

    brewtils_model = brewtils.models.Command

    name = FieldBase(field_type="STRING", required=True)
    description = FieldBase(field_type="STRING")
    parameters = FieldBase(
        field_type=brewtils.models.Parameter, default=None, is_list=True
    )
    command_type = FieldBase(
        field_type="STRING", choices=BrewtilsCommand.COMMAND_TYPES, default="ACTION"
    )
    output_type = FieldBase(
        field_type="STRING", choices=BrewtilsCommand.OUTPUT_TYPES, default="STRING"
    )
    schema = FieldBase(field_type="DICT")
    form = FieldBase(field_type="DICT")
    template = FieldBase(field_type="STRING")
    icon_name = FieldBase(field_type="STRING")
    hidden = FieldBase(field_type="BOOLEAN")

    # system = FieldBase(
    #     field_type=brewtils.models.System, default=None, is_ref=True
    # )

    system = FieldBase(field_type="DICT", default=None)

    def clean(self):
        """Validate before saving to the database"""

        if not self.name:
            raise ModelValidationError(
                f"Can not save Command"
                f"{' for system ' + self.system.name if self.system else ''}"
                f": Missing name"
            )

        if self.command_type not in BrewtilsCommand.COMMAND_TYPES:
            raise ModelValidationError(
                f"Can not save Command {self}: Invalid command type '{self.command_type}'"
            )

        if self.output_type not in BrewtilsCommand.OUTPUT_TYPES:
            raise ModelValidationError(
                f"Can not save Command {self}: Invalid output type '{self.output_type}'"
            )

        if len(self.parameters) != len(
            set(parameter.key for parameter in self.parameters)
        ):
            raise ModelValidationError(
                f"Can not save Command {self}: Contains Parameters with duplicate keys"
            )


class StatusInfo:
    brewtils_model = brewtils.models.StatusInfo
    heartbeat = FieldBase(field_type="DATE")


class Instance:
    brewtils_model = brewtils.models.Instance

    name = FieldBase(field_type="STRING", required=True, default="default")
    description = FieldBase(field_type="STRING")
    status = FieldBase(field_type="STRING", default="INITIALIZING")
    status_info = FieldBase(field_type=brewtils.models.StatusInfo)
    queue_type = FieldBase(field_type="STRING")
    queue_info = FieldBase(field_type="DICT")
    icon_name = FieldBase(field_type="STRING")
    metadata = FieldBase(field_type="DICT")

    def clean(self):
        """Validate before saving to the database"""

        if self.status not in BrewtilsInstance.INSTANCE_STATUSES:
            raise ModelValidationError(
                f"Can not save Instance {self}: Invalid status '{self.status}'"
            )


class RequestTemplate:
    brewtils_model = brewtils.models.RequestTemplate

    system = FieldBase(field_type="STRING", required=True)
    system_version = FieldBase(field_type="STRING", required=True)
    instance_name = FieldBase(field_type="STRING", required=True)
    namespace = FieldBase(field_type="STRING", required=True)
    command = FieldBase(field_type="STRING", required=True)
    command_type = FieldBase(field_type="STRING")
    parameters = FieldBase(field_type="DICT")
    comment = FieldBase(field_type="STRING")
    metadata = FieldBase(field_type="DICT")
    output_type = FieldBase(field_type="STRING")


class Request(RequestTemplate):
    brewtils_model = brewtils.models.Request

    parent = FieldBase(
        field_type=brewtils.models.Request, is_ref=True, reverse_delete_rule="CASCADE"
    )
    children = FieldBase(field_type="PLACE_HOLDER")
    output = FieldBase(field_type="STRING")
    output_type = FieldBase(field_type="STRING", choices=BrewtilsCommand.OUTPUT_TYPES)
    status = FieldBase(
        field_type="STRING", choices=BrewtilsRequest.STATUS_LIST, default="CREATED"
    )
    command_type = FieldBase(field_type="STRING", choices=BrewtilsCommand.COMMAND_TYPES)
    created_at = FieldBase(
        field_type="DATE", default=datetime.datetime.utcnow, required=True
    )
    updated_at = FieldBase(field_type="DATE", default=None, required=True)
    error_class = FieldBase(field_type="STRING")
    has_parent = FieldBase(field_type="BOOLEAN")
    requester = FieldBase(field_type="STRING")


class System:
    brewtils_model = brewtils.models.System

    name = FieldBase(
        field_type="STRING", required=True, unique_with=["name", "version", "namespace"]
    )
    description = FieldBase(field_type="STRING")
    version = FieldBase(field_type="STRING", required=True)
    namespace = FieldBase(field_type="STRING", required=True)
    max_instances = FieldBase(field_type="INT", default=1)
    instances = FieldBase(
        field_type=brewtils.models.Instance,
        is_ref=True,
        is_list=True,
        reverse_delete_rule="PULL",
    )
    commands = FieldBase(
        field_type=brewtils.models.Command,
        is_ref=True,
        is_list=True,
        reverse_delete_rule="PULL",
    )
    icon_name = FieldBase(field_type="STRING")
    display_name = FieldBase(field_type="STRING")
    metadata = FieldBase(field_type="DICT")
    local = FieldBase(field_type="BOOLEAN", default=True)


class Event:
    brewtils_model = brewtils.models.Event

    name = FieldBase(field_type="STRING", required=True)
    namespace = FieldBase(field_type="STRING", required=True)
    garden = FieldBase(field_type="STRING")
    payload = FieldBase(field_type="DICT")
    error = FieldBase(field_type="BOOLEAN")
    metadata = FieldBase(field_type="DICT")
    timestamp = FieldBase(field_type="DATE")


class Role:
    brewtils_model = brewtils.models.Role

    name = FieldBase(field_type="STRING", required=True, unique_with=["name"])
    description = FieldBase(field_type="STRING")
    commands = FieldBase(
        field_type=brewtils.models.Role,
        is_ref=True,
        is_list=True,
        reverse_delete_rule="PULL",
    )
    permissions = FieldBase(field_type="STRING", is_list=True)


class Principal:
    brewtils_model = brewtils.models.Principal

    username = FieldBase(field_type="STRING", required=True, unique_with=["username"])
    hash = FieldBase(field_type="STRING")
    roles = FieldBase(
        field_type=brewtils.models.Role,
        is_ref=True,
        is_list=True,
        reverse_delete_rule="PULL",
    )
    preferences = FieldBase(field_type="DICT")
    metadata = FieldBase(field_type="DICT")


class RefreshToken:
    brewtils_model = brewtils.models.RefreshToken

    issued = FieldBase(field_type="DATE", required=True)
    expires = FieldBase(field_type="DATE", required=True)
    payload = FieldBase(field_type="DICT", required=True)


class DateTrigger:
    brewtils_model = brewtils.models.DateTrigger

    run_date = FieldBase(field_type="DATE", required=True)
    timezone = FieldBase(
        field_type="STRING", required=False, default="utc", choices=pytz.all_timezones
    )


class IntervalTrigger:
    brewtils_model = brewtils.models.IntervalTrigger

    weeks = FieldBase(field_type="INT", default=0)
    days = FieldBase(field_type="INT", default=0)
    hours = FieldBase(field_type="INT", default=0)
    minutes = FieldBase(field_type="INT", default=0)
    seconds = FieldBase(field_type="INT", default=0)
    start_date = FieldBase(field_type="DATE")
    end_date = FieldBase(field_type="DATE")
    timezone = FieldBase(
        field_type="STRING", required=False, default="utc", choices=pytz.all_timezones
    )
    jitter = FieldBase(field_type="INT")
    reschedule_on_finish = FieldBase(
        field_type="BOOLEAN", required=False, default=False
    )


class CronTrigger:
    brewtils_model = brewtils.models.CronTrigger

    year = FieldBase(field_type="STRING", default="*")
    month = FieldBase(field_type="STRING", default="1")
    day = FieldBase(field_type="STRING", default="1")
    week = FieldBase(field_type="STRING", default="*")
    day_of_week = FieldBase(field_type="STRING", default="*")
    hour = FieldBase(field_type="STRING", default="0")
    minute = FieldBase(field_type="STRING", default="0")
    second = FieldBase(field_type="STRING", default="0")
    start_date = FieldBase(field_type="DATE")
    end_date = FieldBase(field_type="DATE")
    timezone = FieldBase(field_type="STRING", default="utc", choices=pytz.all_timezones)
    jitter = FieldBase(field_type="INT")


class Job:
    brewtils_model = brewtils.models.Job

    TRIGGER_MODEL_MAPPING = {
        "date": brewtils.models.DateTrigger,
        "cron": brewtils.models.CronTrigger,
        "interval": brewtils.models.IntervalTrigger,
    }

    name = FieldBase(field_type="STRING", required=True)
    trigger_type = FieldBase(
        field_type="STRING", required=True, choices=BrewtilsJob.TRIGGER_TYPES
    )
    request_template = FieldBase(
        field_type=TRIGGER_MODEL_MAPPING,
        required=True,
        choices=list(TRIGGER_MODEL_MAPPING.values()),
    )
    request_template = FieldBase(
        field_type=brewtils.models.RequestTemplate, required=True
    )
    misfire_grace_time = FieldBase(field_type="INT")
    coalesce = FieldBase(field_type="BOOLEAN", default=True)
    next_run_time = FieldBase(field_type="DATE")
    success_count = FieldBase(field_type="INT", required=True, default=0, min_value=0)
    error_count = FieldBase(field_type="INT", required=True, default=0, min_value=0)
    status = FieldBase(
        field_type="STRING",
        required=True,
        choices=BrewtilsJob.STATUS_TYPES,
        default="RUNNING",
    )
    max_instances = FieldBase(field_type="INT", default=3, min_value=1)

    def clean(self):
        """Validate before saving to the database"""

        if self.trigger_type not in self.TRIGGER_MODEL_MAPPING:
            raise ModelValidationError(
                f"Cannot save job. No mongo model for trigger type {self.trigger_type}"
            )

        trigger_class = self.TRIGGER_MODEL_MAPPING.get(self.trigger_type)
        if not isinstance(self.trigger, trigger_class):
            raise ModelValidationError(
                f"Cannot save job. Expected trigger type {self.trigger_type} but "
                f"actual type was {type(self.trigger)}"
            )


class Garden:
    brewtils_model = brewtils.models.Garden

    name = FieldBase(
        field_type="STRING", required=True, default="default", unique_with=["name"]
    )
    status = FieldBase(field_type="STRING", default="INITIALIZING")
    status_info = FieldBase(field_type=brewtils.models.StatusInfo)
    namespaces = FieldBase(field_type="STRING", is_list=True)
    connection_type = FieldBase(field_type="STRING")
    connection_params = FieldBase(field_type="DICT")
    systems = FieldBase(field_type="STRING", is_list=True)
