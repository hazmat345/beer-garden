import six

try:
    from lark import ParseError
    from lark.exceptions import LarkError
except ImportError:
    from lark.common import ParseError

    LarkError = ParseError

import brewtils
from brewtils.errors import ModelValidationError
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
    details = FieldBase(field_type="JSON")

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
    default = FieldBase(field_type="JSON", required=False, default=None)
    description = FieldBase(field_type="STRING", required=False)
    choices = FieldBase(field_type=brewtils.models.Choices, default=None, is_ref=True)
    nullable = FieldBase(field_type="BOOLEAN", required=False, default=False)
    maximum = FieldBase(field_type="INT", required=False)
    minimum = FieldBase(field_type="INT", required=False)
    regex = FieldBase(field_type="STRING", required=False)
    form_input_type = FieldBase(
        field_type="STRING", required=False, choices=BrewtilsParameter.FORM_INPUT_TYPES
    )
    type_info = FieldBase(field_type="JSON", required=False)
    parameters = FieldBase(
        field_type=brewtils.models.Parameter, default=None, is_ref=True, is_list=True
    )


class Command:
    brewtils_model = brewtils.models.Command

    name = FieldBase(field_type="STRING", required=True)
    description = FieldBase(field_type="STRING")
    parameters = FieldBase(
        field_type=brewtils.models.Parameter, default=None, is_ref=True, is_list=True
    )
    command_type = FieldBase(
        field_type="STRING", choices=BrewtilsCommand.COMMAND_TYPES, default="ACTION"
    )
    output_type = FieldBase(
        field_type="STRING", choices=BrewtilsCommand.OUTPUT_TYPES, default="STRING"
    )
    schema = FieldBase(field_type="JSON")
    form = FieldBase(field_type="JSON")
    template = FieldBase(field_type="STRING")
    icon_name = FieldBase(field_type="STRING")
    parameters = FieldBase(
        field_type=brewtils.models.Parameter, default=None, is_ref=True, is_list=True
    )

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


class StatusInfoBrewtils:
    schema = "StatusInfo"


class StatusInfo:
    brewtils_model = StatusInfoBrewtils
    heartbeat = FieldBase(field_type="DATE")


class Instance:
    brewtils_model = brewtils.models.Instance

    name = FieldBase(field_type="STRING", required=True, default="default")
    description = FieldBase(field_type="STRING")
    status = FieldBase(field_type="STRING", default="INITIALIZING")
    status_info = FieldBase(
        field_type=StatusInfoBrewtils, default=StatusInfo(), is_ref=True
    )
    queue_type = FieldBase(field_type="STRING")
    queue_info = FieldBase(field_type="JSON")
    icon_name = FieldBase(field_type="STRING")
    metadata = FieldBase(field_type="JSON")

    def clean(self):
        """Validate before saving to the database"""

        if self.status not in BrewtilsInstance.INSTANCE_STATUSES:
            raise ModelValidationError(
                f"Can not save Instance {self}: Invalid status '{self.status}'"
            )
