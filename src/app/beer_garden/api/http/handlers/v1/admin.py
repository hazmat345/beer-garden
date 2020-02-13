# -*- coding: utf-8 -*-
from brewtils.errors import ModelValidationError
from brewtils.models import Operation
from brewtils.schema_parser import SchemaParser

from beer_garden.api.http.authorization import check_permission, Permissions
from beer_garden.api.http.base_handler import BaseHandler


class AdminAPI(BaseHandler):
    async def patch(self):
        """
        ---
        summary: Initiate a rescan of the plugin directory
        description: |
          The body of the request needs to contain a set of instructions
          detailing the operations to perform.

          Currently the only operation supported is `rescan`:
          ```JSON
          [
            { "operation": "rescan" }
          ]
          ```
          * Will remove from the registry and database any currently stopped
            plugins who's directory has been removed.
          * Will add and start any new plugin directories.
        parameters:
          - name: patch
            in: body
            required: true
            description: Instructions for operations
            schema:
              $ref: '#/definitions/Patch'
        responses:
          204:
            description: Rescan successfully initiated
          50x:
            $ref: '#/definitions/50xError'
        tags:
          - Admin
        """
        operations = SchemaParser.parse_patch(
            self.request.decoded_body, many=True, from_string=True
        )
        check_permission(self.current_user, [Permissions.SYSTEM_CREATE])

        for op in operations:
            if op.operation == "rescan":
                response = await self.client(Operation(forward_type="SYSTEM_RESCAN"))
            else:
                raise ModelValidationError(f"Unsupported operation '{op.operation}'")

        self.set_status(204)
