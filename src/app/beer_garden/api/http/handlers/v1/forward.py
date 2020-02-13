import json

from beer_garden.api.http.authorization import authenticated, Permissions
from beer_garden.api.http.base_handler import BaseHandler
from brewtils.schema_parser import SchemaParser


class ForwardAPI(BaseHandler):

    # @Todo Create new Persmission
    @authenticated(permissions=[Permissions.SYSTEM_UPDATE])
    async def post(self):
        """
        ---
        summary: Forward a request from a parent or child BG instance
        description: |
            When a Beer Garden needs to forward a request, this API will support routing
            to all CRUD actions exposed by the entry points.
        parameters:
          - name: forward
            in: body
            required: true
            description: The Forward Object
            schema:
                $ref: '#/definitions/Forward'
        responses:
          200:
            description: Forward Request Accepted
          400:
            $ref: '#/definitions/400Error'
          50x:
            $ref: '#/definitions/50xError'
        tags:
          - Forward
        """

        operation = SchemaParser.parse_operation(self.request.decoded_body, from_string=False)

        response = await self.client(operation)

        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write(response)
