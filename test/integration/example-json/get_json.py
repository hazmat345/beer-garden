#!/usr/bin/env python

import json
import os
import sys

from brewtils import get_easy_client
from brewtils.schema_parser import SchemaParser


def main():
    client = get_easy_client(cli_args=sys.argv[1:])

    json_dir = os.path.abspath('./jsons')
    os.makedirs(json_dir, exist_ok=True)

    systems = client.find_systems()

    for system in systems:
        with open(os.path.join(json_dir, system.name+'.json'), 'w') as f:
            json.dump(SchemaParser.serialize_system(system, to_string=False),
                     f, sort_keys=True, indent=2)

        for command in system.commands:
            filename = os.path.join(json_dir, system.name, command.name+'.json')
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            with open(filename, 'w') as f:
                json.dump(SchemaParser.serialize_command(command, to_string=False),
                          f, sort_keys=True, indent=2)


if __name__ == '__main__':
    main()
