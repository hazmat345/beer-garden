# -*- coding: utf-8 -*-
from beer_garden.__version__ import __version__

__all__ = ["__version__", "application", "database_type"]

# COMPONENTS #
application = None
database_type = None


def signal_handler(_signal_number, _stack_frame):
    application.stop()
