# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta
from typing import Tuple, List

from brewtils.stoppable_thread import StoppableThread
from mongoengine import Q

from beer_garden.db.sql.models import Event, Request


class SqlPruner(StoppableThread):
    def __init__(self, tasks=None, run_every=None):
        self.logger = logging.getLogger(__name__)
        self.display_name = "SQL Pruner"
        self._run_every = (run_every or timedelta(minutes=15)).total_seconds()
        self._tasks = tasks or []

        super(SqlPruner, self).__init__(logger=self.logger, name="Remover")
