#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Copyright (C) Josh Sunnex

SPDX-License-Identifier: GPL-3.0-only
"""

import asyncio
import queue
import threading
from types import SimpleNamespace

from peewee import SqliteDatabase
import pytest

from unmanic.libs import library_workload, libraryscanner, plugins, postprocessor
from unmanic.libs.unmodels.libraries import Libraries
from unmanic.libs.unmodels.tasks import Tasks
from unmanic.libs.unplugins import plugin_types
from unmanic.webserver.api_v2.pending_api import ApiPendingHandler
from unmanic.webserver.api_v2.schema.schemas import RequestLibraryWorkloadSchema


@pytest.fixture
def database():
    database = SqliteDatabase(':memory:')
    with database.bind_ctx([Libraries, Tasks]):
        database.create_tables([Libraries, Tasks])
        Libraries.create(id=1, name='Source', path='/source')
        Libraries.create(id=2, name='Processing', path='/processing')
        yield database
    database.close()


def scanner(state='idle'):
    return SimpleNamespace(get_scan_status=lambda: {'state': state})


def test_query_counts_every_outstanding_stage_and_filters_libraries(database):
    for state in (*library_workload.ACTIVE_STATES, 'complete'):
        Tasks.create(abspath='/file-' + state, library_id=1, status=state)
    Tasks.create(abspath='/other', library_id=2, status='pending')
    workload = library_workload.get_library_workload([1], scanner())
    assert not workload['idle']
    assert workload['libraries'] == [{'library_id': 1, 'outstanding_tasks': 4, 'idle': False,
                                     'tasks': {state: 1 for state in library_workload.ACTIVE_STATES}}]
    assert len(library_workload.get_library_workload([], scanner())['libraries']) == 2


@pytest.mark.parametrize('state', ['scheduled', 'scanning', 'paused', 'cancelling'])
def test_a_scan_blocks_idle_even_when_selected_libraries_have_no_tasks(database, state):
    assert not library_workload.get_library_workload([1], scanner(state))['idle']
    assert library_workload.get_library_workload([1], scanner())['idle']


@pytest.mark.parametrize('ids', [[True], [-1], ['1'], '1', [999]])
def test_invalid_or_unknown_libraries_do_not_look_idle(database, ids):
    with pytest.raises(ValueError):
        library_workload.get_library_workload(ids, scanner())


def test_processed_task_is_outstanding_until_cleanup(database):
    task = Tasks.create(abspath='/output', library_id=2, status='processed', success=True)
    assert not library_workload.get_library_workload([2], scanner())['idle']
    task.delete_instance()
    assert library_workload.get_library_workload([2], scanner())['idle']


def test_missing_scanner_does_not_report_idle(database, monkeypatch):
    from unmanic.libs.uiserver import UnmanicRunningTreads
    monkeypatch.setattr(UnmanicRunningTreads, 'get_unmanic_running_thread', lambda *args: None)
    assert library_workload.get_library_workload([1])['scan_state'] == 'unavailable'
    assert not library_workload.get_library_workload([1])['idle']


def test_notification_reports_only_idle_libraries(database, monkeypatch):
    events = []
    monkeypatch.setattr(plugins, 'PluginsHandler', lambda: SimpleNamespace(
        run_event_plugins_for_plugin_type=lambda name, data: events.append((name, data))))
    Tasks.create(abspath='/busy', library_id=2, status='processed')
    library_workload.notify_library_workload_idle(scanner=scanner('scanning'))
    assert events == []
    library_workload.notify_library_workload_idle(scanner=scanner())
    assert events[0][0] == 'events.library_workload_idle'
    assert events[0][1]['library_ids'] == [1]
    assert not events[0][1]['workload']['idle']


def test_full_scan_notifies_after_idle_even_without_any_tasks(database, monkeypatch):
    events = []
    monkeypatch.setattr(plugins, 'PluginsHandler', lambda: SimpleNamespace(
        run_event_plugins_for_plugin_type=lambda name, data: events.append((name, data))))
    manager = object.__new__(libraryscanner.LibraryScannerManager)
    manager.scan_state_lock = threading.RLock()
    manager.scan_requested = True
    manager.scan_state = 'scheduled'
    manager.scan_pause_flag = threading.Event()
    manager.scan_cancel_flag = threading.Event()
    manager.files_to_test = queue.Queue()
    manager.files_to_process = queue.Queue()
    manager.current_library_id = manager.current_library_name = manager.current_library_path = None
    manager.run_scheduled_scan = lambda: None
    manager.logger = SimpleNamespace(exception=lambda *args: pytest.fail('Notification failed'))
    manager.execute_scan_request()
    assert manager.get_scan_status()['state'] == 'idle'
    assert events[0][1]['reason'] == 'scan_complete'
    assert events[0][1]['library_ids'] == [1, 2]


@pytest.mark.parametrize('task_type', ['local', 'remote'])
def test_postprocessor_notifies_only_after_task_cleanup(database, monkeypatch, task_type):
    row = Tasks.create(abspath='/output', library_id=2, status='processed', success=True)
    events = []
    abort = threading.Event()
    def remove():
        row.delete_instance()
        abort.set()
    def complete(status):
        row.status = status
        row.save()
        abort.set()
    task = SimpleNamespace(get_task_library_id=lambda: 2, get_task_id=lambda: row.id,
                           get_task_type=lambda: task_type, get_cache_path=lambda: '/cache',
                           get_source_data=lambda: {}, get_source_abspath=lambda: '/output', delete=remove, set_status=complete)
    class Handler:
        def run_event_plugins_for_plugin_type(self, name, data):
            if name == 'events.library_workload_idle':
                assert not Tasks.select().where(Tasks.id == row.id, Tasks.status != 'complete').exists()
            events.append((name, data))
    monkeypatch.setattr(plugins, 'PluginsHandler', Handler)
    monkeypatch.setattr(postprocessor, 'PluginsHandler', Handler)
    from unmanic.libs.uiserver import UnmanicRunningTreads
    monkeypatch.setattr(UnmanicRunningTreads, 'get_unmanic_running_thread', lambda *args: scanner())
    processor = object.__new__(postprocessor.PostProcessor)
    processor.abort_flag = abort
    processor.event = SimpleNamespace(wait=lambda *args: None)
    processor.logger = SimpleNamespace(info=lambda *args: None,
                                      exception=lambda *args: pytest.fail('Post-processing failed'))
    processor.system_configuration_is_valid = lambda: True
    processor.task_queue = SimpleNamespace(task_list_processed_is_empty=lambda: False,
                                          get_next_processed_tasks=lambda: task)
    processor.post_process_file = processor.write_history_log = processor.commit_task_metadata = lambda: None
    processor.post_process_remote_file = processor.dump_history_log = lambda: None
    processor.run()
    assert events[-1][0] == 'events.library_workload_idle'
    assert events[-1][1]['library_ids'] == [2]


def test_query_api_and_registered_plugin_event(database):
    written = []
    handler = SimpleNamespace(read_json_request=lambda schema: schema.load({'library_ids': [2]}),
                              library_scanner=scanner(), write=written.append)
    asyncio.run(ApiPendingHandler.get_library_workload(handler))
    assert written[0]['idle']
    assert written[0]['libraries'][0]['library_id'] == 2
    assert plugin_types.grab_module('events.library_workload_idle').plugin_runner() == 'emit_library_workload_idle'
    with pytest.raises(Exception):
        RequestLibraryWorkloadSchema().load({'library_ids': [True]})


def test_scan_finishing_during_query_does_not_hide_work_created_after_count(database):
    states = iter(['scanning', 'idle'])
    manager = SimpleNamespace(get_scan_status=lambda: {'state': next(states)})
    assert not library_workload.get_library_workload([1], manager)['idle']
