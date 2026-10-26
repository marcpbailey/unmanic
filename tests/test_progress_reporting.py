#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Copyright (C) Josh Sunnex

SPDX-License-Identifier: GPL-3.0-only
"""

import multiprocessing
import queue
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

from unmanic.libs import libraryscanner, plugins
from unmanic.libs.unplugins import child_process
from unmanic.libs.workers import WorkerSubprocessMonitor
from unmanic.webserver.api_v2.schema.schemas import LibraryScanStatusSchema


def test_completion_is_visible_without_blocking_dependency_idle():
    manager = object.__new__(libraryscanner.LibraryScannerManager)
    manager.scan_state_lock = threading.RLock()
    manager.abort_flag = threading.Event()
    manager.scan_requested = True
    manager.scan_state = 'scheduled'
    manager.scan_pause_flag = threading.Event()
    manager.scan_cancel_flag = threading.Event()
    manager.files_to_test = queue.Queue()
    manager.files_to_process = queue.Queue()
    manager.current_library_id = manager.current_library_name = manager.current_library_path = None
    manager.run_scheduled_scan = lambda: None
    manager.logger = SimpleNamespace(exception=lambda *args: None)
    observed = []

    def completion(**kwargs):
        manager.report_completion_progress(plugin_name='Example plugin')
        manager.report_completion_progress(message='Checking media', current=2, total=8, unit='files')
        status = LibraryScanStatusSchema().dump(manager.get_scan_status())
        assert status['state'] == 'idle'
        assert not status['can_rescan']
        assert status['completion']['current'] == 2
        assert status['completion']['plugin_name'] == 'Example plugin'
        observed.append(status)
        raise RuntimeError('Completion failure must also clear the display')

    with patch('unmanic.libs.library_workload.notify_library_workload_idle', side_effect=completion):
        manager.execute_scan_request()
    assert observed
    assert manager.get_scan_status()['completion'] is None
    assert manager.get_scan_status()['can_rescan']


def child_work(log_queue=None, prog_queue=None):
    log_queue.put('Checking media')
    prog_queue.put({'message': 'Checking media: 1/4', 'percent': 25})
    time.sleep(.2)
    prog_queue.put({'message': 'Generating guide', 'percent': None})
    time.sleep(1.1)
    prog_queue.put({'message': 'Publishing: 3/4', 'percent': 75, 'scope': 'phase'})
    time.sleep(.2)


def test_python_child_reports_unknown_and_measured_progress_with_a_tracked_pid():
    parent = SimpleNamespace(event=threading.Event(), redundant_flag=threading.Event(), paused_flag=threading.Event())
    monitor = WorkerSubprocessMonitor(parent)
    snapshots = []

    def parser(text, **kwargs):
        value = monitor.default_progress_parser(text, **kwargs)
        if 'pid' not in kwargs and not kwargs.get('unset'):
            snapshots.append(monitor.get_subprocess_stats())
        return value

    assert monitor.get_subprocess_stats()['percent'] == ''
    assert parser('0')['percent'] == '0'
    assert parser('')['percent'] == ''
    with multiprocessing.Manager() as manager:
        previous = child_process._shared_manager
        child_process.set_shared_manager(manager)
        try:
            data = {'worker_log': [], 'current_command': [], 'command_progress_parser': parser}
            process = child_process.PluginChildProcess('example', data)
            with patch('multiprocessing.Process', multiprocessing.get_context('fork').Process):
                assert process.run(child_work)
            assert 'Checking media\n' in data['worker_log']
        finally:
            child_process.set_shared_manager(previous)
    reported = [item for item in snapshots if item['pid'] != 'None']
    assert [item['percent'] for item in reported] == ['25', '', '75']
    assert reported[-1]['elapsed'] >= 1
    assert reported[-1]['progress_scope'] == 'phase'
    assert monitor.get_subprocess_stats()['percent'] == ''
