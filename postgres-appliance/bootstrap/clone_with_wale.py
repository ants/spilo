#!/usr/bin/env python

import argparse
import csv
import logging
import os
import re
import subprocess
import sys

from maybe_pg_upgrade import call_maybe_pg_upgrade

from collections import namedtuple
from dateutil.parser import parse

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

LATEST_TIMELINE_PATH = '/tmp/latest.history'


def read_configuration():
    parser = argparse.ArgumentParser(description="Script to clone from S3 with support for point-in-time-recovery")
    parser.add_argument('--scope', required=True, help='target cluster name')
    parser.add_argument('--datadir', required=True, help='target cluster postgres data directory')
    parser.add_argument('--recovery-target-time',
                        help='the timestamp up to which recovery will proceed (including time zone)',
                        dest='recovery_target_time_string')
    parser.add_argument('--recovery-target-timeline',
                        help='the timeline where to restore to in hexadecimal. Default is latest.',
                        dest='recovery_target_timeline_string')
    parser.add_argument('--wale-envdir', required=True, help='wal-e environment directory for restore command',
                        dest="wale_envdir")
    parser.add_argument('--dry-run', action='store_true', help='find a matching backup and build the wal-e '
                                                               'command to fetch that backup without running it')
    args = parser.parse_args()

    options = namedtuple('Options', 'name datadir recovery_target_time recovery_target_timeline wale_envdir dry_run')
    if args.recovery_target_time_string:
        recovery_target_time = parse(args.recovery_target_time_string)
        if recovery_target_time.tzinfo is None:
            raise Exception("recovery target time must contain a timezone")
    else:
        recovery_target_time = None

    if args.recovery_target_timeline_string and args.recovery_target_timeline_string != 'latest':
        try:
            recovery_target_timeline = int(args.recovery_target_timeline_string, 16)
        except ValueError:
            raise Exception("recovery target timeline must be hexadecimal number or 'latest'")
    else:
        recovery_target_timeline = None

    return options(args.scope, args.datadir, recovery_target_time, recovery_target_timeline, args.wale_envdir,
                   args.dry_run)


def build_wale_command(command, datadir=None, backup=None):
    cmd = ['wal-g' if os.getenv('USE_WALG_RESTORE') == 'true' else 'wal-e'] + [command]
    if command == 'backup-fetch':
        if datadir is None or backup is None:
            raise Exception("backup-fetch requires datadir and backup arguments")
        cmd.extend([datadir, backup])
    elif command != 'backup-list':
        raise Exception("invalid {0} command {1}".format(cmd[0], command))
    return cmd


def fix_output(output):
    """WAL-G is using spaces instead of tabs and writes some garbage before the actual header"""

    started = None
    for line in output.decode('utf-8').splitlines():
        if not started:
            started = re.match('^name\s+last_modified\s+', line)
        if started:
            yield '\t'.join(line.split())


def choose_backup(output, recovery_target_time, target_timeline, timeline_history):
    """ pick up the latest backup file starting before time recovery_target_time

    if target timeline and timeline history are specified will return only backups on that
    timeline
    """
    reader = csv.DictReader(fix_output(output), dialect='excel-tab')
    backup_list = list(reader)
    if len(backup_list) <= 0:
        raise Exception("wal-e could not found any backups")
    match_timestamp = match_timeline = match = None
    for backup in backup_list:
        backup_timeline, backup_stop_lsn = parse_stop_point(backup['wal_segment_backup_stop'],
                                                            backup['wal_segment_offset_backup_stop'])
        if timeline_history:
            if backup_timeline != target_timeline and backup_timeline not in timeline_history:
                # Backup is not in current history and is not usable for recovery
                continue
            if timeline_history[backup_timeline] < backup_stop_lsn:
                # Backup was taken after target timeline split off
                continue
        last_modified = parse(backup['last_modified'])
        if not recovery_target_time or last_modified < recovery_target_time:
            if match is None or last_modified > match_timestamp:
                match = backup
                match_timestamp = last_modified
                match_timeline = backup_timeline
    if match is None:
        raise Exception("wal-e could not found any backups prior to the point in time {0}".format(recovery_target_time))
    return match['name'], match_timeline


def run_clone_from_s3(options):
    if options.recovery_target_timeline:
        # Exact target timeline specified
        target_timeline = options.recovery_target_timeline
        if not fetch_timeline(target_timeline):
            raise Exception("Fetching timeline history failed")
        timeline_history = parse_timeline_history()
    elif options.recovery_target_time:
        # TODO: Would need to check modification dates of timeline history files to see which timeline was active at
        # target time. As a temporary measure, use the timeline of latest backup.
        target_timeline = timeline_history = None
    else:
        target_timeline = find_latest_timeline()
        timeline_history = parse_timeline_history()

    backup_list_cmd = build_wale_command('backup-list')
    backup_list = subprocess.check_output(backup_list_cmd)
    backup_name, backup_timeline = choose_backup(backup_list, options.recovery_target_time,
                                                 target_timeline, timeline_history)
    backup_fetch_cmd = build_wale_command('backup-fetch', options.datadir, backup_name)
    logger.info("cloning cluster %s using %s", options.name, ' '.join(backup_fetch_cmd))
    if not options.dry_run:
        ret = subprocess.call(backup_fetch_cmd)
        if ret != 0:
            raise Exception("wal-e backup-fetch exited with exit code {0}".format(ret))

    write_recovery_conf(options, target_timeline)

    return 0


def parse_lsn(lsn_str):
    a, b = lsn_str.split("/")
    return int(a, 16) << 32 + int(b, 16)


def parse_stop_point(segment, offset):
    timeline = int(segment[0:8], 16)
    # TODO: handle nonstandard segment sizes
    lsn = int(segment[8:], 16) << 24 + int(offset, 16)
    return timeline, lsn


def parse_timeline_history(path):
    history = {}
    with open(path) as fd:
        for line in fd:
            if line.strip():
                row = line.split("\t")
                history[int(row[0])] = parse_lsn(row[1])
    return history


def fetch_timeline(timeline_id):
    wal_cmd = build_wale_command('wal-fetch')
    return subprocess.call(wal_cmd + ['{:08X}.history'.format(timeline_id + 1), LATEST_TIMELINE_PATH]) != 0


def find_latest_timeline():
    timeline_id = 1
    while True:
        if not fetch_timeline(timeline_id + 1):
            # Need to fetch again because unsuccessful fetch deleted the last one
            if not fetch_timeline(timeline_id):
                raise Exception("Refetching timeline history failed")
            return timeline_id
        timeline_id += 1


def write_recovery_conf(options, target_timeline):
    recovery_conf = {
        'restore_command': 'envdir "{}" /scripts/restore_command.sh "%f" "%p"'.format(options.wale_envdir),
        'recovery_target_timeline': str(target_timeline),
    }
    if False: #TODO: options.pause_at_recovery_target
        recovery_conf['pause_at_recovery_target'] = 'false'
    else:
        recovery_conf['recovery_target_action'] = 'promote'
    if options.recovery_target_time:
        recovery_conf['recovery_target_time'] = options.recovery_target_time.strftime("%Y-%m-%dT%H:%M:%S%z")
    if False: #TODO: options.target_exclusive
        recovery_conf['recovery_target_inclusive'] = 'false'
    with open(os.path.join(options.datadir, 'recovery.conf')) as fd:
        for k, v in recovery_conf.items():
            fd.write("{} = '{}'\n".format(k, v))


def main():
    options = read_configuration()
    try:
        run_clone_from_s3(options)
    except Exception:
        logger.exception("Clone failed")
        return 1
    return call_maybe_pg_upgrade()


if __name__ == '__main__':
    sys.exit(main())
