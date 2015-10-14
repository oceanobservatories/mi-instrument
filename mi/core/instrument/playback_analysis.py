#!/usr/bin/env python
import io
import struct
import os
import re
from datetime import datetime, timedelta
import sys
from tqdm import tqdm

__author__ = 'petercable'

datere = re.compile('(\d{8}T\d{4}_UTC)')
binary_sync = '\xa3\x9d\x7a'
file_scan_depth = 256000


def lrc(data, seed=0):
    for b in bytearray(data):
        seed ^= b
    return seed


def find_sensor(filename):
    if '_' in filename:
        return filename.split('_')[0]


def find_time_range(filename):
    # initial implementation will assume 1 day per file
    match = datere.search(filename)
    if match:
        dt = datetime.strptime(match.group(1), '%Y%m%dT%H%M_%Z')
        return dt, dt + timedelta(1)


def check_chunk_ascii(chunk):
    return 'OOI-TS' in chunk


def check_chunk_binary(chunk, fh):
    # look for binary sync
    sync_index = chunk.find(binary_sync)
    if sync_index != -1:
        # make sure we have enough bytes to read the packet size
        if len(chunk) < sync_index + 4:
            chunk += fh.read(4)

        if len(chunk) >= sync_index + 4:
            packet_size = struct.unpack_from('>H', chunk, sync_index+4)[0]
            # make sure we have at least packet size bytes
            if len(chunk) < sync_index + packet_size:
                chunk += fh.read(packet_size)

            if len(chunk) >= sync_index + packet_size:
                # The LRC of the entire packet should be 0 if this is a valid packet
                if lrc(chunk[sync_index:sync_index+packet_size]) == 0:
                    return True
    return False


def find_type(path, f):
    found_ascii = False
    found_binary = False
    ffw = False
    with io.open(os.path.join(path, f), 'rb') as fh:
        while not all((found_ascii, found_binary)):
            chunk = fh.read(256)
            if chunk == '':
                break

            if not found_ascii:
                found_ascii = check_chunk_ascii(chunk)
            if not found_binary:
                found_binary = check_chunk_binary(chunk, fh)

            # If we have scanned through the file to at least file_scan_depth
            # then seek file_scan_depth from the end of file and search from
            # there
            if not ffw and fh.tell() >= file_scan_depth:
                ffw = True
                here = fh.tell()
                fh.seek(-file_scan_depth, io.SEEK_END)
                # go back where we were if the file happens to be smaller than
                # file_scan_depth x 2
                if fh.tell() < here:
                    fh.seek(here)

    return found_ascii, found_binary


def walk_tree(root, sensor):
    found = []
    for path, dirs, files in os.walk(root):
        files = [f for f in files if f.endswith('.dat')]
        if sensor is not None:
            files = [f for f in files if sensor in f]
        if files:
            print('Processing %d files in %s: ' % (len(files), path))
            for f in tqdm(files, leave=True):
                sensor = find_sensor(f)
                time_range = find_time_range(f)
                if time_range:
                    start, stop = time_range
                    record_type = find_type(path, f)
                    found.append((sensor, start, stop, record_type, os.path.join(path, f)))
    return found


def analyze(items):
    total_size = 0
    items_start = None
    items_stop = None
    count = 0
    for _, start, stop, _, path in items:
        size = os.stat(path).st_size
        if size == 0:
            continue
        if size < 1000:
            # attempt to filter out "empty" files
            valid = False
            with open(path) as fh:
                for line in fh:
                    if line.startswith('####'):
                        continue
                    if line.strip() == '':
                        continue
                    valid = True
            if not valid:
                continue

        total_size += size
        count += 1
        if items_start is None:
            items_start = start
        else:
            items_start = min(items_start, start)
        if items_stop is None:
            items_stop = stop
        else:
            items_stop = max(items_stop, stop)

    return count, total_size, items_start, items_stop


def main():
    root = sys.argv[1]
    sensor = None
    if len(sys.argv) > 2:
        sensor = sys.argv[2]
    found = walk_tree(root, sensor)
    found.sort()
    results = {}
    for _ in found:
        sensor, start, stop, record_type, path = _
        if record_type == (False, False):
            results.setdefault(sensor, {}).setdefault('chunky', []).append(_)
        elif record_type == (True, False):
            results.setdefault(sensor, {}).setdefault('ascii', []).append(_)
        else:
            results.setdefault(sensor, {}).setdefault('binary', []).append(_)

    for sensor in results:
        print sensor
        print 'chunky', analyze(results[sensor].get('chunky', []))
        print 'ascii', analyze(results[sensor].get('ascii', []))
        print 'binary', analyze(results[sensor].get('binary', []))


if __name__ == '__main__':
    main()
