import tempfile
import subprocess
import yaml
import logging
import time
import gevent.queue
import fcntl
import sys
import errno
from gevent import monkey, socket
from os import stat, path, system, O_NONBLOCK

def converter(queue):
    LOGGER.debug('converter started')
    while True:
        data = queue.get()
        LOGGER.debug('new data for conversion')
        if data == StopIteration:
            queue.task_done()
            break
        LOGGER.debug('flv file: %s' % path.abspath(data['source_file'].name))
        LOGGER.debug('target file: %s' % data['target_file'])
        ffmpeg_args = ['ffmpeg',
                       '-i', path.abspath(data['source_file'].name),
                       '-vn',
                       '-acodec', data['acodec'],
                       '-aq', data['quality'],
                       '-y', data['target_file']]

        p = subprocess.Popen(ffmpeg_args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
        fcntl.fcntl(p.stdin, fcntl.F_SETFL, O_NONBLOCK)
        fcntl.fcntl(p.stdout, fcntl.F_SETFL, O_NONBLOCK)
        p.stdin.close()

        output = ""

        while True:
            try:
                chunk = p.stdout.read(4096)
                if not chunk:
                    break
                output += chunk
            except IOError:
                ex = sys.exc_info()[1]
                if ex[0] != errno.EAGAIN:
                    raise
                sys.exc_clear()
            socket.wait_read(p.stdout.fileno())

        p.stdout.close()

        data['source_file'].close()
        LOGGER.debug('convertion done')
        queue.task_done()

def stream_handler(data, output_queue):
    LOGGER.debug('stream_handler')
    tmp_file = tempfile.NamedTemporaryFile(suffix='.flv')
    rtmpdump_args = ['rtmpdump',
                     '--live', '--quiet',
                     '--stop', data['length'],
                     '--rtmp', data['source'],
                     '--flv', path.abspath(tmp_file.name)]
    LOGGER.debug(" ".join(rtmpdump_args))

    p = subprocess.Popen(rtmpdump_args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    fcntl.fcntl(p.stdin, fcntl.F_SETFL, O_NONBLOCK)
    fcntl.fcntl(p.stdout, fcntl.F_SETFL, O_NONBLOCK)
    p.stdin.close()

    output = ""

    while True:
        try:
            chunk = p.stdout.read(4096)
            if not chunk:
                break
            output += chunk
        except IOError:
            ex = sys.exc_info()[1]
            if ex[0] != errno.EAGAIN:
                raise
            sys.exc_clear()
        socket.wait_read(p.stdout.fileno())

    p.stdout.close()

    data['source_file'] = tmp_file
    output_queue.put(data)
    LOGGER.debug('stream_handler done')

def scheduler():
    f = file('list.yml', 'r')
    recordings = yaml.load(f)
    now_recording = {}
    stream_handlers = []
    while True:
        for i, start_time in now_recording.items():
            if time.time() >= start_time + i[2] * 60:
                LOGGER.debug('removing %s' % str(i))
                now_recording.pop(i)

        for sh in stream_handlers:
            if sh.ready():
                LOGGER.debug('ready')
                stream_handlers.remove(sh)
            else:
                LOGGER.debug('not ready')

        if stat(f.name).st_mtime > time.time() - 60:
            f.seek(0)
            recordings = yaml.load(f)
            LOGGER.debug('zmiana')
        else:
            LOGGER.debug('brak zmiany')
        t = time.localtime()

        for station in recordings:
            if 'programs' not in station:
                continue
            for program in station['programs']:
                identifier = (station['source'],
                              "%d%d" % tuple(program['start'].values()[0:2]),
                              program['length'])
                if identifier not in now_recording and \
                             program['start']['hours'] == t.tm_hour and \
                             program['start']['minutes'] == t.tm_min and \
                             ('day' not in program['start'] or \
                              t.tm_wday + 1 in program['start']['day']):
                    LOGGER.debug('starting recording')
                    data = {'source': station['source'],
                            'target_file':
                            path.join(config['output_path'],
                                      "_".join([time.strftime("%Y.%m.%d-%H:%M"),
                                                station['name'],
                                                program['name'].replace(" ",
                                                                        "-")]) +
                                      '.' +
                                      config['audio']['ext']),
                            'length': str(program['length'] * 60),
                            'acodec': config['audio']['codec'],
                            'quality': str(config['audio']['q'])}
                    g = gevent.spawn(stream_handler, data, converter_queue)
                    LOGGER.debug('spawned')
                    stream_handlers.append(g)
                    now_recording[identifier] = time.time()
        gevent.sleep(60 - t.tm_sec)

monkey.patch_all()

logging.basicConfig(format='%(asctime)-15s %(message)s',
                    filename='log',
                    level=logging.DEBUG)
LOGGER = logging.getLogger('recorder')

with file('config.yml', 'r') as f:
    config = yaml.load(f)

LOGGER.debug('start')

converter_queue = gevent.queue.JoinableQueue()
c = gevent.spawn(converter, converter_queue)
s = gevent.spawn(scheduler)

c.join()
s.join()
