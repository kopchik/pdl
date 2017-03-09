#!/usr/bin/env python3
# TODO: allow for changing chunk size
# TODO: separate loop for locks
# TODO: sessions

from os.path import basename, exists, isfile
from urllib.parse import urlparse
from os import unlink
import logging as log
import argparse
import atexit
import pickle
import time


from threading import Lock
import aiohttp
import asyncio

from chunk import chunkize, merge

VERSION = 6
MEG = 1 * 1024 * 1024
CHUNKSIZE = 5   # in megabytes
WORKERS = 5
MINREAD = 1448 * 100  # in bytes

MINBACKOFF = 1
MAXBACKOFF = 30
BFACTOR = 1.5


class Status:
    # __slots__ = ['fd', 'size', 'queue', 'completed', 'lock', 'url']

    def __init__(self, size, chunksize):
        self.fd = None
        self.size = size
        self.queue = chunkize(size, chunksize)
        log.debug("chunks to download: %s" % self.queue)
        self.chunksize = chunksize
        self.completed = []
        self.lock = Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        state['fd'] = None
        state['queue'] = None
        state['lock'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.rechunkize()
        self.lock = Lock()

    def status(self):
        # TODO: potentially long computation
        downloaded = sum(stop - start for start, stop in self.completed)
        total = self.size
        return downloaded, total

    def rechunkize(self, chunksize):
        self.completed = merge(self.completed)

        self.chunksize = chunksize


async def worker(st):
    backoff = MINBACKOFF
    while True:
        # thread-safe way to pop element from the queue
        try:
            start, stop = st.queue.pop(0)
        except IndexError:
            log.debug("worker complete")
            break

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    headers = {'Range': 'bytes=%s-%s' % (start, stop)}
                    r = await session.get(st.url, headers=headers)
                    log.debug("downloading bytes %s - %s" % (start, stop))
                    chunks = []
                    while True:
                        chunk = await r.content.read(10000)  # TODO: size
                        if not chunk:
                            await r.release()
                            break
                        chunks.append(chunk)
                    break
            except Exception as err:
                log.error("error fetching chunk {chunk}: {err}, sleeping {sleep}"
                          .format(chunk=(start, stop), err=err, sleep=backoff))
                time.sleep(backoff)
                backoff = min(backoff * BFACTOR, MAXBACKOFF)
                continue

        assert sum(len(c) for c in chunks) == stop - start + 1
        backoff = MINBACKOFF

        with st.lock:
            st.fd.seek(start)
            for chunk in chunks:
                st.fd.write(chunk)
        st.completed.append((start, stop))
        log.debug("complete %s - %s" % (start, stop))


@asyncio.coroutine
def output_status(status):
    old_downloaded = 0
    old_ts = time.time()
    speed = 0
    while True:
        downloaded, total = status.status()
        percentage = downloaded / total
        if downloaded != old_downloaded:
            now = time.time()
            speed = (downloaded - old_downloaded) / (now - old_ts) // 1000
            old_ts = now
            old_downloaded = downloaded
        print("\rprogress: {:,}/{:,} {:.2%} ({:,}KB/s)"
              .format(downloaded, total, percentage, speed), end='')
        yield from asyncio.sleep(5)


@asyncio.coroutine
def downloader(loop=None, num_workers=3, chunksize=5 * MEG, url=None, out=None):
    # calculate download filename
    r = urlparse(url)                       # request object from urllib
    outfile = out or basename(r.path)       # download file name
    statusfile = outfile + ".download"      # track downloaded chunks
    log.info("url: '%s'" % url)
    if exists(outfile) and not exists(statusfile):
        log.info("It seems file already downloaded as '%s'" % outfile)
        return
    log.info("saving to '%s'" % outfile)

    # check for stalled status file
    if not isfile(outfile) and isfile(statusfile):
        raise Exception("There is a status file (\"%s\"),"
                        "but no output file (\"%s\"). "
                        "Please stalled status file." % (statusfile, outfile))

    # get file size
    r = yield from aiohttp.head(url)
    rawsize = r.headers.get('Content-Length', None)
    r.close()
    assert rawsize, "No Content-Length header"
    size = int(rawsize)
    assert size < 20000 * MEG, "very large file, are you sure?"
    log.info("download size: %s bytes" % size)

    # load status from file or create new
    try:
        status = pickle.load(open(statusfile, "rb"))
        log.debug("status restored from %s" % statusfile)
        assert status.size == size,  \
            "cannot resume download:"  \
            "original file had %s size, this one is %s" \
            % (status.size, size)
        if chunksize != status.chunksize:
            log.info("chunk size: %s => %s" % (chunksize, status.chunksize))
            status.rechunkize(chunksize)
    except FileNotFoundError:
        status = Status(size, chunksize)
    except Exception as err:
        log.error("error unpickling db: %s" % err)
        return False
    status.url = url

    # save status when interrupted
    #@asyncio.coroutine
    def save_status():
        with status.lock:
            log.info("\n\nsaving state to %s\n" % statusfile)
            pickle.dump(status, open(statusfile, "wb"))
    atexit.register(save_status)

    # open file for writing and launch workers
    # open() does not support O_CREAT
    mode = "rb+" if isfile(outfile) else "wb"
    # with open(outfile, mode) as fd:
    #status.fd = fd
    status.fd = open(outfile, mode)
    status.fd.truncate(size)

    # start workers
    status_worker = loop.create_task(output_status(status))
    tasks = []
    for i in range(num_workers):
        t = loop.create_task(worker(status))
        tasks.append(t)

    while True:
        done, pending = yield from asyncio.wait(tasks)
        print(done, pending)
        #TODO("check download complete")
        break

    status_worker.cancel()
    # yield from asyncio.sleep(1)
    log.info("\ndownload finished")
    atexit.unregister(save_status)
    try:
        unlink(statusfile)
    except FileNotFoundError:
        pass

    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='The best downloader. Ever. Version %s.' % VERSION)
    parser.add_argument('-o', '--output', type=str, default=None,
                        help="where to store the downloaded content")
    parser.add_argument('-w', '--workers', type=int, default=WORKERS,
                        help="number of workers (default %s)" % WORKERS)
    parser.add_argument('-c', '--chunksize', type=int, default=CHUNKSIZE,
                        help='chunk size in megs (default %s)' % CHUNKSIZE)
    parser.add_argument('-d', '--debug', default=False, const=True,
                        action='store_const',
                        help='enable debug messages')
    parser.add_argument('url', help='URL to download')
    args = parser.parse_args()

    if args.debug:
        log.root.setLevel("DEBUG")
        log.debug("debug output enabled")
    else:
        log.root.setLevel("INFO")
    loop = asyncio.get_event_loop()
    d = downloader(loop=loop, url=args.url, num_workers=args.workers,
                   chunksize=args.chunksize * MEG, out=args.output)
    try:
        loop.run_until_complete(d)
    except KeyboardInterrupt:
        pass
