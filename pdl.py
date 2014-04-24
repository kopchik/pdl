#!/usr/bin/env python3

from urllib.request import urlopen, Request
from urllib.parse import urlparse
from threading import Thread, Lock
from functools import partial
from os.path import basename
from sys import stderr
from os import unlink
import argparse
import atexit
import pickle
import re

VERSION = 2
MEG = 1*1024*1024
CHUNKSIZE = 5   # in megabytes
WORKERS = 5
class Log:
  lvlmap = {lvl:i for i,lvl in enumerate("debug info warning error".split())}

  def __init__(self):
    self._verb = 1

  def log(self, msg, lvl=1):
    if self.lvlmap[lvl] < self._verb: return
    msg = "{lvl}: {msg}".format(lvl=lvl.upper(), msg=msg)
    print(msg, file=stderr)

  def __getattr__(self, lvl):
    return partial(self.log, lvl=lvl)

  def verbosity(self, lvl):
    assert lvl in self.lvlmap
    self._verb = self.lvlmap[lvl]
log = Log()


def chunkize(size, completed, chunksize=CHUNKSIZE):
  chunklist = []
  start, stop = 0, min(size, chunksize)-1
  while True:
    chunk = (start, stop)
    if chunk not in completed:
      chunklist.append(chunk)
    if stop == size-1:
      break
    start = stop+1
    stop  = min(size-1, stop+chunksize)
  log.debug("chunks to download: %s" % chunklist)
  return chunklist


def worker(url, queue, completed, fd, lock):
  while True:
    try:
      start, stop = queue.pop(0)
    except IndexError:
      break
    req = Request(url)
    req.headers['Range'] = 'bytes=%s-%s' % (start, stop)
    resp = urlopen(req)
    log.debug("downloading bytes %s - %s" % (start,stop))
    data = resp.read()
    assert len(data) == stop - start + 1
    with lock:
      fd.seek(start)
      fd.write(data)
    completed.append((start, stop))
    log.debug("complete %s - %s" % (start,stop))


class Downloader(Thread):
  def __init__(self, url, num_workers, chunksize):
    super().__init__()
    self.url = url
    self.num_workers = num_workers
    self.chunksize = chunksize


  def run(self):
    lock = Lock()
    url = self.url
    r = urlparse(url)
    outfile = basename(r.path)
    statusfile = outfile+".download"
    log.info("url: %s" % url)
    log.info("saving to %s" % outfile)
    response = urlopen( Request(url, method='HEAD') )
    rawsize = response.getheader('Content-Length')
    assert rawsize, "No Content-Length header"
    size = int(rawsize)
    log.info("download size %s" % size)

    def save_status():
      pickle.dump(completed, open(statusfile, "wb"))
    atexit.register(save_status)

    completed = []
    try:
      completed = pickle.load(open(statusfile, "rb"))
    except Exception as err:
      log.error("error unpickling db: %s" % err)
    queue = chunkize(size, completed, self.chunksize)

    with open(outfile, "w+b") as fd:
      fd.truncate(size)
      workers = []
      global worker
      for i in range(self.num_workers):
        w = Thread(target=worker, args=(url, queue, completed, fd, lock), daemon=True)
        w.start()
        workers.append(w)

      for worker in workers:
        worker.join()
      log.info("download finished")
      atexit.unregister(save_status)
      try:
        unlink(statusfile)
      except FileNotFoundError:
        pass


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='The best downloader. Ever. Version %s.' % VERSION)
  parser.add_argument('-w', '--workers', type=int, default=WORKERS,
                      help="number of workers (default %s)" % WORKERS)
  parser.add_argument('-c', '--chunksize', type=int, default=CHUNKSIZE,
                      help='chunk size in megs (default %s)' % CHUNKSIZE)
  parser.add_argument('-d', '--debug', default=False, const=True, action='store_const', help='enable debug mode')
  parser.add_argument('url', help='URL to download')
  args = parser.parse_args()

  if args.debug:
    log.verbosity("debug")
    log.debug("debug output enabled")

  downloader = Downloader(args.url, args.workers, args.chunksize*MEG)
  downloader.start()
  downloader.join()
