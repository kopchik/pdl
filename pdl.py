#!/usr/bin/env python3
# TODO: allow for changing chunk size

from os.path import basename, exists, isfile
from urllib.request import urlopen, Request
from threading import Thread, Lock
from urllib.parse import urlparse
from functools import partial
from sys import stderr, exit
from os import unlink
import logging as log
import argparse
import atexit
import pickle
import time
import re

VERSION = 4
MEG = 1*1024*1024
CHUNKSIZE = 5   # in megabytes
WORKERS = 5


class Status:
  # __slots__ = ['fd', 'size', 'queue', 'completed', 'lock', 'url']
  def __init__(self, size):
    self.fd = None
    self.size = size
    self.queue = []
    self.completed = []
    self.lock = Lock()
    self.chunkize()

  def __getstate__(self):
    state = self.__dict__.copy()
    state['fd'] = None
    state['queue'] = None
    state['lock'] = None
    return state

  def __setstate__(self, state):
    self.__dict__.update(state)
    self.chunkize()
    self.lock = Lock()

  def chunkize(self, chunksize=CHUNKSIZE*MEG):
    self.queue = []
    start, stop = 0, min(self.size, chunksize)-1
    while True:
      chunk = (start, stop)
      if chunk not in self.completed:
        self.queue.append(chunk)
      if stop == self.size-1:
        break
      start = stop + 1
      stop  = min(self.size-1, stop+chunksize)
    log.debug("chunks to download: %s" % self.queue)

  def status(self):
    downloaded = sum(stop-start for start, stop in self.completed)
    total = self.size
    return downloaded, total


def worker(st):
  while True:
    try:
      start, stop = st.queue.pop(0)
    except IndexError:
      break
    req = Request(st.url)
    req.headers['Range'] = 'bytes=%s-%s' % (start, stop)
    resp = urlopen(req)
    log.debug("downloading bytes %s - %s" % (start, stop))
    data = resp.read()
    assert len(data) == stop - start + 1
    with st.lock:
      st.fd.seek(start)
      st.fd.write(data)
    st.completed.append((start, stop))
    log.debug("complete %s - %s" % (start, stop))


# output status
def output_status(status):
  while True:
    downloaded, total = status.status()
    percentage = downloaded / total
    print("\rprogress: {:,}/{:,} {:.2%}"
          .format(downloaded, total, percentage), end='')
    time.sleep(5)


def downloader(num_workers=3, chunksize=5*MEG, url=None, out=None):
  # calculate download filename
  r = urlparse(url)                       # request object from urllib
  outfile = out or basename(r.path)       # download file name
  statusfile = outfile + ".download"      # track downloaded chunks
  log.info("url: '%s'" % url)
  if exists(outfile) and not exists(statusfile):
    log.info("It seems file already downloaded as '%s'" % outfile)
    return None
  log.info("saving to '%s'" % outfile)

  # check for stalled status file
  if not isfile(outfile) and isfile(statusfile):
    raise Exception("There is a status file (\"%s\"),"
                    "but no output file (\"%s\"). "
                    "Please stalled status file." % (statusfile, outfile))

  # get file size
  response = urlopen(Request(url, method='HEAD'))
  rawsize = response.getheader('Content-Length')
  assert rawsize, "No Content-Length header"
  size = int(rawsize)
  assert size < 20000*MEG, "very large file, are you sure?"
  log.info("download size: %s bytes" % size)

  # load status from file or create new
  try:
    status = pickle.load(open(statusfile, "rb"))
    log.debug("status restored from %s" % statusfile)
    assert status.size == size,  \
        "cannot resume download:"  \
        "original file had %s size, this one is %s" \
        % (status.size, size)
  except FileNotFoundError:
    status = Status(size)
  except Exception as err:
    log.error("error unpickling db: %s" % err)
    return False

  status.url = url

  # save status when interrupted
  def save_status():
    with status.lock:
      log.info("saving state to %s" % statusfile)
      pickle.dump(status, open(statusfile, "wb"))
  atexit.register(save_status)

  # open file for writing and launch workers
  mode = "rb+" if isfile(outfile) else "wb"  # open() does not support O_CREAT
  with open(outfile, mode) as fd:
    status.fd = fd
    status.fd.truncate(size)
    workers = []

    # start workers
    Thread(target=output_status, args=(status,), daemon=True).start()
    global worker
    for i in range(num_workers):
      w = Thread(target=worker, args=(status,), daemon=True)
      w.start()
      workers.append(w)

    for worker in workers:
      worker.join()
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
  kwargs = dict(url=args.url, num_workers=args.workers,
                chunksize=args.chunksize*MEG, out=args.output)
  downloader = Thread(target=downloader, kwargs=kwargs, daemon=True)
  downloader.start()
  try:
    downloader.join()
  except KeyboardInterrupt:
    pass
