#!/usr/bin/env python3

from urllib.request import urlopen, Request
from urllib.parse import urlparse
from threading import Thread, Lock
from os.path import basename
from os import unlink
import atexit
import pickle
import re

CHUNKSIZE = 1*1024*1024
#CHUNKSIZE = 3
WORKERS = 5


def chunkize(size, completed, chunksize=CHUNKSIZE):
  chunklist = []
  start, stop = 0, min(size, chunksize)-1
  while True:
    print("appending", start, stop)
    chunk = (start, stop)
    if chunk not in completed:
      chunklist.append(chunk)
    if stop == size-1:
      break
    start = stop+1
    stop  = min(size-1, stop+chunksize)
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
    print("downloading", start, "-", stop)
    data = resp.read()
    assert len(data) == stop - start + 1
    with lock:
      fd.seek(start)
      fd.write(data)
    completed.append((start, stop))
    print("complete", start, "-", stop)


class Downloader(Thread):
  def __init__(self, url):
    super().__init__()
    self.url = url

  def run(self):
    lock = Lock()
    url = self.url
    r = urlparse(url)
    outfile = basename(r.path)
    statusfile = outfile+".download"
    print("url is", url)
    print("output to", outfile)
    print("getting size... ", end='')
    response = urlopen( Request(url, method='HEAD') )
    rawsize = response.getheader('Content-Length')
    assert rawsize, "No Content-Length header"
    size = int(rawsize)
    print(size)

    def save_status():
      pickle.dump(completed, open(statusfile, "wb"))
    atexit.register(save_status)

    completed = []
    try:
      completed = pickle.load(open(statusfile, "rb"))
    except Exception as err:
      print("error unpickling db:", err)
    queue = chunkize(size, completed)

    with open(outfile, "ab") as fd:
      fd.truncate(size)
      workers = []
      global worker
      for i in range(WORKERS):
        w = Thread(target=worker, args=(url, queue, completed, fd, lock), daemon=True)
        w.start()
        workers.append(w)

      for worker in workers:
        worker.join()
      print("download finished")
      atexit.unregister(save_status)
      try:
        unlink(statusfile)
      except FileNotFoundError:
        pass


if __name__ == '__main__':
  d = Downloader("http://messir.net/static/test")
  d.start()
  d.join()
