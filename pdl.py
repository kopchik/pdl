#!/usr/bin/env python3

from urllib.request import urlopen, Request
from urllib.parse import urlparse
from threading import Thread
from os.path import basename
import re

CHUNKSIZE = 500*1024*1024
queue = []
class Worker(Thread):
  def __init__(self, url, fd):
    super().__init__()
    self.url = url
    self.fd  = fd
  def run(self):
    while True:
      try: start, stop = queue.pop(0)
      except IndexError: break

      req = Request(self.url)
      req.headers['Range'] = 'bytes=%s-%s' % (start, stop)
      resp = urlopen(req)
      print(resp.headers.get('Content-Range')) #TODO: make sanity check
      data = resp.read()
      assert len(data) == stop - start
      self.fd.seek(start)
      self.fd.write(data)


class Downloader(Thread):
  def __init__(self, url):
    super().__init__()
    self.url = url

  def run(self):
    url = self.url
    #host, uri = re.findall("https*://(.+)/.*", self.url)
    #print(host, uri)
    r = urlparse(url)
    outfile = basename(r.path)
    print("url is", url)
    print("output to", outfile)
    print("getting size... ", end='')
    response = urlopen( Request(url, method='HEAD') )
    assert 'Content-Length' 
    rawlen = response.getheader('Content-Length')
    assert rawlen, "No Content-Length header"
    assert rawlen.isdigit(), "Content-Length is not a number: %s" % rawlen
    flen = int(rawlen)
    print(flen)
    start, stop = 0, min(flen, CHUNKSIZE)
    while True:
      print("appending", start, stop)
      queue.append((start,stop))
      if stop == flen: break
      start += CHUNKSIZE
      stop  = min(flen, stop+CHUNKSIZE)
    with open(outfile, "wb") as fd:  #TODO: will overwrite existing file
      fd.truncate(flen)
      workers = []
      for i in range(5):
        worker = Worker(url, fd)
        worker.start()
        workers.append(worker)

      for worker in workers:
        worker.join()
      print("downloading finished")


if __name__ == '__main__':
  d = Downloader("http://messir.net/static/test")
  d.start()
  d.join()
