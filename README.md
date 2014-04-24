PDL (Parallel DownLoader)
=========================

This tool is to download videos in multiple streams
but in more or less sequential way (useful for watching
movies while downloading).

It is intented to be simple; in case of error it will
fail loudly (I hope).


~~~
usage: pdl.py [-h] [-w WORKERS] [-c CHUNKSIZE] [-d] url

The best downloader. Ever.

positional arguments:
  url                   URL to download

optional arguments:
  -h, --help            show this help message and exit
  -w WORKERS, --workers WORKERS
                        number of workers (default 5)
  -c CHUNKSIZE, --chunksize CHUNKSIZE
                        chunk size in megs (default 5)
  -d, --debug           enable debug mode
~~~

TODO:

1. Intercept CTRL+C better
