PDL (~~Porn~~ Parallel DownLoader)
=========================

This tool is for downloading video files. It brakes the job
into chunks and downloads the in parallel, but first chunks
are downloaded first. This allows for watching videos while downloading.

It is intented to be dead-simple; in case of an error it will
fail loudly (I hope).

~~~
usage: pdl.py [-h] [-o OUTPUT] [-w WORKERS] [-c CHUNKSIZE] [-d] url

The best downloader. Ever. Version 4.

positional arguments:
  url                   URL to download

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        where to store the downloaded content
  -w WORKERS, --workers WORKERS
                        number of workers (default 5)
  -c CHUNKSIZE, --chunksize CHUNKSIZE
                        chunk size in megs (default 5)
  -d, --debug           enable debug messages
~~~

TODO
----

1. Chunksize cannot be altered after resuming the download.