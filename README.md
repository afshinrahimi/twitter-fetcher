# twitter-fetcher
A simple Twitter fetcher which compresses and dumps the downloads
using lzo or gzip compression.

Features
------------

1. The Twitter streaming process can be monitored by the main process.
2. The fetcher can be stopped safely (e.g. closing all the files/streams) at run-time by setting safe_stop=True in config file.
3. It supports hourly or daily file rotation for data dumps.
4. The downloaded data is compressed at run-time using lzo which is a very fast (de)compressor or gzip.
5. The program either restarts every hour or in case of no new tweets within sleep_time seconds.
