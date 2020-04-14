#!/usr/bin/env python
'''
created on 6 May 2016
@author: af
'''
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import subprocess
from datetime import datetime
import time
import sys
import os
import logging
import pdb
import ConfigParser
import signal
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

class TwitterStreamListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    """
    def __init__(self, dump_path):
        super(TwitterStreamListener, self).__init__()
        self.dump_path = dump_path
        self.current_file = datetime.now().strftime("twitter.%Y-%m-%d-%H.json." + compression)
        self.dump_file = open(path.join(self.dump_path, self.current_file), 'ab')
        if compression == 'lzo':
            self.compressor = subprocess.Popen('lzop -c '.split(), stdout=self.dump_file, shell=False, stdin=subprocess.PIPE)
        elif compression == 'gz':
            self.compressor = subprocess.Popen('gzip -c '.split(), stdout=self.dump_file, shell=False, stdin=subprocess.PIPE)
        self.should_stop = False
        self.counter = 0
        self.rate_limit_exceeded = False

    def on_data(self, data):
        if self.should_stop:
            return False
        self.counter += 1
        self.compressor.stdin.write(data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            logging.warn('rate limit exceeded.')
            self.rate_limit_exceeded = True
            return False
        logging.warn('error occurred in fetcher Twitter API with status_code ' + str(status_code))

    def stop(self):
        self.should_stop = True
        if not stream.listener.dump_file.closed:
            stream.listener.dump_file.close()
        try:
            self.compressor.kill()
        except:
            pass


def start_stream(stream, firehose):
    if firehose:
        stream.firehose(count=None, is_async=True)
    else:
        stream.sample(is_async=True)
def stop_stream(stream):
    stream.listener.stop()
    stream.disconnect()
    if not stream.listener.dump_file.closed:
        stream.listener.dump_file.close()

def getConf(conf_file='conf.cfg'):
    config = ConfigParser.ConfigParser()
    config.read(conf_file)
    return config
def signal_handler(signum, frame):
    logging.info('program interrupted by signal ' + str(signum) + ' quitting safely...')
    stop_stream(stream)
    sys.exit(1)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    config = getConf()
    dump_dir = config.get('appinfo', 'dump_dir')
    firehose = config.getboolean('appinfo', 'firehose')
    sleep_time = config.getint('appinfo', 'sleep_time')
    safe_stop = config.getboolean('appinfo', 'safe_stop')
    compression = config.getint('appinfo', 'compression')
    #safe stop is turned on
    if safe_stop:
        logging.info('safe_stop is True. Quitting safely. To start the fetcher turn off safe_stop.')
        sys.exit()

    listener = TwitterStreamListener(dump_dir)
    consumer_key, consumer_secret = config.get('authinfo', 'consumer_key'), config.get('authinfo', 'consumer_secret')
    access_token, access_token_secret = config.get('authinfo', 'access_token'), config.get('authinfo', 'access_token_secret')
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    start_stream(stream, firehose)
    logging.info('The fetcher started working,')

    num_tweets = 0
    while True:
        #this should never happen in streaming mode, unless a lot of new connections are opened.
        if listener.rate_limit_exceeded:
            stop_stream(stream)
            time.sleep(10)
            logging.warn('Restarting the script because rate limit exceeded...')
            os.execv(__file__, sys.argv)
        else:
            #let the main process sleep and the fetcher work for a while. Meanwhile check if safe_stop is on so to end the program.
            slept_time = 0
            while slept_time < sleep_time:
                config = getConf()
                sleep_time = config.getint('appinfo', 'sleep_time')
                safe_stop = config.getboolean('appinfo', 'safe_stop')
                #safe_stop is turned on. stop the stream, quit the program.
                if safe_stop:
                    logging.info('safe_stop is True. Quitting safely. To start the fetcher turn off safe_stop.')
                    stop_stream(stream)
                    sys.exit()
                else:
                    #do the hourly rotation check.
                    if listener.current_file != datetime.now().strftime("twitter.%Y-%m-%d-%H.json." + compression):
                        stop_stream(stream)
                        logging.info('Restarting the script because of hourly file rotation.')
                        os.execv(__file__, sys.argv)


                slept_time += 10
                time.sleep(10)
        #no tweet in sleep_time seconds. restart the process.
        if listener.counter == num_tweets:
            try:
                stop_stream(stream)
                logging.debug('Fetcher stopped for process restart.')
            except:
                pass
            logging.warn('Restarting the script because no new tweet is downloaded in the last ' + str(sleep_time) + ' seconds.')
            os.execv(__file__, sys.argv)
        else:
            num_tweets = listener.counter
            logging.info('#tweets since fetcher started: ' + str(num_tweets))
                                                                                     
