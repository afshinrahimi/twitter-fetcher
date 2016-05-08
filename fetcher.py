#!/usr/bin/env python
'''
Created on 6 May 2016

@author: af
'''
from authinfo import access_token, access_token_secret, consumer_key, consumer_secret, firehose
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
from os import path
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

class TwitterStreamListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    """
    def __init__(self, dump_path):
        super(TwitterStreamListener, self).__init__()
        self.dump_path = dump_path
        self.current_file = self.get_current_file()
        self.dump_file = open(path.join(self.dump_path, self.current_file), 'ab')
        self.lzop = subprocess.Popen('lzop -c '.split(), stdout=self.dump_file, shell=False, stdin=subprocess.PIPE)
        self.should_stop = False
        self.counter = 0
        self.rate_limit_exceeded = False
            
    def on_data(self, data):
        self.check_everything()
        if self.should_stop:
            return False
        self.counter += 1
        self.lzop.stdin.write(data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            logging.warn('rate limit exceeded.')
            self.rate_limit_exceeded = True
            return False
    
    def stop(self):
        self.should_stop = True
        

    def check_everything(self):
        current_file = self.get_current_file()
        #if one hour has passed
        if self.current_file == current_file:
            pass
        else:
            #1 hour passed, flush the files, close them and restart the process
            self.stop()
            self.dump_file.close()
            try:
                time.sleep(3)
                logging.info('Restarting the script for hourly rotation...')
                os.execv(__file__, sys.argv)
            except:
                pass

            
            
    def get_current_file(self):
        current_file = datetime.now().strftime("twitter.%Y-%m-%d-%H.lzo")
        return current_file
        
    
def start_stream(stream):
    if firehose:
        stream.firehose(count=None, async=True)
    else:
        stream.sample(async=True)
def stop_stream(stream):
    stream.listener.stop()
    stream.disconnect()
               
if __name__ == '__main__':
    dump_dir = '/home/af/Documents/tweets'
    listener = TwitterStreamListener(dump_dir)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    start_stream(stream)
    
    num_tweets = 0
    while True:
        #this should never happen in streaming mode, unless a lot of new connections are opened.
        if listener.rate_limit_exceeded:
            stop_stream(stream)
            time.sleep(1000)
            logging.warn('Restarting the script because rate limit exceeded...')
            os.execv(__file__, sys.argv)
        else:
            #let the fetcher work for 1000s.
            time.sleep(1000)
        #no tweet in 1000 seconds. restart the process.
        if listener.counter == num_tweets:
            try:
                stop_stream(stream)
                logging.debug('Fetcher stopped for process restart.')
            except:
                pass
            logging.warn('Restarting the script because no new tweet is downloaded in the last 1000 seconds.')
            os.execv(__file__, sys.argv)
        else:
            num_tweets = listener.counter
            logging.info('#tweets since fetcher started: ' + str(num_tweets))