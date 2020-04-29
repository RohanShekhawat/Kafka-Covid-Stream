# Licensed to the Apache Software Foundation (ASF) under one or more contributor
#   license agreements. See the NOTICE file distributed with this work for additional
#   information regarding copyright ownership. The ASF licenses this file to
#   you under the Apache License, Version 2.0 (the "License"); you may not use
#   this file except in compliance with the License. You may obtain a copy of
#   the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
#   by applicable law or agreed to in writing, software distributed under the
#   License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
#   OF ANY KIND, either express or implied. See the License for the specific
#   language governing permissions and limitations under the License.
# -*- coding: utf-8 -*-
import tweepy
import os
import json
import argparse
from credentials import *

class EchoStreamListener(tweepy.StreamListener):
    def __init__(self, api ):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

    def on_data(self, tweet):
        tweet_data = json.loads(tweet)
        if 'text' in tweet_data:
            print(tweet.rstrip())

    def on_error(self, status_code):
        return True

    def on_timeout(self):
        return True


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
    sapi = tweepy.streaming.Stream(
        auth, EchoStreamListener( api=api ))
    sapi.sample()
