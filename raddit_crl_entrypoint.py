# !/usr/bin/python
# -*- coding: utf-8 -*-

from crawler.pushShift import PushShift
from crawler.sqlConnector import DisconnectSafeConnection
import praw
import logging
import os
import json
from praw.models import MoreComments

__author__ = 'qiaoyang'
__time__ = '2021/5/23 19:02'
__project__ = 'hackthon2021'


def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter(fmt='%(asctime)s %(filename)s[line:%(lineno)d] %(message)s',
                                  datefmt="%y-%m-%d %H:%M:%S")
    fileHandler = logging.FileHandler(log_file, mode='a')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)

SQL = "INSERT INTO bitcoinreddit (postid, body, score, author, created, id, parentid, submission, permalink) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"


def start(st="1322653264", et="1417347664", sub='BitcoinMarkets', q='/daily_discussion', srt="top", cnt=25):
    setup_logger('my_log', os.path.join('./crawler/log', 'crl.log'))
    my_log = logging.getLogger('my_log')

    # ps = PushShift()
    # ps.setAfter(st)
    # if et:
    #     ps.setBefore(et)
    # ps.setSub(sub)
    # ps.setQuery(q)
    # post_ids = ps.retrievePushshiftData()
    # ps.createOutputJson(post_ids)
    post_ids = []
    for f in os.listdir('.'):
        if f.startswith('submission'):
            post_ids += json.loads(open(f, 'r').read())['id']
    my_log.info(f'{len(post_ids)} post_ids in all')

    db = DisconnectSafeConnection(
        host='dcphackathon.mysql.database.azure.com',
        user='yangqiao@dcphackathon',
        password='Hackathon2021',
        database='dcphackathonRaddit',
        ssl_ca='./crawler/DigiCertGlobalRootG2.crt.pem'
    )
    cursor = db.cursor()

    reddit = praw.Reddit(
        user_agent="Comment Extraction (by u/USERNAME)",
        client_id="GokPV-Y6oQzvSg",
        client_secret="aGT3D6dlQFIyQ-HxJ0yXs5InpE3FgQ",
        # username="USERNAME",
        # password="PASSWORD",
    )

    for post_id in post_ids:
        print(f'post_id: {post_id}')
        submission = reddit.submission(id=post_id)
        submission.comment_sort = srt
        submission.comments.replace_more(limit=0)  # limit=None means all comments
        for comment in submission.comments.list()[:cnt]:
            my_log.info(f'write postid {post_id} commentid {comment.id}')
            cursor.execute(SQL, (
                post_id, comment.body, comment.score, str(comment.author), int(comment.created), comment.id,
                comment.parent_id, str(comment.submission), comment.permalink))
        db.commit()


start()
