# -*- coding:utf-8 -*-
__author__ = 'youjia'
__date__ = '2018/8/5 16:08'
import requests
from lxml import etree
import threading
from queue import Queue
import csv


class Producer(threading.Thread):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36'
    }
    # gLock = threading.Lock()

    def __init__(self, page_queue, joke_queue, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.base_domain = 'http://www.budejie.com'
        self.page_queue = page_queue
        self.joke_queue = joke_queue

    def spider(self, url):
        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 200:
            text = resp.text
            html = etree.HTML(text)
            desc = html.xpath('//div[@class="j-r-list-c"]')
            for u in desc:
                joke = u.xpath('.//text()')[4]
                link = self.base_domain+u.xpath('..//a/@href')[0]
                self.joke_queue.put((joke, link))
            print('='*30+'第%s页下载完成' % url.split('/')[-1]+'='*30)

    def run(self):
        while True:
            if self.page_queue.empty():
                break
            url = self.page_queue.get()
            self.spider(url)


class SaveJoke(threading.Thread):
    def __init__(self, joke_queue, writer,  gLock, *args, **kwargs):
        super(SaveJoke, self).__init__(*args, **kwargs)
        self.joke_queue = joke_queue
        self.writer = writer
        self.lock = gLock

    def run(self):
        while True:
            try:
                joke, link = self.joke_queue.get(timeout=5)  # 20秒后没结果又就break掉
                self.lock.acquire()
                self.writer.writerow((joke, link))
                self.lock.release()
                print('保存一条 %s %s' % (joke, link))
            except:
                break


def main():
    page_queue = Queue(100)
    joke_queue = Queue(1000)
    gLock = threading.Lock()
    fp = open('bsbdj.csv', 'a', newline='', encoding='utf-8')
    writer = csv.writer(fp)
    writer.writerow(('content', 'link'))
    for x in range(1, 11):
        url = 'http://www.budejie.com/text/%d' % x
        page_queue.put(url)

    for x in range(5):
        Producer(page_queue, joke_queue).start()

    for x in range(5):
        SaveJoke(joke_queue, writer, gLock).start()


if __name__ == '__main__':
    main()
