# coding=utf-8

"""
Collect http status code from the specified urls

#### Dependencies

 * urllib2

"""

from urllib2 import urlopen, URLError, HTTPError
import diamond.collector

class HttpStatusCodeCollector(diamond.collector.Collector):

    def get_default_config(self):
        config = super(HttpStatusCodeCollector, self).get_default_config()
        config.update({
            'urls':     'https://www.tipbit.com'
        })
        return config

    def collect(self):
        urls = self.config['urls']

        for url in urls:
            emit_url = url.split('//').pop().replace('.', '_')
            try:
                response = urlopen(url)
                self.publish(emit_url, response.code)

            except HTTPError, e:
                self.publish(emit_url, e.code)

            except URLError, e:
                self.publish(emit_url, 522)
