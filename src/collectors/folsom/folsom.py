# coding=utf-8

"""
Collect folsom stats

#### Dependencies

 * urlib2

"""

import urllib2
from types import *

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import diamond.collector


class FolsomCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(FolsomCollector,
                            self).get_default_config_help()
        config_help.update({
            'host': "",
            'port': ""
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(FolsomCollector, self).get_default_config()
        config.update({
            'host':     '127.0.0.1',
            'port':     5565
        })
        return config

    def _get_dump(self):
        url = 'http://%s:%i/_dump' % (
            self.config['host'], int(self.config['port']))
        return self._http_get(url)

    def _get_memory(self):
        url = 'http://%s:%i/_nodes/_memory' % (
            self.config['host'], int(self.config['port']))
        return self._http_get(url)

    def _get_statistics(self):
        url = 'http://%s:%i/_nodes/_statistics' % (
            self.config['host'], int(self.config['port']))
        return self._http_get(url)

    def _http_get(self, url):
        try:
            response = urllib2.urlopen(url)
            jresponse = json.load(response)
            #self.log.debug('http GET %s -> %s' % (url, jresponse))
            return jresponse
        except urllib2.HTTPError, err:
            self.log.error("%s: %s", url, err)
            return False
        except (TypeError, ValueError):
            self.log.error("Unable to parse response from folsom as a"
                           + " json object")
            return False

    def _copy_one_level(self, metrics, prefix, data, filter=lambda key: True):
        for key, value in data.iteritems():
            if filter(key):
                metrics['%s.%s' % (prefix, key)] = value

    def _copy_two_level(self, metrics, prefix, data, filter=lambda key: True):
        for key1, d1 in data.iteritems():
            if filter(key1):
                self._copy_one_level(metrics, '%s.%s' % (prefix, key1), d1)

    def collect(self):
        if json is None:
            self.log.error('Unable to import json')
            return {}

        # get _dump
        result = self._get_dump()
        if result:
            metrics = {}
            for metric in result:
                for v in metric:
                    if (metric[v]['type'] == "counter"):
                        self._copy_one_level(metrics, v, metric[v], lambda key: not key.endswith('type'))
                    elif (metric[v]['type'] == "spiral"):
                        self._copy_one_level(metrics, v, metric[v], lambda key: not key.endswith('type'))
                    elif (metric[v]['type'] == "meter"):
                        self._copy_one_level(metrics, v, metric[v], lambda key: not (key.endswith('type') or key.endswith('acceleration')))
                        self._copy_two_level(metrics, v, metric[v], lambda key: key.endswith('acceleration'))

            for key in metrics:
                #print '%s = %s' % (key, metrics[key])
                self.publish(key, metrics[key])

        # get _nodes/_memory
        result = self._get_memory()
        if result:
            for node in result:
                for metric in result[node]:
                    key = "%s.%s" % (node.replace('.', '_'), metric)
                    #print '%s = %s' % (key, result[node][metric])
                    self.publish(key, result[node][metric])

        # get _nodes/_statistics
        result = self._get_statistics()
        if result:
            metrics = {}
            for node in result:
                for m in result[node]:
                    if (type(result[node][m]) == DictType):
                        key = "%s.%s" % (node.replace('.', '_'), m)
                        self._copy_one_level(metrics, key, result[node][m])
                    else:
                        metrics['%s.%s' % (node.replace('.', '_'), m)] = result[node][m]

            for key in metrics:
                #print '%s = %s' % (key, metrics[key])
                self.publish(key, metrics[key])
