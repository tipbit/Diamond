# coding=utf-8

"""
Collect folsom stats

#### Dependencies

 * urlib2

"""

import urllib2

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

    def _get(self):
        url = 'http://%s:%i/_dump' % (
            self.config['host'], int(self.config['port']))
        try:
            response = urllib2.urlopen(url)
        except urllib2.HTTPError, err:
            self.log.error("%s: %s", url, err)
            return False

        try:
            return json.load(response)
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

        result = self._get()
        if not result:
            return

        metrics = {}
        for metric in result:
            for v in metric:
                if (metric[v]['type'] == "counter"):
                    self._copy_one_level(metrics, v, metric[v], lambda key: not key.endswith('type'))
                elif (metric[v]['type'] == "meter"):
                    self._copy_one_level(metrics, v, metric[v], lambda key: not (key.endswith('type') or key.endswith('acceleration')))
                    self._copy_two_level(metrics, v, metric[v], lambda key: key.endswith('acceleration'))

        #print result
        for key in metrics:
            #print '%s = %s' % (key, metrics[key])
            self.publish(key, metrics[key])
