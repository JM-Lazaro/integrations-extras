# (C) Datadog, Inc. 2010-2017
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

import pytest
import requests
import json

from datadog_checks.checks import AgentCheck
from hashlib import md5

class apache_hive(AgentCheck):
    hive_metrics={}
    def check(self, instance):
        APPID=instance.get('APPID')
        ambari_api_url=instance.get('ambari_api_url')
        if not APPID or not ambari_api_url:
           raise CheckException("Configuration error, please fix apache-hive-ambari.yaml")
        self.getListOfHiveMetrics(instance)
        for key,value in hive_metrics.iteritems():
          data=self.getMetric(ambari_api_url,key,APPID)
          metricName='hive.'+key
          if value=='GAUGE':
              self.gauge(self,metricName,data)
          else value=='COUNTER':
              self.count(self,metricName,data)
        pass
    def getListOfHiveMetrics(self,url):
        data=self.httpRequest('http://'+url+'/ws/v1/timeline/metrics/metadata')
        for metrics in data['hiveserver2']:
           hive_metrics[metrics['metricname']] = metrics['type']
    def getMetric(self, url,metricname, APPID):
        data=self.httpRequest('http://'+url+'/ws/v1/timeline/metrics?metricNames='+metricname+'&appId='+APPID)
        metrics = data['metrics']
        if metrics:
          timestamp = metrics[0]['starttime']
          return metrics[0]['metrics'][str(timestamp)]
        else:
          self.log.info(metricname+' is empty!')
          return
   def httpRequest(self,url):
       aggregation_key = md5(url).hexdigest()
       try:
         r = requests.get(url, timeout=8)
       except requests.exceptions.Timeout as e:
         self.timeout_event(url, timeout, aggregation_key)
         return
       if r.status_code != 200:
         self.status_code_event(url, r, aggregation_key)
         return
       else:
         return r.json()
   def timeout_event(self, url, timeout, aggregation_key):
        self.event({
            'timestamp': int(time.time()),
            'event_type': 'http_check',
            'msg_title': 'URL timeout',
            'msg_text': '%s timed out after %s seconds.' % (url, timeout),
            'aggregation_key': aggregation_key
        })

   def status_code_event(self, url, r, aggregation_key):
        self.event({
            'timestamp': int(time.time()),
            'event_type': 'http_check',
            'msg_title': 'Invalid response code for %s' % url,
            'msg_text': '%s returned a status of %s' % (url, r.status_code),
            'aggregation_key': aggregation_key
        })
