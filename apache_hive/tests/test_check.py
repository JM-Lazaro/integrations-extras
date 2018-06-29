#) Datadog, Inc. 2010-2017
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)
import pytest

from datadog_checks.apache-hive-ambari import hive
from datadog_checks.errors import CheckException

def test_check(aggregator):
    c = hive('apache-hive-ambari', {}, {}, None)
    c.check({})
    aggregator.assert_all_metrics_covered()

    instance={}
    with pytest.raises(CheckException):
      c.check(instance)

    with pytest.raises(CheckException):
      c.check({'ambari_api_url': 'https://localhost:6188'})

    with pytest.raises(CheckException):
      c.check({'APPID': 'hiveserver2'})

    c.check({'ambari_api_url': 'http://localhost:6188','APPID': 'hiveserver2'})
