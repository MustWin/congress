# Copyright (c) 2016 NTT All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from oslo_log import log as logging

from congress.datasources import datasource_driver

LOG = logging.getLogger(__name__)


def d6service(name, keys, inbox, datapath, args):
    """This method is called by d6cage to create a dataservice instance."""
    return MonascaAlarmDriver(name, keys, inbox, datapath, args)


class MonascaAlarmDriver(datasource_driver.PushedDataSourceDriver):
    """A DataSource Driver for Monasca webhook notifications.

    request body:
      {
        "alarm_id": "01234567-89ab-cdef-0123-456789abcdef",
        "alarm_definition_id": "01234567-89ab-cdef-0123-456789abcdef",
        "alarm_name": "CPU Usage",
        "alarm_timestamp": 1458915757,
        "state": "ALARM",
        "old_state", "OK",
        "message", "Alarms when CPU usage is high",
        "tenant_id": "ed209deadbeef3709a2ab8eb8ea7996c",
        "metrics": [
            {
                "name": "cpu.system_perc",
                "dimensions": {
                    "key": "value",
                    .....
                },
            },
            .....
        ]
      }
    """

    value_trans = {'translation-type': 'VALUE'}

    """Flattening the metric dimensions with the metric name reduces the
    schema to 2 tables instead of 3"""
    def flatten_metrics(metrics):
        flatten = {}
        for metric in metrics:
            name_prefix = metric['name'] + '.'
            dimensions = metric.pop('dimensions')
            for k, v in dimensions.items():
                flatten[name_prefix + k] = v
        return flatten

    def widen_alarm(alarm):
        return [alarm]

    metrics_translator = {
        'translation-type': 'VDICT',
        'table-name': 'metric_dimensions',
        'parent-key': 'alarm_id',
        'key-col': 'key',
        'val-col': 'value',
        'objects-extract-fn': flatten_metrics,
        'translator': value_trans
        }

    alarms_translator = {
        'translation-type': 'HDICT',
        'table-name': 'alarms',
        'selector-type': 'DICT_SELECTOR',
        'objects-extract-fn': widen_alarm,
        'field-translators':
            ({'fieldname': 'alarm_id', 'translator': value_trans},
             {'fieldname': 'alarm_definition_id', 'translator': value_trans},
             {'fieldname': 'alarm_name', 'translator': value_trans},
             {'fieldname': 'alarm_description', 'translator': value_trans},
             {'fieldname': 'alarm_timestamp', 'translator': value_trans},
             {'fieldname': 'state', 'translator': value_trans},
             {'fieldname': 'old_state', 'translator': value_trans},
             {'fieldname': 'metrics', 'translator': metrics_translator},
             {'fieldname': 'tenant_id', 'translator': value_trans},
             {'fieldname': 'message', 'translator': value_trans},
             {'fieldname': 'hostname', 'translator': value_trans},)
        }

    TRANSLATORS = [alarms_translator]

    def __init__(self, name='', keys='', inbox=None, datapath=None, args=None):
        super(MonascaAlarmDriver, self).__init__(name, keys, inbox, datapath,
                                                 args)

    @staticmethod
    def get_datasource_info():
        result = {}
        result['id'] = 'monasca_alarm'
        result['description'] = ('Datasource driver that interfaces with '
                                 'Monasca webhook alarm notifications.')
        result['config'] = {}
        return result
