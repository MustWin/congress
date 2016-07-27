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

import mock

from congress.datasources import monasca_alarm_driver
from congress.tests import base


class TestMonascaAlarmDriver(base.TestCase):

    def setUp(self):
        super(TestMonascaAlarmDriver, self).setUp()
        self.monasca = monasca_alarm_driver.MonascaAlarmDriver(
            'test-monasca-alarm')

    def numbered_string(self, string, number):
        return string + str(number)

    def dbl_num_string(self, string, number_a, number_b):
        return string + str(number_a) + "," + str(number_b)

    def generate_alarm_object(self, num_metrics, num_dimensions):
        metrics = []
        for i in range(0, num_metrics):
            dimensions = {}
            for j in range(0, num_dimensions):
                name = self.dbl_num_string('test_dimension_name', i, j)
                val = self.dbl_num_string('test_dimension_val', i, j)
                dimensions[name] = val
            metric = {
                "name": self.numbered_string('test_metric_name', i),
                "dimensions": dimensions
            }
            metrics.append(metric)
        object = {
            "alarm_id": 'test_alarm_id',
            "alarm_definition_id": 'test_alarm_definition_id',
            "alarm_name": 'test_alarm_name',
            "alarm_description": 'test_alarm_description',
            "alarm_timestamp": 'test_alarm_timestamp',
            "state": 'test_state',
            "old_state": 'test_old_state',
            "tenant_id": 'test_tenant_id',
            "message": 'test_message',
            "hostname": 'test_hostname',
            "metrics": metrics
            }
        return object

    @mock.patch.object(monasca_alarm_driver.MonascaAlarmDriver, 'publish')
    def test_alarms_table(self, mocked_publish):
        num_metrics = 3
        num_dimensions = 4
        obj = self.generate_alarm_object(num_metrics, num_dimensions)
        self.monasca.update_entire_data('alarms', obj)

        self.assertEqual(1, len(self.monasca.state['alarms']))

        metdims = num_metrics * num_dimensions
        self.assertEqual(metdims, len(self.monasca.state['metric_dimensions']))

        # change elements in state['alarms'] set to list and sort by id
        alarm = list(self.monasca.state['alarms'])[0]

        self.assertEqual('test_alarm_id', alarm[0])
        self.assertEqual('test_alarm_definition_id', alarm[1])
        self.assertEqual('test_alarm_name', alarm[2])
        self.assertEqual('test_alarm_description', alarm[3])
        self.assertEqual('test_alarm_timestamp', alarm[4])
        self.assertEqual('test_state', alarm[5])
        self.assertEqual('test_old_state', alarm[6])
        self.assertEqual('test_tenant_id', alarm[7])
        self.assertEqual('test_message', alarm[8])
        self.assertEqual('test_hostname', alarm[9])

        metric_dimensions = sorted(
            list(self.monasca.state['metric_dimensions']),
            key=lambda x: x[1])

        metrics = {}
        for i, row in enumerate(metric_dimensions):
            self.assertEqual(alarm[0], row[0])
            splitted = row[1].split('.', 1)
            if splitted[0] not in metrics:
                metric = {
                    "name": splitted[0],
                    "dimensions": {}
                    }
                metrics[splitted[0]] = metric

            metrics[splitted[0]]["dimensions"][splitted[1]] = row[2]

        for i, name_metric in enumerate(sorted(metrics.items(),
                                               key=lambda kv: kv[0])):
            name, metric = name_metric
            self.assertEqual(self.numbered_string('test_metric_name', i),
                             metric["name"])
            for j, dimension_name_val in enumerate(sorted(
                    metric["dimensions"].items(), key=lambda kv: kv[0])):
                dimension_name, dimension_val = dimension_name_val
                expect_name = self.dbl_num_string('test_dimension_name', i, j)
                expect_val = self.dbl_num_string('test_dimension_val', i, j)
                self.assertEqual(expect_name, dimension_name)
                self.assertEqual(expect_val, dimension_val)
