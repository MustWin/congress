# Copyright (c) 2015 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from oslo_config import cfg
cfg.CONF.distributed_architecture = True

from congress.api import policy_model
from congress.api import rule_model
from congress.api import table_model
from congress.tests import base
from congress.tests2.api import base as api_base


class TestTableModel(base.SqlTestCase):
    def setUp(self):
        super(TestTableModel, self).setUp()
        # Here we load the fake driver
        cfg.CONF.set_override(
            'drivers',
            ['congress.tests.fake_datasource.FakeDataSource'])

        self.table_model = table_model.TableModel('api-table',
                                                  policy_engine='engine')
        self.api_rule = rule_model.RuleModel('api-rule',
                                             policy_engine='engine')
        self.policy_model = policy_model.PolicyModel('api-policy',
                                                     policy_engine='engine')
        result = api_base.setup_config([self.table_model, self.api_rule,
                                        self.policy_model])
        self.node = result['node']
        self.engine = result['engine']
        self.data = result['data']
        # create test policy
        self._create_test_policy()

    def tearDown(self):
        super(TestTableModel, self).tearDown()
        self.node.stop()
        self.node.wait()

    def _create_test_policy(self):
        # create policy
        self.policy_model.add_item({"name": 'test-policy'}, {})

    def test_get_datasource_table_with_id(self):
        context = {'ds_id': self.data.service_id,
                   'table_id': 'fake_table'}
        expected_ret = {'id': 'fake_table'}
        ret = self.table_model.get_item('fake_table', {}, context)
        self.assertEqual(expected_ret, ret)

    def test_get_datasource_table_with_name(self):
        context = {'ds_id': self.data.service_id,
                   'table_id': 'fake_table'}
        expected_ret = {'id': 'fake_table'}
        ret = self.table_model.get_item('fake_table', {}, context)
        self.assertEqual(expected_ret, ret)

# TODO(dse2): Enable these tests once returning proper exceptions
# This test should ideally raise congress exception instead of None,
# to be consistent with other api models, reenable it once exceptions
# patch is merged.
#    def test_get_invalid_datasource(self):
#        context = {'ds_id': 'invalid-id',
#                   'table_id': 'fake_table'}
#        expected_ret = None
#        ret = self.table_model.get_item('fake_table', {}, context)
#        self.assertEqual(expected_ret, ret)

    def test_get_invalid_datasource_table(self):
        context = {'ds_id': self.data.service_id,
                   'table_id': 'invalid-table'}
        expected_ret = None
        ret = self.table_model.get_item('invalid-table', {}, context)
        self.assertEqual(expected_ret, ret)

    def test_get_policy_table(self):
        context = {'policy_id': 'test-policy',
                   'table_id': 'p'}
        expected_ret = {'id': 'p'}

        self.api_rule.add_item({'rule': 'p(x) :- q(x)'}, {}, context=context)
        self.api_rule.add_item({'rule': 'q(x) :- r(x)'}, {}, context=context)

        ret = self.table_model.get_item('p', {}, context)
        self.assertEqual(expected_ret, ret)

    def test_get_invalid_policy(self):
        context = {'policy_id': 'test-policy',
                   'table_id': 'fake-table'}
        invalid_context = {'policy_id': 'invalid-policy',
                           'table_id': 'fake-table'}
        expected_ret = None

        self.api_rule.add_item({'rule': 'p(x) :- q(x)'}, {}, context=context)
        self.api_rule.add_item({'rule': 'q(x) :- r(x)'}, {}, context=context)

        ret = self.table_model.get_item('test-policy',
                                        {}, invalid_context)
        self.assertEqual(expected_ret, ret)

    def test_get_invalid_policy_table(self):
        context = {'policy_id': 'test-policy',
                   'table_id': 'fake-table'}
        invalid_context = {'policy_id': 'test-policy',
                           'table_id': 'invalid-name'}
        expected_ret = None

        self.api_rule.add_item({'rule': 'p(x) :- q(x)'}, {}, context=context)
        self.api_rule.add_item({'rule': 'q(x) :- r(x)'}, {}, context=context)

        ret = self.table_model.get_item('test-policy', {},
                                        invalid_context)
        self.assertEqual(expected_ret, ret)

    def test_get_items_datasource_table(self):
        context = {'ds_id': self.data.service_id}
        expected_ret = {'results': [{'id': 'fake_table'}]}

        ret = self.table_model.get_items({}, context)
        self.assertEqual(expected_ret, ret)

# TODO(dse2): Enable these tests once returning proper exceptions
# This test should ideally raise congress exception instead of None,
# to be consistent with other api models, reenable it once exceptions
# patch is merged.
#    def test_get_items_invalid_datasource(self):
#        context = {'ds_id': 'invalid-id',
#                   'table_id': 'fake-table'}
#
#        ret = self.table_model.get_items({}, context)
#        self.assertIsNone(ret)

    def _get_id_list_from_return(self, result):
        return [r['id'] for r in result['results']]

    def test_get_items_policy_table(self):
        context = {'policy_id': 'test-policy'}
        expected_ret = {'results': [{'id': x} for x in ['q', 'p', 'r']]}

        self.api_rule.add_item({'rule': 'p(x) :- q(x)'}, {}, context=context)
        self.api_rule.add_item({'rule': 'q(x) :- r(x)'}, {}, context=context)

        ret = self.table_model.get_items({}, context)
        self.assertEqual(set(self._get_id_list_from_return(expected_ret)),
                         set(self._get_id_list_from_return(ret)))

    def test_get_items_invalid_policy(self):
        context = {'policy_id': 'test-policy'}
        invalid_context = {'policy_id': 'invalid-policy'}
        expected_ret = None

        self.api_rule.add_item({'rule': 'p(x) :- q(x)'}, {}, context=context)
        self.api_rule.add_item({'rule': 'q(x) :- r(x)'}, {}, context=context)

        ret = self.table_model.get_items({}, invalid_context)
        self.assertEqual(expected_ret, ret)
