"""
Copyright <YEAR> Amazon.com, Inc. or its affiliates. All Rights Reserved.
  
  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  A copy of the License is located at
  
      http://www.apache.org/licenses/LICENSE-2.0
  
  or in the "license" file accompanying this file. This file is distributed 
  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
  express or implied. See the License for the specific language governing 
  permissions and limitations under the License.
"""

import unittest
from argparse import Namespace
from migrationtools import DocumentDbIndexTool


class BaseTestCase(unittest.TestCase):
    """
    base test case class that enables debug logs
    """

    def setUp(self):
        args = Namespace()
        args.debug = True
        self.index_tool = DocumentDbIndexTool(args)


class TestCompatibilityIssues(BaseTestCase):
    """
    test case to check incompatible issues
    """

    def test_incompatible_collection(self):
        """
        tests unsupported collection types
        """
        metadata = self.index_tool.get_metadata(
            'test/fixtures/metadata/capped_collection')
        compatibility_issues = self.index_tool.find_compatibility_issues(
            metadata)
        expected_compatibility_issues = {
            'foo_db': {
                'foo_col': {
                    'unsupported_collection_options': ['capped']
                }
            }
        }
        self.assertDictEqual(compatibility_issues, \
                            expected_compatibility_issues, \
                             "Compatibility issues should've matched")

    def test_incompatible_index_type(self):
        """
        tests unsupported index types
        """
        metadata = self.index_tool.get_metadata(
            'test/fixtures/metadata/geo_index')
        compatibility_issues = self.index_tool.find_compatibility_issues(
            metadata)
        expected_compatibility_issues = {
            'foo_db': {
                'foo_col': {
                    'loc2_2d': {
                        'unsupported_index_types': '2d'
                    },
                    'myloc_2dsphere_category_-1_name_1': {
                        'unsupported_index_types': '2dsphere'
                    }
                }
            }
        }
        self.assertDictEqual(compatibility_issues, \
                             expected_compatibility_issues, \
                             "Compatibility issues should've matched")

    def test_incompatible_index_options(self):
        """
        tests unsupported index options
        """
        metadata = self.index_tool.get_metadata(
            'test/fixtures/metadata/storage_engine')
        compatibility_issues = self.index_tool.find_compatibility_issues(
            metadata)
        expected_compatibility_issues = {
            'foo_db': {
                'foo_col': {
                    'engine_field_1': {
                        'unsupported_index_options': ['storageEngine']
                    }
                }
            }
        }
        self.assertDictEqual(compatibility_issues, \
                             expected_compatibility_issues, \
                             "Compatibility issues should've matched")


if __name__ == '__main__':
    unittest.main()
