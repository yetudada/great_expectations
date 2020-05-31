import datetime
import glob
import logging
import os
import re
import warnings

from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import (
    BatchKwargsGenerator,
)
from great_expectations.datasource.types import PathBatchKwargs
from great_expectations.exceptions import BatchKwargsError

logger = logging.getLogger(__name__)


import re
from collections import defaultdict

class OsWalkBatchKwargsGenerator(BatchKwargsGenerator):
    def __init__(self,
        base_directory,
        data_asset_name_regexes:dict={},
        ignored_regexes:list=[],
    ):
        self.base_directory = base_directory
        self.data_asset_name_regexes = data_asset_name_regexes
        self.ignored_regexes = ignored_regexes
        
    def _check_if_data_asset_name_matches_ignored_regexes(self, string):
        for regex in self.ignored_regexes:
            if re.search(regex, string):
                return True
            
        return False
    
    def _get_first_matching_data_asset_name_regex(self, string):
        for data_asset_name, regex in self.data_asset_name_regexes.items():
            if re.search(regex, string):
                return data_asset_name
            
        return None
    
    def _get_data_asset_name_mapping(self):
        data_asset_name_mapping = defaultdict(list)
        
        for root, dirs, files in os.walk(self.base_directory):
            for f_ in files:
                file_name = os.path.relpath(os.path.join(root, f_), self.base_directory)

                if self._check_if_data_asset_name_matches_ignored_regexes(file_name):
                    continue
                
                matching_data_asset_name = self._get_first_matching_data_asset_name_regex(file_name)
                if matching_data_asset_name:
                    data_asset_name_mapping[matching_data_asset_name].append(file_name)
                
                else:
                    data_asset_name_mapping[file_name].append(file_name)
        
        return data_asset_name_mapping
    
    def get_available_data_asset_names(self):
        data_asset_name_list = list(self._get_data_asset_name_mapping().keys())
        data_asset_name_list.sort()
        
        return data_asset_name_list

    def get_available_partition_ids(self, generator_asset=None, data_asset_name=None):
        if generator_asset != None:
            raise ValueError("What the devil?")
            
        data_asset_name_mapping = self._get_data_asset_name_mapping()
        matching_file_names = data_asset_name_mapping[data_asset_name]
        
        if data_asset_name in self.data_asset_name_regexes.keys():
            partition_ids = []
            for file_name in matching_file_names:
                matches = re.findall(
                    self.data_asset_name_regexes[data_asset_name],
                    file_name
                )
                partition_ids.append(matches[0])

            partition_ids.sort()
            return partition_ids
        
        else:
            return [data_asset_name]