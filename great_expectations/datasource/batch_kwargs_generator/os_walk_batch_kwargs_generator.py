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
    """OsWalkBatchKwargsGenerator maps the files in a (potentially nested) directory to produce semantically meaningful batches.

    Example:

        Given this directory structure:

            some_dir
            ├── 100
            │   ├── A.csv
            │   ├── B.txt
            │   └── extra.png
            ├── 101
            │   ├── A.csv
            │   ├── B.txt
            │   └── extra.png
            ├── 102
            │   ├── A.csv
            │   ├── bonus.pdf
            │   └── extra.png
            └── manifest.txt

        And this config:

            base_directory: some_dir
            default_partition_keys:
                - partition_key

            partition_key_groups:
                my_group_name # e.g. "weekly_events", "batches_from_vendor_X"
                    partition_keys:
                        -   key_name : some_other_partition_key
                            key_parser_method : _default_key_parser


            data_assets:
                A
                    regex: (\d+)/A.csv
                    reader_method: read_csv
                    reader_options:
                        skiprows: 2
                    partition_keys:
                        -   key_name : some_other_partition_key
                            key_parser_method : _default_key_parser
                B
                    regex: (\d+)/B.csv
                    reader_method: read_table
                    partition_key_group: my_group_name

            ignored_regexes:
                - manifest.txt
                - *.pdf
                - *.png

        OsWalkBatchKwargsGenerator indexes the following data_asset_names and batch_partition_ids:

            A : [100, 101, 102]
            B : [100, 101]

    DONE:
    * Can a data_asset have more than one regex? (Yes. Think of the case where you want to slice tables by day, week, and month)

    TODO:
    * Figure out how to handle sorting on partition ids. (e.g Y/D/M timestamps, ["1","11","12","2","3","4"...])
        ---> Make partition_keys a list of dicts and add a key_parser_method
            Lists of dicts are slightly unfriendly yaml. Are we okay with that?
    * Figure out how to handle the case where multiple data_assets share and don't share the same partition_ids.
        ---> Make partition_keys a top-level variable (to handle the "all-the-same" case),
            with individual overrides (to handle the "some are different" case),
            and a partition_key_groups tag, (to handle the "some are different, but in groups" case),
        ---> individual overrrides override partition_key_groups, which override top-level partition keys

    * Figure out where to put reader_method and options. Is there a default option?
        ---> This should follow the same pattern as partition_keys.
            In fact, it's tempting to put them in the same object, but there's nothing that guarantees that the way a file is named corresponds with how it should be parsed. Keep 'em seperate.
    * Figure out how to handle the case of grouping multiple files into a single batch.
        grouping: #expect_exactly_one_match, expect_at_most_one_match, multiple_files_can_match, multiple_files_must_match
        grouping_method
        ---> when we specify the function to group and load multiple files, this becomes the general case of reader_method and options.
    * How does all this interact with the Datasource?
    * Figure out how external directives (like downsampling) get passed through.


    * Figure out which operations are fast and slow, and where caching will be most valuable.
        ---> Listing data is potentially slow. Attempting to load data is potentially slow
    * Review external-facing verbs, to make sure they're really the ones we want.
    * how do we handle the case where file formats change within data assets? (e.g. at a specific point in time)
        ---> Conditional grouping methods (i.e. grouping method is a function of partition_keys)
    """

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