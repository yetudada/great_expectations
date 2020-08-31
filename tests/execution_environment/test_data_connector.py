import logging

import pytest

try:
    from unittest import mock
except ImportError:
    import mock

import great_expectations.execution_environment.data_connector
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.execution_environment.execution_environment import (
    ExecutionEnvironment as exec,
)
from great_expectations.execution_environment.execution_environment import *

logger = logging.getLogger(__name__)


def test_build_execution_environment():
    # we have this DataSource Design documennt : https://github.com/superconductive/design/blob/main/docs/20200813_datasource_configuration.md
    """
    execution_environments:
     fa   pandas:
        default: true
        execution_engine:
            class_name: PandasDataset
        data_connectors:
            simple:
            default: true
            class_name: DataConnector
            # knows about: dataset, path, query, table_name, but requires a data_asset_name (and partition id??) to use them
    """

    execution_engine = {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine.pandas_execution_engine",
    }

    test_directory = "/Users/work/Development/GE_Data/Covid_renamed/"

    asset_params = {
        "test_glob": {
            "glob": "/Users/work/Development/GE_Data/Covid_renamed/*.csv",
            "partition_regex": r"/Users/work/Development/GE_Data/Covid_renamed/file_(.*)_(.*).csv",
            "partition_param": ["year", "file_num"],
            "partition_delimiter": "-",
            "reader_method": "read_csv",
        }
    }
    execution_environment = ExecutionEnvironment(
        name="foo", execution_engine=execution_engine
    )
    assert isinstance(execution_environment, ExecutionEnvironment)

    # do we do this through config?
    # print(exec.build_configuration(class_name = "MetaPandasExecutionEngine"))

    ret = execution_environment.add_data_connector(
        name="covid_files_connector",
        class_name="FilesDataConnector",
        base_directory=test_directory,
        asset_globs=asset_params,
    )

    my_connector = execution_environment.get_data_connector("covid_files_connector")

    result_we_get = my_connector.get_available_partition_ids(
        data_asset_name="test_glob"
    )

    result_we_want = [
        {
            "partition_definition": {"year": "2020", "file_num": "1"},
            "partition_id": "2020-1",
        },
        {
            "partition_definition": {"year": "2020", "file_num": "3"},
            "partition_id": "2020-3",
        },
        {
            "partition_definition": {"year": "2020", "file_num": "2"},
            "partition_id": "2020-2",
        },
    ]

    assert result_we_get == result_we_want


# def test_data_connector():

# you have something called build data connect
# you dont configure it on it's onw
#          - but you do ti
# DataSource --> Execution Environment :
# so we still need to build this
#
# data connector requires an execution environment, and default batchparameters
# once it happens, it will take the batch parameters and generate batch kwargs
# see if we can make it work for glob reader


# new_data_connector = DataConnector(name"test", )
