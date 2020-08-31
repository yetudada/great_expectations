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
    # what we are mimicing is this line from
    # we have this DataSource Design documennt : https://github.com/superconductive/design/blob/main/docs/20200813_datasource_configuration.md
    """
    execution_environments:
        pandas:
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

    covid_directory = "/Users/work/Development/GE_Data/Covid_renamed/"
    # covid_directory = "/Users/work/Development/GE_Data/covidall"
    # asset_globs = {
    #    "default": {
    #        "glob": "*",
    #        "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\.csv",
    #        "match_group_id": 1,
    #        "reader_method": "read_csv",
    #    }
    # }

    """
    test_data_connector.py::test_build_execution_environment PASSED          [100%][{'partition_definition': {'year': '2020', 'file_num': '1'}, 'partition_id': '2020-1'}, {'partition_definition': {'year': '2020', 'file_num': '3'}, 'partition_id': '2020-3'}, {'partition_definition': {'year': '2020', 'file_num': '2'}, 'partition_id': '2020-2'}]


    """
    asset_globs = {
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
        base_directory=covid_directory,
        asset_globs=asset_globs,
    )

    """
    {'execution_engine': {'class_name': 'MetaPandasExecutionEngine', 'module_name': 'great_expectations.execution_engine.pandas_execution_engine'}, 'data_connectors': {'hello_sir': {'class_name': 'GlobReaderDataConnector'}}}
    """
    # print(execution_environment._execution_environment_config)

    my_connector = execution_environment.get_data_connector("covid_files_connector")

    # print(
    #    my_connector.get_available_data_asset_names()
    # )

    print(my_connector.get_available_partition_ids(data_asset_name="test_glob"))
    # {'names': [('default', 'path')]} .. this is ok?

    # kwargs = [
    #    kwargs for kwargs in my_connector.get_iterator(data_asset_name="covid_glob")
    # ]


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
