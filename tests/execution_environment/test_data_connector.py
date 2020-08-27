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
        "class_name": "MetaPandasExecutionEngine",
        "module_name": "great_expectations.execution_engine.pandas_execution_engine",
    }

    covid_directory = "/Users/work/Development/GE_Data/"

    asset_globs = {
        "default": {
            "glob": "*",
            "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\.csv",
            "match_group_id": 1,
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
        name="testing_me",
        class_name="GlobReaderDataConnector",
        base_directory=covid_directory,
    )

    """
    {'execution_engine': {'class_name': 'MetaPandasExecutionEngine', 'module_name': 'great_expectations.execution_engine.pandas_execution_engine'}, 'data_connectors': {'hello_sir': {'class_name': 'GlobReaderDataConnector'}}}
    """
    print(execution_environment._execution_environment_config)

    my_connector = execution_environment.get_data_connector("testing_me")
    # print(my_connector.get_available_data_asset_names()) # {'names': [('default', 'path')]} .. this is ok?

    print(my_connector.get_config())  # {'class_name': 'GlobReaderDataConnector'}
    # print(my_connector.build_batch_kwargs(data_asset_name="default"))

    my_iterator = my_connector.get_iterator(data_asset_name="default")

    for item in my_iterator:
        print(item)
        print("-----")

    # "default": {
    #    "glob": "*",
    #    "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\.csv",
    #    "match_group_id": 1,
    #    "reader_method": "read_csv",
    # }

    # def test_s3_generator_build_batch_kwargs_partition_id(s3_generator):
    #    batch_kwargs = s3_generator.build_batch_kwargs("data", "you")
    #    assert batch_kwargs["s3"] == "s3a://test_bucket/data/for/you.csv"

    # print(execution_environment.list_data_connectors())
    # obs = datasource.list_batch_kwargs_generators()
    # Execution Environment has its own methods to do this


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
