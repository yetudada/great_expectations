import datetime
import glob
import logging
import os
import re
import warnings

from great_expectations.exceptions import BatchKwargsError
from great_expectations.execution_environment.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.execution_environment.types import PathBatchKwargs

logger = logging.getLogger(__name__)


class FilesDataConnector(DataConnector):
    r"""FilesDataConnector processes files in a directory according to glob patterns to produce batches of data.

    A more interesting asset_glob might look like the following::

        daily_logs:
          glob: daily_logs/*.csv
          partition_regex: daily_logs/((19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]))_(.*)\.csv


    The "glob" key ensures that every csv file in the daily_logs directory is considered a batch for this data asset.
    The "partition_regex" key ensures that files whose basename begins with a date (with components hyphen, space,
    forward slash, period, or null separated) will be identified by a partition_id equal to just the date portion of
    their name.

    A fully configured FilesDataConnector in yml might look like the following::
        my_datasource:
          class_name: PandasDatasource
          batch_kwargs_generators:
            my_generator:
              class_name: GlobReaderBatchKwargsGenerator
              base_directory: /var/log
              reader_options:
                sep: %
                header: 0
              reader_method: csv
              asset_globs:
                wifi_logs:
                  glob: wifi*.log
                  partition_regex: wifi-((0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])-20\d\d).*\.log
                  reader_method: csv
    """
    recognized_batch_parameters = {
        "data_asset_name",
        "partition_id",
        "reader_method",
        "reader_options",
        "limit",
    }

    def __init__(
        self,
        name="default",
        execution_environment=None,
        base_directory="/data",
        reader_options=None,
        asset_globs=None,
        reader_method=None,
    ):
        logger.debug("Constructing FilesDataConnector {!r}".format(name))
        super().__init__(name, execution_environment=execution_environment)

        if reader_options is None:
            reader_options = {}

        if asset_globs is None:
            asset_globs = {
                "default": {
                    "glob": "*",
                    "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\.csv",
                    "match_group_id": 1,
                    "reader_method": "read_csv",
                }
            }

        self._base_directory = base_directory
        self._reader_options = reader_options
        self._asset_globs = asset_globs
        self._reader_method = reader_method

    @property
    def reader_options(self):
        return self._reader_options

    @property
    def asset_globs(self):
        return self._asset_globs

    @property
    def reader_method(self):
        return self._reader_method

    @property
    def base_directory(self):
        # If base directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if (
            os.path.isabs(self._base_directory)
            or self._execution_environment.data_context is None
        ):
            return self._base_directory
        else:
            return os.path.join(
                self._execution_environment.data_context.root_directory,
                self._base_directory,
            )

    def get_available_data_asset_names(self):
        known_assets = []
        if not os.path.isdir(self.base_directory):
            return {"names": [(asset, "path") for asset in known_assets]}
        for data_asset_name in self.asset_globs.keys():
            batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)
            if len(batch_paths) > 0 and data_asset_name not in known_assets:
                known_assets.append(data_asset_name)

        return {"names": [(asset, "path") for asset in known_assets]}

        # TODO: deprecate generator_asset argument

    def get_available_partition_ids(self, data_asset_name=None):
        # TODO: add the depcrecated option: generator_asset
        files_config = self._get_data_asset_config(data_asset_name=data_asset_name)
        batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)
        partition_ids = [
            self._partitioner(path, files_config)
            for path in batch_paths
            if self._partitioner(path, files_config) is not None
        ]
        return partition_ids

    def _partitioner(self, path, files_config):
        partitions = {}
        if "partition_regex" in files_config:
            matches = re.match(files_config["partition_regex"], path)
            if matches is None:
                logger.warning("No match found for path: %s" % path)
                return (
                    datetime.datetime.now(datetime.timezone.utc).strftime(
                        "%Y%m%dT%H%M%S.%fZ"
                    )
                    + "__unmatched"
                )
            else:
                # need to check that matches length is the same as partition_param
                if "partition_param" in files_config:
                    partition_params = files_config["partition_param"]

                    try:
                        _ = matches[
                            len(partition_params)
                        ]  # check validitiy of partition params and whether the regex was configured correctly

                    except:
                        logger.warning(
                            "The number of matches not match the delimiter. See if your partitions are defined correctly"
                        )
                        print("please check regex")  # TODO: Beef up this error message

                    # build dictionary:
                    # NOTE : matches begin with the full regex match at index=0 and then each matching group
                    # and then each subsequent match in following indices.
                    # this is why partition_definition_inner_dict is loaded with partition_params[i] as key
                    # and matches[i+1] as value
                    partition_definition_inner_dict = {}
                    for i in range(len(partition_params)):
                        partition_definition_inner_dict[partition_params[i]] = matches[
                            i + 1
                        ]
                    partitions["partition_definition"] = partition_definition_inner_dict

                if "partition_delimiter" in files_config:
                    delim = files_config["partition_delimiter"]
                else:
                    delim = "-"

                # process partition_definition into partition id
                partition_id = []
                for key in partitions["partition_definition"].keys():
                    partition_id.append(str(partitions["partition_definition"][key]))
                partition_id = delim.join(partition_id)
                partitions["partition_id"] = partition_id

        return partitions

    def _get_data_asset_paths(self, data_asset_name):
        """
        Returns a list of filepaths associated with the given data_asset_name
        Args:
            data_asset_name:

        Returns:
            paths (list)
        """
        # TODO : change the naming here to be consistent with the rest <WILL>
        glob_config = self._get_data_asset_config(data_asset_name)
        return glob.glob(os.path.join(self.base_directory, glob_config["glob"]))

    def _get_iterator(
        self, data_asset_name, reader_method=None, reader_options=None, limit=None
    ):
        glob_config = self._get_data_asset_config(data_asset_name)
        paths = glob.glob(os.path.join(self.base_directory, glob_config["glob"]))
        return self._build_batch_kwargs_path_iter(
            paths,
            glob_config,
            reader_method=reader_method,
            reader_options=reader_options,
            limit=limit,
        )

    def _build_batch_kwargs_path_iter(
        self,
        path_list,
        glob_config,
        reader_method=None,
        reader_options=None,
        limit=None,
    ):
        for path in path_list:
            yield self._build_batch_kwargs_from_path(
                path,
                glob_config,
                reader_method=reader_method,
                reader_options=reader_options,
                limit=limit,
            )

    def _build_batch_kwargs_from_path(
        self, path, glob_config, reader_method=None, reader_options=None, limit=None
    ):

        batch_kwargs = self._execution_environment.execution_engine.process_batch_parameters(
            reader_method=reader_method
            or glob_config.get("reader_method")
            or self.reader_method,
            reader_options=reader_options
            or glob_config.get("reader_options")
            or self.reader_options,
            limit=limit or glob_config.get("limit"),
        )

        batch_kwargs["path"] = path
        batch_kwargs[
            "execution_environment"
        ] = self._execution_environment.name  # TODO : check if this breaks anything
        return PathBatchKwargs(batch_kwargs)

    def _get_data_asset_config(self, data_asset_name):
        try:
            return self.asset_globs[data_asset_name]
        except KeyError:
            batch_kwargs = {
                "data_asset_name": data_asset_name,
            }
            raise BatchKwargsError(
                "Unknown asset_name %s" % data_asset_name, batch_kwargs
            )
