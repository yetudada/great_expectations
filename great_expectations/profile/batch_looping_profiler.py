class BatchLoopingProfiler(BasicDatasetProfilerBase):
    """
    Accepts profiler configurations
    Learns column types and cardinalities

    Uses configurations, column types, and cardinalities together
    to determine which expectations should be developed for each
    column

    Leverages an ExpectationBuilder to build an expectation suite
    """

    @classmethod
    def _get_default_config(cls, expectation, column=None):
        if expectation == "expect_table_row_count_to_be_between":
            config = {
                "number_of_stds": 2,
            }

        elif column:
            if expectation == "expect_column_mean_to_be_between":
                config = {
                    "column": column,
                    "number_of_stds": 2,
                }
            elif expectation == "expect_column_values_to_be_in_set":
                config = {
                    "column": column,
                    "batch_frequency_threshold": 0.5,
                    "row_frequency_threshold": 0.001,
                }
            else:
                raise NotImplementedError(
                    "The specified column level expectation has not been implemented"
                )

        else:
            raise NotImplementedError(
                "The specified table level expectation has not been implemented"
            )

        return config

    @classmethod
    def profile(
        cls,
        context,
        suite_name,
        datasource_name,
        generator_name,
        data_asset_name,
        all_columns_config=None,
        table_level_config=None,
        column_subset_config=None,
        individual_columns_config=None,
    ):
        # build default configs
        default_table_level_config = {
            "expect_table_row_count_to_be_between": cls._get_default_config(
                "expect_table_row_count_to_be_between"
            )
        }

        default_column_subset_config = {
            ProfilerDataType.INT: ["expect_column_mean_to_be_between"],
            ProfilerDataType.FLOAT: ["expect_column_mean_to_be_between"],
            ProfilerDataType.STRING: ["expect_column_values_to_be_in_set"],
            ProfilerDataType.BOOLEAN: ["expect_column_values_to_be_in_set"],
            ProfilerDataType.DATETIME: [],
            ProfilerDataType.UNKNOWN: [],
        }

        # all_columns_config will be a way for the user to specify expectations
        # to be included or excluded for all columns, as well as columns to be
        # excluded by the profiler
        excluded_columns = None
        if all_columns_config:
            for item in all_columns_config:
                if item == "excluded_columns":
                    excluded_columns = all_columns_config["excluded_columns"]
                else:
                    raise NotImplementedError

        # table_level_config will be a way for the user to specify included
        # or excluded table level expectations
        if table_level_config:
            raise NotImplementedError

        # column_subset_config will be a way for users to specify
        # here's what I want for all columns of type INT, STRING, etc
        if column_subset_config:
            if "semantic_types" in column_subset_config:
                semantic_types = column_subset_config["semantic_types"]
                for semantic_type in semantic_types:
                    if "additional_expectations" in semantic_types[semantic_type]:
                        raise NotImplementedError
                    if "excluded_expectations" in semantic_types[semantic_type]:
                        raise NotImplementedError
                    if "included_expectations" in semantic_types[semantic_type]:
                        raise NotImplementedError
            if "regex" in column_subset_config:
                raise NotImplementedError

        # we are going to need to do some sort of join between
        # the user specified configs and the default configs
        table_level_config = default_table_level_config
        column_subset_config = default_column_subset_config

        # initialize suite
        suite = ExpectationSuite(suite_name)

        # get partition ids
        datasource = context.get_datasource(datasource_name=datasource_name)
        generator = datasource.get_batch_kwargs_generator(name=generator_name)
        partition_ids = generator.get_available_partition_ids(
            data_asset_name=data_asset_name
        )
        partition_ids.sort()

        # get final batch
        batch_kwargs = context.build_batch_kwargs(
            datasource=datasource_name,
            batch_kwargs_generator=generator_name,
            data_asset_name=data_asset_name,
            partition_id=partition_ids[-1],
        )
        final_batch = context.get_batch(batch_kwargs, suite)

        # gather column information
        columns = final_batch.get_table_columns()
        if excluded_columns:
            for column in excluded_columns:
                if column in columns:
                    columns.remove(column)
                else:
                    raise ValueError(
                        "Specified column does not exist in the final batch"
                    )
        column_types = {
            column: cls._get_column_type(final_batch, column) for column in columns
        }

        # instantiate configs
        for expectation in table_level_config:
            if expectation == "expect_table_row_count_to_be_between":
                row_count_config = table_level_config[expectation]
            else:
                raise NotImplementedError

        column_mean_configs = []
        column_value_set_configs = []
        for column in columns:

            if individual_columns_config and (column in individual_columns_config):
                column_config = individual_columns_config[column]
                if "semantic_type" in column_config:
                    column_types[column] = individual_columns_config[column][
                        "semantic_type"
                    ]
                if "additional_expectations" in column_config:
                    raise NotImplementedError
                if "included_expectations" in column_config:
                    raise NotImplementedError
                if "excluded_expectations" in column_config:
                    raise NotImplementedError

            column_type = column_types[column]
            expectations = column_subset_config[column_type]
            for expectation in expectations:
                if expectation == "expect_column_mean_to_be_between":
                    column_mean_configs.append(
                        cls._get_default_config(expectation, column)
                    )
                elif expectation == "expect_column_values_to_be_in_set":
                    column_value_set_configs.append(
                        cls._get_default_config(expectation, column)
                    )
                else:
                    raise NotImplementedError

        suite = BatchLoopingExpectationBuilder().build_expectations(
            context,
            expectation_suite_name,
            datasource_name,
            generator_name,
            data_asset_name,
            row_count_config=row_count_config,
            column_mean_configs=column_mean_configs,
            column_value_set_configs=column_value_set_configs,
        )

        return suite
