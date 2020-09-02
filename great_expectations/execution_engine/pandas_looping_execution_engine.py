# TODO: catch BatchKwargsError
# TODO: this is pulled straight from Rex's notebook. Are these errors still a thing?


class BatchLoopingExpectationBuilder:
    """
    Accepts expectation configurations, leverages MetricBuilders
    while looping over batches, and returns an expectation
    suite containing the specified expectations.
    """

    @classmethod
    def build_expectations(
        cls,
        context,
        suite_name,
        datasource_name,
        generator_name,
        data_asset_name,
        row_count_config=None,
        column_mean_configs=None,
        column_value_set_configs=None,
    ):

        # initialize suite

        suite = ExpectationSuite(suite_name)

        # get partition ids

        datasource = context.get_datasource(datasource_name=datasource_name)
        generator = datasource.get_batch_kwargs_generator(name=generator_name)
        partition_ids = generator.get_available_partition_ids(
            data_asset_name=data_asset_name
        )
        partition_ids.sort()

        # loop batches

        for i, partition_id in enumerate(partition_ids):
            batch_kwargs = context.build_batch_kwargs(
                datasource=datasource_name,
                batch_kwargs_generator=generator_name,
                data_asset_name=data_asset_name,
                partition_id=partition_id,
            )
            batch = context.get_batch(batch_kwargs, suite)
            columns = batch.get_table_columns()

            if row_count_config:
                if i == 0:
                    row_count_aggregate = MyRowCountMetricBuilder().initialize(batch)
                else:
                    row_count_aggregate = MyRowCountMetricBuilder().update(
                        row_count_aggregate, batch
                    )

            if column_mean_configs:
                if i == 0:
                    column_mean_first_batch = {
                        config["column"]: True for config in column_mean_configs
                    }
                    column_mean_aggregates = {}

                for j, config in enumerate(column_mean_configs):
                    column = config["column"]

                    if column in columns:
                        if column_mean_first_batch[column]:
                            column_mean_aggregates[
                                column
                            ] = MyColumnMeanMetricBuilder().initialize(batch, column)
                        else:
                            column_mean_aggregates[
                                column
                            ] = MyColumnMeanMetricBuilder().update(
                                column_mean_aggregates[column], batch, column
                            )

                        column_mean_first_batch[column] = False

            if column_value_set_configs:
                if i == 0:
                    column_value_set_first_batch = {
                        config["column"]: True for config in column_value_set_configs
                    }
                    column_value_set_aggregates = {}

                for j, config in enumerate(column_value_set_configs):
                    column = config["column"]

                    if column in columns:

                        if column_value_set_first_batch[column]:
                            column_value_set_aggregates[
                                column
                            ] = MyColumnValueSetMetricBuilder().initialize(
                                batch, column
                            )
                        else:
                            column_value_set_aggregates[
                                column
                            ] = MyColumnValueSetMetricBuilder().update(
                                column_value_set_aggregates[column], batch, column
                            )

                        column_value_set_first_batch[column] = False

        # finalize and add expectations

        if row_count_config:
            number_of_stds = row_count_config["number_of_stds"]

            final_aggregate = MyRowCountMetricBuilder().finalize(row_count_aggregate)

            rc_mean = final_aggregate["mean"]
            rc_std = final_aggregate["standard_deviation"]

            rc_min_value = round(
                rc_mean - (number_of_stds * rc_std)
            )  # min value should be an integer
            if (
                "min_value" in row_count_config
            ):  # check to see if a minimum min value has been specified
                if row_count_config["min_value"] < 0:
                    raise ValueError("Minimum value for row count should be at least 0")
                if rc_min_value < row_count_config["min_value"]:
                    rc_min_value = row_count_config["min_value"]
            else:
                rc_min_value *= int(
                    rc_min_value >= 0
                )  # row count should always be >= 0

            rc_max_value = round(
                rc_mean + (number_of_stds * rc_std)
            )  # max value should be an integer
            if (
                "max_value" in row_count_config
            ):  # check to see if a maximum max value has been specified
                if row_count_config["max_value"] < 0:
                    raise ValueError("Maximum value for row count should be at least 0")
                if rc_max_value > row_count_config["max_value"]:
                    rc_max_value = row_count_config["max_value"]

            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_be_between",
                    kwargs={"min_value": rc_min_value, "max_value": rc_max_value},
                    meta={"BatchLoopingProfiler": final_aggregate},
                )
            )

        if column_mean_configs:
            for j, config in enumerate(column_mean_configs):
                column = config["column"]

                number_of_stds = 2
                if "number_of_stds" in config:
                    number_of_stds = config["number_of_stds"]

                final_aggregate = MyColumnMeanMetricBuilder().finalize(
                    column_mean_aggregates[column]
                )

                cm_mean = final_aggregate["mean"]
                cm_std = final_aggregate["standard_deviation"]

                cm_min_value = cm_mean - (number_of_stds * cm_std)
                if (
                    "min_value" in config
                ):  # check to see if a minimum min value has been specified
                    if cm_min_value < config["min_value"]:
                        cm_min_value = config["min_value"]

                cm_max_value = cm_mean + (number_of_stds * cm_std)
                if (
                    "max_value" in config
                ):  # check to see if a maximum max value has been specified
                    if cm_max_value > config["max_value"]:
                        cm_max_value = config["max_value"]

                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_mean_to_be_between",
                        kwargs={
                            "column": column,
                            "min_value": cm_min_value,
                            "max_value": cm_max_value,
                        },
                        meta={"BatchLoopingProfiler": final_aggregate},
                    )
                )

        if column_value_set_configs:
            for j, config in enumerate(column_value_set_configs):
                column = config["column"]
                if (
                    "batch_frequency_threshold" not in config
                    or "row_frequency_threshold" not in config
                ):
                    raise ValueError(
                        "Please specify a batch_frequency_threshold and a row_frequency_threshold"
                    )

                batch_frequency_threshold = config["batch_frequency_threshold"]
                row_frequency_threshold = config["row_frequency_threshold"]

                final_aggregate = MyColumnValueSetMetricBuilder().finalize(
                    column_value_set_aggregates[column]
                )

                batch_count = final_aggregate["batch_count"]
                total_row_count = final_aggregate["total_row_count"]

                value_set = []
                meta = {
                    "batch_count": batch_count,
                    "total_row_count": total_row_count,
                    "values": {},
                }

                for value in final_aggregate["values"]:
                    batch_frequency = final_aggregate["values"][value][
                        "batch_frequency"
                    ]
                    row_frequency = final_aggregate["values"][value]["row_frequency"]

                    if batch_frequency >= batch_frequency_threshold:
                        value_set.append(value)
                        meta["values"][value] = {
                            "batch_frequency": batch_frequency,
                            "row_frequency": row_frequency,
                        }
                    elif row_frequency >= row_frequency_threshold:
                        value_set.append(value)
                        meta["values"][value] = {
                            "batch_frequency": batch_frequency,
                            "row_frequency": row_frequency,
                        }

                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_set",
                        kwargs={"column": column, "value_set": value_set},
                        meta={"BatchLoopingProfiler": meta},
                    )
                )

        return suite
