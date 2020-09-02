class MyRowCountMetricBuilder:
    """
    Leverages Welford's algorithm, a famous algorithm for
    calculating a running variance. For numerical
    stability, we don't keep track of variance as
    we go, rather keeping track of the squares of
    the distance from the mean. Then, at the end,
    we can |calculate the variance
    """

    @classmethod
    def initialize(cls, batch):
        """
        Initializes an aggregate that will track:
            count   the number of data points in the set
            mean    the mean of the data set
            M2      the squared distance from the mean
        as a dictionary
        """
        row_count = batch.get_row_count()

        batch_count = 1
        mean = row_count
        M2 = 0

        return {"batch_count": batch_count, "mean": mean, "M2": M2}

    @classmethod
    def update(cls, current_aggregate, batch):
        """
        Adds one data point to an aggregate, tracking:
            count   the number of data points in the set
            mean    the mean of the data set
            M2      the squared distance from the mean
        """
        row_count = batch.get_row_count()

        batch_count = current_aggregate["batch_count"]
        mean = current_aggregate["mean"]
        M2 = current_aggregate["M2"]

        batch_count += 1
        delta = row_count - mean
        mean += delta / batch_count
        delta2 = row_count - mean
        M2 += delta * delta2

        return {"batch_count": batch_count, "mean": mean, "M2": M2}

    @classmethod
    def finalize(cls, current_aggregate):
        """
        Uses an aggregate as defined in cls.init and cls.update
        to retrieve mean, variance, and sample variance
        of the data set
        """
        import math

        batch_count = current_aggregate["batch_count"]
        mean = current_aggregate["mean"]
        M2 = current_aggregate["M2"]

        if batch_count < 2:
            final_aggregate = {
                "batch_count": batch_count,
                "mean": mean,
                "variance": float("nan"),
                "sample_variance": float("nan"),
                "standard_deviation": float("nan"),
            }
        else:
            mean = mean
            variance = M2 / batch_count
            standard_deviation = math.sqrt(variance)
            final_aggregate = {
                "batch_count": batch_count,
                "mean": mean,
                "standard_deviation": standard_deviation,
            }

        return final_aggregate


class MyColumnMeanMetricBuilder:
    """
    Leverages Welford's algorithm, a famous algorithm for
    calculating a running variance. For numerical
    stability, we don't keep track of variance as
    we go, rather keeping track of the squares of
    the distance from the mean. Then, at the end,
    we can calculate the variance
    """

    @classmethod
    def initialize(cls, batch, column):
        """
        Initializes an aggregate that will track:
            count   the number of data points in the set
            mean    the mean of the data set
            M2      the squared distance from the mean
        as a dictionary
        """
        column_mean = batch.get_column_mean(column)

        batch_count = 1
        mean = column_mean
        M2 = 0

        return {"batch_count": batch_count, "mean": mean, "M2": M2}

    @classmethod
    def update(cls, current_aggregate, batch, column):
        """
        Adds one data point to an aggregate, tracking:
            count   the number of data points in the set
            mean    the mean of the data set
            M2      the squared distance from the mean
        """
        column_mean = batch.get_column_mean(column)

        batch_count = current_aggregate["batch_count"]
        mean = current_aggregate["mean"]
        M2 = current_aggregate["M2"]

        batch_count += 1
        delta = column_mean - mean
        mean += delta / batch_count
        delta2 = column_mean - mean
        M2 += delta * delta2

        return {"batch_count": batch_count, "mean": mean, "M2": M2}

    @classmethod
    def finalize(cls, current_aggregate):
        """
        Uses an aggregate as defined in cls.init and cls.update
        to retrieve mean, variance, and sample variance
        of the data set
        """
        import math

        batch_count = current_aggregate["batch_count"]
        mean = current_aggregate["mean"]
        M2 = current_aggregate["M2"]

        if batch_count < 2:
            final_aggregate = {
                "batch_count": batch_count,
                "mean": mean,
                "variance": float("nan"),
                "sample_variance": float("nan"),
                "standard_deviation": float("nan"),
            }
        else:
            mean = mean
            variance = M2 / batch_count
            standard_deviation = math.sqrt(variance)
            final_aggregate = {
                "batch_count": batch_count,
                "mean": mean,
                "standard_deviation": standard_deviation,
            }

        return final_aggregate


class MyColumnValueSetMetricBuilder:
    """
    The aggregate we are building for each column
    looks as follows:

    {
        'batch_count': number_of_batches,
        'total_row_count': aggregate_number_of_rows,
        'values':{
            'value_1': {
                'total_instances': total_across_all_batches,
                'batches_found_in': total_batches_found_in
            },
            'value_2': {
                'total_instances': total_across_all_batches,
                'batches_found_in': total_batches_found_in
            }
        }
    }
    """

    @classmethod
    def initialize(cls, batch, column):
        value_counts = batch.get_column_value_counts(column)
        row_count = batch.get_row_count()

        aggregate = {}
        aggregate["values"] = {}
        for value in value_counts.index:
            aggregate["values"][value] = {
                "total_instances": value_counts[value],
                "batches_found_in": 1,
            }

        aggregate["batch_count"] = 1
        aggregate["total_row_count"] = row_count

        return aggregate

    @classmethod
    def update(cls, current_aggregate, batch, column):
        value_counts = batch.get_column_value_counts(column)
        row_count = batch.get_row_count()

        current_aggregate["batch_count"] += 1
        current_aggregate["total_row_count"] += row_count

        for value in value_counts.index:
            if value in current_aggregate["values"]:
                current_aggregate["values"][value]["total_instances"] += value_counts[
                    value
                ]
                current_aggregate["values"][value]["batches_found_in"] += 1
            else:
                current_aggregate["values"][value] = {
                    "total_instances": value_counts[value],
                    "batches_found_in": 1,
                }

        return current_aggregate

    @classmethod
    def finalize(cls, current_aggregate):
        batch_count = current_aggregate["batch_count"]
        total_row_count = current_aggregate["total_row_count"]

        final_aggregate = current_aggregate.copy()

        for value in current_aggregate["values"]:
            batches_found_in = current_aggregate["values"][value]["batches_found_in"]
            total_instances = current_aggregate["values"][value]["total_instances"]

            batch_frequency = batches_found_in / batch_count
            row_frequency = total_instances / total_row_count

            final_aggregate["values"][value] = {
                "batch_frequency": batch_frequency,
                "row_frequency": row_frequency,
            }

        return final_aggregate
