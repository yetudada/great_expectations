"""This is currently helping bridge APIs"""
from great_expectations.dataset import PandasDataset, SparkDFDataset, SqlAlchemyDataset
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyBatchReference
from great_expectations.expectations.expectation import Expectation, DatasetExpectation
from great_expectations.expectations.registry import list_registered_expectation_implementations, get_expectation_impl
from great_expectations.types import ClassConfig
from great_expectations.util import load_class, verify_dynamic_loading_support


class Validator(object):
    def __init__(self, batch, expectation_suite, expectation_engine=None, **kwargs):
        self.batch = batch
        if expectation_suite is None:
            expectation_suite = ExpectationSuite(expectation_suite_name="foo")
        self.expectation_suite = expectation_suite
        for expectation in self._expectations:
            setattr(self, expectation, partial(self._process_expectation, expectation))
        self._config = {
            "interactive_evaluation": True
        }
        self._active_validation = False
        self.runtime_configuration = RuntimeValidationConfiguration()

        if isinstance(expectation_engine, dict):
            expectation_engine = ClassConfig(**expectation_engine)

        if isinstance(expectation_engine, ClassConfig):
            module_name = expectation_engine.module_name or "great_expectations.dataset"
            verify_dynamic_loading_support(module_name=module_name)
            expectation_engine = load_class(
                class_name=expectation_engine.class_name, module_name=module_name
            )

        self.expectation_engine = expectation_engine
        if self.expectation_engine is None:
            # Guess the engine
            try:
                import pandas as pd

                if isinstance(batch.data, pd.DataFrame):
                    self.expectation_engine = PandasDataset
            except ImportError:
                pass
        if self.expectation_engine is None:
            if isinstance(batch.data, SqlAlchemyBatchReference):
                self.expectation_engine = SqlAlchemyDataset

        if self.expectation_engine is None:
            try:
                import pyspark

                if isinstance(batch.data, pyspark.sql.DataFrame):
                    self.expectation_engine = SparkDFDataset
            except ImportError:
                pass

        if self.expectation_engine is None:
            raise ValueError(
                "Unable to identify expectation_engine. It must be a subclass of DataAsset."
            )

        self.init_kwargs = kwargs

    def _process_expectation(self, expectation_name, *args, **kwargs):
        expectation_impl = get_expectation_impl(expectation_name)
        configuration = expectation_impl.build_configuration(*args, **kwargs)
        runtime_configuration = configuration.build_runtime_configuration(self.runtime_configuration)
        raised_exception = False
        exception_traceback = None
        exception_message = None

        # Finally, execute the expectation method itself
        if self._config.get("interactive_evaluation", True) or self._active_validation:
            try:
                # TODO: HANDLE RUNTIME CONFIGURATION
                return_obj = expectation_impl(configuration).validate(self.batch.data)
            except Exception as err:
                if runtime_configuration.catch_exceptions:
                    raised_exception = True
                    exception_traceback = traceback.format_exc()
                    exception_message = "{}: {}".format(type(err).__name__, str(err))

                    return_obj = ExpectationValidationResult(success=False)

                else:
                    raise err

        else:
            return_obj = ExpectationValidationResult(expectation_config=configuration)

        # If validate has set active_validation to true, then we do not save the config to avoid
        # saving updating expectation configs to the same suite during validation runs
        if self._active_validation is True:
            pass
        else:
            # Append the expectation to the config.
            self.expectation_suite.append_expectation(configuration)

        return_obj.expectation_config = configuration

        # If there was no interactive evaluation, success will not have been computed.
        if return_obj.success is not None:
            # Add a "success" object to the config
            configuration.success_on_last_run = return_obj.success

        if runtime_configuration.catch_exceptions:
            return_obj.exception_info = {
                "raised_exception": raised_exception,
                "exception_message": exception_message,
                "exception_traceback": exception_traceback
            }

        # Add meta to return object
        if configuration.meta is not None:
            return_obj.meta = configuration.meta

        if self.batch.data_context is not None:
            return_obj = self.batch.data_context.update_return_obj(self, return_obj)

        return return_obj


    def get_dataset(self):
        if issubclass(self.expectation_engine, PandasDataset):
            import pandas as pd

            if not isinstance(self.batch["data"], pd.DataFrame):
                raise ValueError(
                    "PandasDataset expectation_engine requires a Pandas Dataframe for its batch"
                )

            return self.expectation_engine(
                self.batch.data,
                expectation_suite=self.expectation_suite,
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                **self.init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )

        elif issubclass(self.expectation_engine, SqlAlchemyDataset):
            if not isinstance(self.batch.data, SqlAlchemyBatchReference):
                raise ValueError(
                    "SqlAlchemyDataset expectation_engine requires a SqlAlchemyBatchReference for its batch"
                )

            init_kwargs = self.batch.data.get_init_kwargs()
            init_kwargs.update(self.init_kwargs)
            return self.expectation_engine(
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                expectation_suite=self.expectation_suite,
                **init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )

        elif issubclass(self.expectation_engine, SparkDFDataset):
            import pyspark

            if not isinstance(self.batch.data, pyspark.sql.DataFrame):
                raise ValueError(
                    "SparkDFDataset expectation_engine requires a spark DataFrame for its batch"
                )

            return self.expectation_engine(
                spark_df=self.batch.data,
                expectation_suite=self.expectation_suite,
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                **self.init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )
