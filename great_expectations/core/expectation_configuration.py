import json
import logging
from copy import copy
from typing import Optional

from marshmallow import Schema, fields, post_load, ValidationError

from great_expectations.core.util import parse_evaluation_parameter_urn, nested_update
from great_expectations.exceptions import InvalidExpectationConfigurationError, InvalidExpectationKwargsError, \
    ParserError
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.types import DictDot

logger = logging.getLogger(__name__)

RESULT_FORMATS = [
    "BOOLEAN_ONLY",
    "BASIC",
    "COMPLETE",
    "SUMMARY"
]


class ExpectationKwargs(dict):
    ignored_keys = ['result_format', 'include_config', 'catch_exceptions']

    """ExpectationKwargs store information necessary to evaluate an expectation."""
    def __init__(self, *args, **kwargs):
        include_config = kwargs.pop("include_config", None)
        if include_config is not None and not isinstance(include_config, bool):
            raise InvalidExpectationKwargsError("include_config must be a boolean value")

        result_format = kwargs.get("result_format", None)
        if result_format is None:
            pass
        elif result_format in RESULT_FORMATS:
            pass
        elif isinstance(result_format, dict) and result_format.get('result_format', None) in RESULT_FORMATS:
            pass
        else:
            raise InvalidExpectationKwargsError("result format must be one of the valid formats: %s"
                                                % str(RESULT_FORMATS))

        catch_exceptions = kwargs.pop("catch_exceptions", None)
        if catch_exceptions is not None and not isinstance(catch_exceptions, bool):
            raise InvalidExpectationKwargsError("catch_exceptions must be a boolean value")

        super(ExpectationKwargs, self).__init__(*args, **kwargs)
        # TODO TODO FIXME -- re-enable
        #ensure_json_serializable(self)

    def isEquivalentTo(self, other):
        try:
            n_self_keys = len([k for k in self.keys() if k not in self.ignored_keys])
            n_other_keys = len([k for k in other.keys() if k not in self.ignored_keys])
            return n_self_keys == n_other_keys and all([
                self[k] == other[k] for k in self.keys() if k not in self.ignored_keys
            ])
        except KeyError:
            return False

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        # TODO TODO FIXME -- re-enable
        # myself = convert_to_json_serializable(self)
        # return myself
        return self


class RuntimeValidationConfiguration(object):
    def __init__(self, result_format=None, catch_exceptions=None):
        if result_format is None:
            self._result_format = {
                'result_format': "SUMMARY",
                'partial_unexpected_count': 20
            }
        if catch_exceptions is None:
            self._catch_exceptions = True

    def update(self, expectation_configuration: 'ExpectationConfiguration'):
        self.result_format = expectation_configuration.kwargs.get("result_format", self.result_format)
        self.catch_exceptions = expectation_configuration.kwargs.get("catch_exceptions", self.catch_exceptions)
        return self

    @property
    def result_format(self):
        return copy(self._result_format)

    @result_format.setter
    def result_format(self, result_format):
        """This is a simple helper utility that can be used to parse a string result_format into the dict format used
         internally by great_expectations. It is not necessary but allows shorthand for result_format in cases where
         there is no need to specify a custom partial_unexpected_count."""
        if isinstance(result_format, str):
            self._result_format = {
                'result_format': result_format,
                'partial_unexpected_count': 20
            }
        elif 'partial_unexpected_count' not in result_format:
            raise InvalidExpectationConfigurationError("partial_unexpected_count is required when setting "
                                                       "result_format")

    @property
    def catch_exceptions(self):
        return self._catch_exceptions

    @catch_exceptions.setter
    def catch_exceptions(self, catch_exceptions):
        if not isinstance(catch_exceptions, bool):
            raise InvalidExpectationConfigurationError("catch_exceptions must be a boolean value")
        self._catch_exceptions = catch_exceptions


class ExpectationConfiguration(DictDot):
    """ExpectationConfiguration defines the parameters and name of a specific expectation."""

    def __init__(self, expectation_type, kwargs, meta=None, success_on_last_run=None):
        if not isinstance(expectation_type, str):
            raise InvalidExpectationConfigurationError("expectation_type must be a string")
        self._expectation_type = expectation_type
        if not isinstance(kwargs, dict):
            raise InvalidExpectationConfigurationError("expectation configuration kwargs must be an "
                                                       "ExpectationKwargs object.")
        self._kwargs = ExpectationKwargs(kwargs)
        if meta is None:
            meta = {}
        # We require meta information to be serializable, but do not convert until necessary
        # ensure_json_serializable(meta)
        self.meta = meta
        self.success_on_last_run = success_on_last_run

    @property
    def expectation_type(self):
        return self._expectation_type

    @property
    def kwargs(self):
        return self._kwargs

    def isEquivalentTo(self, other):
        """ExpectationConfiguration equivalence does not include meta, and relies on *equivalence* of kwargs."""
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other = expectationConfigurationSchema.load(other)
                except ValidationError:
                    logger.debug("Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                                 "dict other could not be instantiated as an ExpectationConfiguration")
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented
        return all((
            self.expectation_type == other.expectation_type,
            self.kwargs.isEquivalentTo(other.kwargs)
        ))

    def __eq__(self, other):
        """ExpectationConfiguration equality does include meta, but ignores instance identity."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all((
            self.expectation_type == other.expectation_type,
            self.kwargs == other.kwargs,
            self.meta == other.meta
        ))

    def __ne__(self, other):
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = expectationConfigurationSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump

        # TODOFIXME TODO FIXME
        # myself['kwargs'] = convert_to_json_serializable(myself['kwargs'])
        return myself['kwargs']

    def get_evaluation_parameter_dependencies(self):
        dependencies = {}
        for key, value in self.kwargs.items():
            if isinstance(value, dict) and '$PARAMETER' in value:
                if value["$PARAMETER"].startswith("urn:great_expectations:validations:"):
                    try:
                        evaluation_parameter_id = parse_evaluation_parameter_urn(value["$PARAMETER"])
                    except ParserError:
                        logger.warning("Unable to parse great_expectations urn {}".format(value["$PARAMETER"]))
                        continue

                    if evaluation_parameter_id.metric_kwargs_id is None:
                        nested_update(dependencies, {
                            evaluation_parameter_id.expectation_suite_name: [evaluation_parameter_id.metric_name]
                        })
                    else:
                        nested_update(dependencies, {
                            evaluation_parameter_id.expectation_suite_name: [{
                                "metric_kwargs_id": {
                                    evaluation_parameter_id.metric_kwargs_id: [evaluation_parameter_id.metric_name]
                                }
                            }]
                        })
                    # if evaluation_parameter_id.expectation_suite_name not in dependencies:
                    #     dependencies[evaluation_parameter_id.expectation_suite_name] = {"metric_kwargs_id": {}}
                    #
                    # if evaluation_parameter_id.metric_kwargs_id not in dependencies[evaluation_parameter_id.expectation_suite_name]["metric_kwargs_id"]:
                    #     dependencies[evaluation_parameter_id.expectation_suite_name]["metric_kwargs_id"][evaluation_parameter_id.metric_kwargs_id] = []
                    # dependencies[evaluation_parameter_id.expectation_suite_name]["metric_kwargs_id"][
                    #     evaluation_parameter_id.metric_kwargs_id].append(evaluation_parameter_id.metric_name)

        return dependencies

    def build_runtime_configuration(self, runtime_configuration: Optional[RuntimeValidationConfiguration]) -> \
            RuntimeValidationConfiguration:
        if runtime_configuration is None:
            runtime_configuration = RuntimeValidationConfiguration()
        return runtime_configuration.update(self)

    def _get_expectation_impl(self):
        return get_expectation_impl(self.expectation_type)

    def validate(self, data, runtime_configuration=None):
        expectation_impl = self._get_expectation_impl()
        return expectation_impl(self).validate(data=data, runtime_configuration=runtime_configuration)


class ExpectationConfigurationSchema(Schema):
    expectation_type = fields.Str(
        required=True,
        error_messages={"required": "expectation_type missing in expectation configuration"}
    )
    kwargs = fields.Dict()
    meta = fields.Dict()

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_configuration(self, data, **kwargs):
        return ExpectationConfiguration(**data)



expectationConfigurationSchema = ExpectationConfigurationSchema()
