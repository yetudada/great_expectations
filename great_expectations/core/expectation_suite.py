import datetime
import json
import logging
from copy import deepcopy

from marshmallow import post_load, pre_dump, Schema, fields, ValidationError

from great_expectations import __version__ as ge_version
from great_expectations.core.expectation_configuration import ExpectationConfigurationSchema, ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable, ensure_json_serializable, nested_update

logger = logging.getLogger(__name__)

class ExpectationSuite(object):
    def __init__(
        self,
        expectation_suite_name,
        expectations=None,
        evaluation_parameters=None,
        data_asset_type=None,
        meta=None
    ):
        self.expectation_suite_name = expectation_suite_name
        if expectations is None:
            expectations = []
        self.expectations = [ExpectationConfiguration(**expectation) if isinstance(expectation, dict) else
                             expectation for expectation in expectations]
        if evaluation_parameters is None:
            evaluation_parameters = {}
        self.evaluation_parameters = evaluation_parameters
        self.data_asset_type = data_asset_type
        if meta is None:
            meta = {"great_expectations.__version__": ge_version}
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta

    def add_citation(self, comment, batch_kwargs=None, batch_markers=None, batch_parameters=None, citation_date=None):
        if "citations" not in self.meta:
            self.meta["citations"] = []
        self.meta["citations"].append({
            "citation_date": citation_date or datetime.datetime.now().isoformat(),
            "batch_kwargs": batch_kwargs,
            "batch_markers": batch_markers,
            "batch_parameters": batch_parameters,
            "comment": comment
        })

    def isEquivalentTo(self, other):
        """
        ExpectationSuite equivalence relies only on expectations and evaluation parameters. It does not include:
        - data_asset_name
        - expectation_suite_name
        - meta
        - data_asset_type
        """
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other = expectationSuiteSchema.load(other)
                except ValidationError:
                    logger.debug("Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                                 "dict other could not be instantiated as an ExpectationConfiguration")
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented
        return all(
            [mine.isEquivalentTo(theirs) for (mine, theirs) in zip(self.expectations, other.expectations)]
        )

    def __eq__(self, other):
        """ExpectationSuite equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all((
            self.expectation_suite_name == other.expectation_suite_name,
            self.expectations == other.expectations,
            self.evaluation_parameters == other.evaluation_parameters,
            self.data_asset_type == other.data_asset_type,
            self.meta == other.meta,
        ))

    def __ne__(self, other):
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = expectationSuiteSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself['expectations'] = convert_to_json_serializable(myself['expectations'])
        try:
            myself['evaluation_parameters'] = convert_to_json_serializable(myself['evaluation_parameters'])
        except KeyError:
            pass  # Allow evaluation parameters to be missing if empty
        myself['meta'] = convert_to_json_serializable(myself['meta'])
        return myself

    def get_evaluation_parameter_dependencies(self):
        dependencies = {}
        for expectation in self.expectations:
            t = expectation.get_evaluation_parameter_dependencies()
            nested_update(dependencies, t)

        return dependencies

    def get_citations(self, sort=True, require_batch_kwargs=False):
        citations = self.meta.get("citations", [])
        if require_batch_kwargs:
            citations = self._filter_citations(citations, "batch_kwargs")
        if not sort:
            return citations
        return self._sort_citations(citations)

    def get_table_expectations(self):
        """Return a list of table expectations."""
        return [e for e in self.expectations if e.expectation_type.startswith("expect_table_")]

    def get_column_expectations(self):
        """Return a list of column map expectations."""
        return [e for e in self.expectations if "column" in e.kwargs]

    @staticmethod
    def _filter_citations(citations, filter_key):
        citations_with_bk = []
        for citation in citations:
            if filter_key in citation and citation.get(filter_key):
                citations_with_bk.append(citation)
        return citations_with_bk

    @staticmethod
    def _sort_citations(citations):
        return sorted(citations, key=lambda x: x["citation_date"])


    def _copy_and_clean_up_expectation(self,
        expectation,
        discard_result_format_kwargs=True,
        discard_include_config_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        """Returns copy of `expectation` without `success_on_last_run` and other specified key-value pairs removed

          Returns a copy of specified expectation will not have `success_on_last_run` key-value. The other key-value \
          pairs will be removed by default but will remain in the copy if specified.

          Args:
              expectation (json): \
                  The expectation to copy and clean.
              discard_result_format_kwargs (boolean): \
                  if True, will remove the kwarg `output_format` key-value pair from the copied expectation.
              discard_include_config_kwargs (boolean):
                  if True, will remove the kwarg `include_config` key-value pair from the copied expectation.
              discard_catch_exceptions_kwargs (boolean):
                  if True, will remove the kwarg `catch_exceptions` key-value pair from the copied expectation.

          Returns:
              A copy of the provided expectation with `success_on_last_run` and other specified key-value pairs removed

          Note:
              This method may move to ExpectationConfiguration, minus the "copy" part.
        """
        new_expectation = deepcopy(expectation)

        if "success_on_last_run" in new_expectation:
            del new_expectation["success_on_last_run"]

        if discard_result_format_kwargs:
            if "result_format" in new_expectation.kwargs:
                del new_expectation.kwargs["result_format"]
                # discards["result_format"] += 1

        if discard_include_config_kwargs:
            if "include_config" in new_expectation.kwargs:
                del new_expectation.kwargs["include_config"]
                # discards["include_config"] += 1

        if discard_catch_exceptions_kwargs:
            if "catch_exceptions" in new_expectation.kwargs:
                del new_expectation.kwargs["catch_exceptions"]
                # discards["catch_exceptions"] += 1

        return new_expectation

    def _copy_and_clean_up_expectations_from_indexes(
        self,
        match_indexes,
        discard_result_format_kwargs=True,
        discard_include_config_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        """Copies and cleans all expectations provided by their index in DataAsset._expectation_suite.expectations.

           Applies the _copy_and_clean_up_expectation method to multiple expectations, provided by their index in \
           `DataAsset,_expectation_suite.expectations`. Returns a list of the copied and cleaned expectations.

           Args:
               match_indexes (List): \
                   Index numbers of the expectations from `expectation_config.expectations` to be copied and cleaned.
               discard_result_format_kwargs (boolean): \
                   if True, will remove the kwarg `output_format` key-value pair from the copied expectation.
               discard_include_config_kwargs (boolean):
                   if True, will remove the kwarg `include_config` key-value pair from the copied expectation.
               discard_catch_exceptions_kwargs (boolean):
                   if True, will remove the kwarg `catch_exceptions` key-value pair from the copied expectation.

           Returns:
               A list of the copied expectations with `success_on_last_run` and other specified \
               key-value pairs removed.

           See also:
               _copy_and_clean_expectation
        """
        rval = []
        for i in match_indexes:
            rval.append(
                self._copy_and_clean_up_expectation(
                    self.expectations[i],
                    discard_result_format_kwargs,
                    discard_include_config_kwargs,
                    discard_catch_exceptions_kwargs,
                )
            )

        return rval

    ### CRUD methods ###

    def append_expectation(self, expectation_config):
        """Appends an expectation.

           Args:
               expectation_config (ExpectationConfiguration): \
                   The expectation to be added to the list.

           Notes:
               May want to add type-checking in the future.
        """
        self.expectations.append(expectation_config)

    def find_expectation_indexes(self,
        expectation_type=None,
        column=None,
        expectation_kwargs=None
    ):
        """Find matching expectations and return their indexes.
        Args:
            expectation_type=None                : The name of the expectation type to be matched.
            column=None                          : The name of the column to be matched.
            expectation_kwargs=None              : A dictionary of kwargs to match against.

        Returns:
            A list of indexes for matching expectation objects.
            If there are no matches, the list will be empty.
        """
        if expectation_kwargs is None:
            expectation_kwargs = {}

        if "column" in expectation_kwargs and column is not None and column is not expectation_kwargs["column"]:
            raise ValueError("Conflicting column names in find_expectation_indexes: %s and %s" % (
                column, expectation_kwargs["column"]))

        if column is not None:
            expectation_kwargs["column"] = column

        match_indexes = []
        for i, exp in enumerate(self.expectations):
            if expectation_type is None or (expectation_type == exp.expectation_type):
                # if column == None or ('column' not in exp['kwargs']) or
                # (exp['kwargs']['column'] == column) or (exp['kwargs']['column']==:
                match = True

                for k, v in expectation_kwargs.items():
                    if k in exp['kwargs'] and exp['kwargs'][k] == v:
                        continue
                    else:
                        match = False

                if match:
                    match_indexes.append(i)

        return match_indexes

    def find_expectations(self,
        expectation_type=None,
        column=None,
        expectation_kwargs=None,
        discard_result_format_kwargs=True,
        discard_include_config_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        """Find matching expectations and return them.
        Args:
            expectation_type=None                : The name of the expectation type to be matched.
            column=None                          : The name of the column to be matched.
            expectation_kwargs=None              : A dictionary of kwargs to match against.
            discard_result_format_kwargs=True    : In returned expectation object(s), \
            suppress the `result_format` parameter.
            discard_include_config_kwargs=True  : In returned expectation object(s), \
            suppress the `include_config` parameter.
            discard_catch_exceptions_kwargs=True : In returned expectation object(s), \
            suppress the `catch_exceptions` parameter.

        Returns:
            A list of matching expectation objects.
            If there are no matches, the list will be empty.
        """

        match_indexes = self.find_expectation_indexes(
            expectation_type,
            column,
            expectation_kwargs,
        )

        return self._copy_and_clean_up_expectations_from_indexes(
            match_indexes,
            discard_result_format_kwargs,
            discard_include_config_kwargs,
            discard_catch_exceptions_kwargs,
        )

    def remove_expectation(self,
        expectation_type=None,
        column=None,
        expectation_kwargs=None,
        remove_multiple_matches=False,
        dry_run=False,
    ):
        """Remove matching expectation(s).
        Args:
            expectation_type=None                : The name of the expectation type to be matched.
            column=None                          : The name of the column to be matched.
            expectation_kwargs=None              : A dictionary of kwargs to match against.
            remove_multiple_matches=False        : Match multiple expectations
            dry_run=False                        : Return a list of matching expectations without removing

        Returns:
            None, unless dry_run=True.
            If dry_run=True and remove_multiple_matches=False then return the expectation that *would be* removed.
            If dry_run=True and remove_multiple_matches=True then return a list of expectations that *would be* removed.

        Note:
            If remove_expectation doesn't find any matches, it raises a ValueError.
            If remove_expectation finds more than one matches and remove_multiple_matches!=True, it raises a ValueError.
            If dry_run=True, then `remove_expectation` acts as a thin layer to find_expectations, with the default \
            values for discard_result_format_kwargs, discard_include_config_kwargs, and discard_catch_exceptions_kwargs
        """

        match_indexes = self.find_expectation_indexes(
            expectation_type,
            column,
            expectation_kwargs,
        )

        if len(match_indexes) == 0:
            raise ValueError('No matching expectation found.')

        elif len(match_indexes) > 1:
            if not remove_multiple_matches:
                raise ValueError(
                    'Multiple expectations matched arguments. No expectations removed.')
            else:

                if not dry_run:
                    self.expectations = [i for j, i in enumerate(
                        self.expectations) if j not in match_indexes]
                else:
                    return self._copy_and_clean_up_expectations_from_indexes(match_indexes)

        else:  # Exactly one match
            expectation = self._copy_and_clean_up_expectation(
                self.expectations[match_indexes[0]]
            )

            if not dry_run:
                del self.expectations[match_indexes[0]]

            else:
                if remove_multiple_matches:
                    return [expectation]
                else:
                    return expectation

class ExpectationSuiteSchema(Schema):
    expectation_suite_name = fields.Str()
    expectations = fields.List(fields.Nested(ExpectationConfigurationSchema))
    evaluation_parameters = fields.Dict(allow_none=True)
    data_asset_type = fields.Str(allow_none=True)
    meta = fields.Dict()

    # NOTE: 20191107 - JPC - we may want to remove clean_empty and update tests to require the other fields;
    # doing so could also allow us not to have to make a copy of data in the pre_dump method.
    def clean_empty(self, data):
        if not hasattr(data, 'evaluation_parameters'):
            pass
        elif len(data.evaluation_parameters) == 0:
            del data.evaluation_parameters

        if not hasattr(data, 'meta'):
            pass
        elif data.meta is None or data.meta == []:
            pass
        elif len(data.meta) == 0:
            del data.meta
        return data

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        data.meta = convert_to_json_serializable(data.meta)
        data = self.clean_empty(data)
        return data

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite(self, data, **kwargs):
        return ExpectationSuite(**data)

expectationSuiteSchema = ExpectationSuiteSchema()
