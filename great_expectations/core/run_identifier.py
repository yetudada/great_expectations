import datetime
import warnings
from parser import ParserError

from dateutil.parser import parse
from marshmallow import Schema, fields, post_load

from great_expectations.core.data_context_key import DataContextKey


class RunIdentifier(DataContextKey):
    """A RunIdentifier identifies a run (collection of validations) by run_name and run_time."""

    def __init__(self, run_name=None, run_time=None):
        super(RunIdentifier, self).__init__()
        self._run_name = run_name

        if isinstance(run_time, str):
            try:
                run_time = parse(run_time)
            except ParserError:
                warnings.warn(
                    f'Unable to parse provided run_time str ("{run_time}") to datetime. Defaulting '
                    f"run_time to current time."
                )
                run_time = datetime.datetime.now(datetime.timezone.utc)

        self._run_time = run_time or datetime.datetime.now(datetime.timezone.utc)

    @property
    def run_name(self):
        return self._run_name

    @property
    def run_time(self):
        return self._run_time

    def to_tuple(self):
        return self._run_name or "__none__", self._run_time.isoformat()

    def to_fixed_length_tuple(self):
        return self._run_name or "__none__", self._run_time.isoformat()

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = runIdentifierSchema.dump(self)
        return myself

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(tuple_[0], tuple_[1])

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls(tuple_[0], tuple_[1])


class RunIdentifierSchema(Schema):
    run_name = fields.Str()
    run_time = fields.DateTime(format="iso")

    @post_load
    def make_run_identifier(self, data, **kwargs):
        return RunIdentifier(**data)


runIdentifierSchema = RunIdentifierSchema()
