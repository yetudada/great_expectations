import logging
from collections import namedtuple
from collections.abc import Mapping

from IPython import get_ipython

from great_expectations.exceptions import (
    InvalidExpectationConfigurationError,
    ParserError,
)

EvaluationParameterIdentifier = namedtuple(
    "EvaluationParameterIdentifier",
    ["expectation_suite_name", "metric_name", "metric_kwargs_id"],
)

logger = logging.getLogger(__name__)

# Updated from the stack overflow version below to concatenate lists
# https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
def nested_update(d, u):
    for k, v in u.items():
        if isinstance(v, Mapping):
            d[k] = nested_update(d.get(k, {}), v)
        elif isinstance(v, list) or (k in d and isinstance(d[k], list)):
            l1 = d.get(k, [])
            l2 = v or []
            d[k] = l1 + l2
        else:
            d[k] = v
    return d


# function to determine if code is being run from a Jupyter notebook
def in_jupyter_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def get_metric_kwargs_id(metric_name, metric_kwargs):
    ###
    #
    # WARNING
    # WARNING
    # THIS IS A PLACEHOLDER UNTIL WE HAVE REFACTORED EXPECTATIONS TO HANDLE THIS LOGIC THEMSELVES
    # WE ARE NO WORSE OFF THAN THE PREVIOUS SYSTEM, BUT NOT FULLY CUSTOMIZABLE
    # WARNING
    # WARNING
    #
    ###
    if "metric_kwargs_id" in metric_kwargs:
        return metric_kwargs["metric_kwargs_id"]
    if "column" in metric_kwargs:
        return "column=" + metric_kwargs.get("column")
    return None


def parse_evaluation_parameter_urn(urn):
    if urn.startswith("urn:great_expectations:validations:"):
        split = urn.split(":")
        if len(split) == 6:
            return EvaluationParameterIdentifier(split[3], split[4], split[5])
        elif len(split) == 5:
            return EvaluationParameterIdentifier(split[3], split[4], None)
        else:
            raise ParserError(
                "Unable to parse URN: must have 5 or 6 components to be a valid GE URN"
            )

    raise ParserError("Unrecognized evaluation parameter urn {}".format(urn))


def convert_to_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable

    Args:
        data: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    import numpy as np
    import pandas as pd
    import datetime
    import decimal
    import sys

    try:
        if not isinstance(data, list) and np.isnan(data):
            # np.isnan is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return None
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, (str, int, float, bool)):
        # No problem to encode json
        return data

    elif isinstance(data, dict):
        new_dict = {}
        for key in data:
            # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string
            new_dict[str(key)] = convert_to_json_serializable(data[key])

        return new_dict

    elif isinstance(data, (list, tuple, set)):
        new_list = []
        for val in data:
            new_list.append(convert_to_json_serializable(val))

        return new_list

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        return [convert_to_json_serializable(x) for x in data.tolist()]

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return data

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return bool(data)

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return int(data)

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(data, sys.float_info.dig))

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        return [
            {
                index_name: convert_to_json_serializable(idx),
                value_name: convert_to_json_serializable(val),
            }
            for idx, val in data.iteritems()
        ]

    elif isinstance(data, pd.DataFrame):
        return convert_to_json_serializable(data.to_dict(orient="records"))

    elif isinstance(data, decimal.Decimal):
        if not (-1e-55 < decimal.Decimal.from_float(float(data)) - data < 1e-55):
            logger.warning(
                "Using lossy conversion for decimal %s to float object to support serialization."
                % str(data)
            )
        return float(data)

    else:
        raise TypeError(
            "%s is of type %s which cannot be serialized."
            % (str(data), type(data).__name__)
        )


def ensure_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable

    Args:
        data: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    import numpy as np
    import pandas as pd
    from six import string_types, integer_types
    import datetime
    import decimal

    try:
        if not isinstance(data, list) and np.isnan(data):
            # np.isnan is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, (string_types, integer_types, float, bool)):
        # No problem to encode json
        return

    elif isinstance(data, dict):
        for key in data:
            str(key)  # key must be cast-able to string
            ensure_json_serializable(data[key])

        return

    elif isinstance(data, (list, tuple, set)):
        for val in data:
            ensure_json_serializable(val)
        return

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        _ = [ensure_json_serializable(x) for x in data.tolist()]
        return

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        _ = [
            {
                index_name: ensure_json_serializable(idx),
                value_name: ensure_json_serializable(val),
            }
            for idx, val in data.iteritems()
        ]
        return
    elif isinstance(data, pd.DataFrame):
        return ensure_json_serializable(data.to_dict(orient="records"))

    elif isinstance(data, decimal.Decimal):
        return

    else:
        raise InvalidExpectationConfigurationError(
            "%s is of type %s which cannot be serialized to json"
            % (str(data), type(data).__name__)
        )
