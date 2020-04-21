from great_expectations.exceptions import GreatExpectationsError


class ExpectationEngineError(GreatExpectationsError):
    pass


class UnrecognizedDataAssetError(ExpectationEngineError):
    pass


class UnimplementedBackendError(ExpectationEngineError):
    pass
