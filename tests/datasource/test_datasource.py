from great_expectations.datasource import Datasource


def test_list_generators_returns_empty_list_if_no_generators_exist():
    # configuration :

    execution_engine = {}

    # datasource = Datasource(name="foo", execution_engine = {})
    # assert isinstance(datasource, Datasource)
    # obs = datasource.list_batch_kwargs_generators()
    # assert obs == []
