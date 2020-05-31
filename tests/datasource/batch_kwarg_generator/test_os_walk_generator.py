import pytest
import os
import yaml

from great_expectations.datasource.batch_kwargs_generator import (
    OsWalkBatchKwargsGenerator,
)

def create_temp_dir_structure(base_dir, dir_list, file_list):
    for some_dir in dir_list:
        new_dir = os.path.join(base_dir, some_dir)
        os.makedirs(new_dir, exist_ok=True)
    
    for filename in file_list:
        with open(os.path.join(base_dir, filename), 'w') as f_:
            #Files are empty, since these fixtures are only for testing directory structures and file existence.
            f_.write("")
            f_.close()

@pytest.fixture
def walkable_directory_bravo(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp("walkable_directory_bravo")

    create_temp_dir_structure(
        base_dir,
        [],
        [
            'A1.csv',
            'A2.csv',
            'A3.csv',
            'B1.csv',
            'B2.csv',
            'B3.csv',
            'C1.csv',
            'C2.csv',
            'C3.csv',
            'D1.csv',
            'D2.csv',
            'D3.csv',
        ]
    )
    return base_dir

def test_basic_configuration(walkable_directory_bravo):

    my_generator = OsWalkBatchKwargsGenerator(**yaml.safe_load(r"""
base_directory: {base_dir}
data_asset_name_regexes:
  A: A(\d+).csv
  B: B(\d+).csv
  C: C(\d+).csv
  D: D(\d+).csv
""".format(**{
    "base_dir" : walkable_directory_bravo
})))

    assert set(my_generator.get_available_data_asset_names()) == set(list("ABCD"))
    assert my_generator.get_available_partition_ids(data_asset_name="A") == ["1","2","3"]
    assert my_generator.get_available_partition_ids(data_asset_name="B") == ["1","2","3"]
    assert my_generator.get_available_partition_ids(data_asset_name="C") == ["1","2","3"]
    assert my_generator.get_available_partition_ids(data_asset_name="D") == ["1","2","3"]

    my_generator = OsWalkBatchKwargsGenerator(**yaml.safe_load(r"""
base_directory: {base_dir}
data_asset_name_regexes:
  A: A(\d+).csv
  B: B(\d+).csv
  C: C\d+.csv
""".format(**{
    "base_dir" : walkable_directory_bravo
})))

    assert set(my_generator.get_available_data_asset_names()) == set(["A","B","C", "D1.csv", "D2.csv", "D3.csv"])
    assert my_generator.get_available_partition_ids(data_asset_name="A") == ["1","2","3"]
    assert my_generator.get_available_partition_ids(data_asset_name="B") == ["1","2","3"]
    assert my_generator.get_available_partition_ids(data_asset_name="C") == ["C1.csv","C2.csv","C3.csv"]
    assert my_generator.get_available_partition_ids(data_asset_name="D1.csv") == ["D1.csv"]