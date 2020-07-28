from great_expectations.data_context.types.base import Checkpoint, CheckpointSchema
import pytest

@pytest.fixture
def example_checkpoint():
    return Checkpoint("bob", "action_list_operator", [{"foo":"bar"}])

def test_checkpoint_serialization(example_checkpoint):
    checkpoint_schema = CheckpointSchema()
    assert checkpoint_schema.dump(example_checkpoint) == {
        'batches': [{'foo': 'bar'}],
         'checkpoint_name': 'bob',
         'validation_operator_name': 'action_list_operator'
    }


def test_checkpoint_attributes(example_checkpoint):
    assert example_checkpoint.checkpoint_name == 'bob'
    assert example_checkpoint.validation_operator_name == 'action_list_operator'
    assert example_checkpoint.batches == [{'foo':'bar'}]


# def test_checkpoint_config_attributes():
#     checkpoint_config = CheckpointConfig('bob','action_list_operator', {'foo':'bar'})
#
#     assert checkpoint_config.checkpoint_name == 'bob'
#     assert checkpoint_config.validation_operator_name == 'action_list_operator'
