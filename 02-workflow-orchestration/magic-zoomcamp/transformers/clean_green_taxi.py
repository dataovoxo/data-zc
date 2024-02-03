if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(f"Processing: rows with zero passengers: {data['passenger_count'].isin([0]).sum()}")

    data_clean = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    data_clean['lpep_pickup_date'] = data_clean['lpep_pickup_datetime'].dt.date
    data_clean = data_clean.rename(columns = {
                    'VendorID': 'vendor_id',
                    'RatecodeID': 'ratecode_id',
                    'PULocationID': 'pu_location_id',
                    'DOLocationID': 'do_location_id'
                })

    print(f"Processing: rows with zero passengers: {data_clean['passenger_count'].isin([0]).sum()}")
    print(f"Processing: rows with zero trip distance: {data_clean['trip_distance'].isin([0]).sum()}")
    print(data_clean.shape)
    print(data_clean['vendor_id'].unique())

    return data_clean


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, 'Vendor_ID is not a column'
    assert output['passenger_count'].isin([0]).sum() == 0, 'Records with zero passenger count in dataset'
    assert output['trip_distance'].isin([0]).sum() == 0, 'Records with zero trip distance in dataset'
