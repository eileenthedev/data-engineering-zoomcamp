import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    df = df.rename(columns={'VendorID': 'vendor_id', 'RatecodeID': 'rate_code_id', 'PULocationID': 'pu_location_id', 'DOLocationID': 'do_location_id'})
    return df


@test
def test_passenger_count(output, *args):
    assert output['passenger_count'].isin([0]). sum() == 0, 'There are rides with zero passengers'

@test
def test_trip_distance(output, *args):
    assert output['trip_distance'].isin([0]). sum() == 0, 'There are rides with zero trip distance'


@test
def test_vendor_id_field(output, *args):
    assert set(output['vendor_id']).issubset([1,2]), 'Vendor Ids does not exist'
    assert output['vendor_id'].nunique() == 2, "Only two distinct VendorID values should be present."
    