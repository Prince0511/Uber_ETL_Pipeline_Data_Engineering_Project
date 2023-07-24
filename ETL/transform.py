import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    #Converting the datatypes of some columns

    df['passenger_count'] = df['passenger_count'].astype(int)
    df['RatecodeID'] = df['RatecodeID'].astype(int)
    df['extra'] = df['extra'].round(2)
    df['tolls_amount'] = df['tolls_amount'].round(2)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    #Creating datetime_dim table
    #Extracting hour, day, month, year, and weekday from datetime column

    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].reset_index(drop = True)

    datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index

    #Arranging the columns according to our data model

    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 
                                'pick_month', 'pick_year', 'pick_weekday',
                                'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 
                                'drop_month', 'drop_year', 'drop_weekday']]

    #Creating passenger_count_dim table

    passenger_count_dim = df[['passenger_count']].reset_index(drop = True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index

    #Arranging the columns according to our data model

    passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

    #Creating trip_distance_dim table

    trip_distance_dim = df[['trip_distance']].reset_index(drop = True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index

    #Arranging the columns according to our data model

    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

    #Creating a dictionary for each Rate Code Type

    rate_code_type = {
        1:'Standard rate',
        2:'JFK',
        3:'Newark',
        4:'Nassau or Westchester',
        5:'Negotiated fare',
        6:'Group ride',
        99: 'Other'
    }

    #Creating rate_code_dim table

    rate_code_dim = df[['RatecodeID']].reset_index(drop = True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index

    #Mapping the values of Rate Code to our dictionary

    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)

    #Arranging the columns according to our data model

    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

    #Creating a dictionary for each Payment Type

    payment_type_name = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip',
    }

    #Creating rate_code_dim table

    payment_type_dim = df[['payment_type']].reset_index(drop = True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index

    #Mapping the values of Payment Type to our dictionary

    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)

    #Arranging the columns according to our data model

    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    #Merging the columns with common attributed to produce our Fact Table 

    fact_table = df.merge(passenger_count_dim, left_on = 'trip_id', right_on = 'passenger_count_id') \
                .merge(trip_distance_dim, left_on = 'trip_id', right_on = 'trip_distance_id') \
                .merge(rate_code_dim, left_on = 'trip_id', right_on = 'rate_code_id') \
                .merge(datetime_dim, left_on = 'trip_id', right_on = 'datetime_id') \
                .merge(payment_type_dim, left_on = 'trip_id', right_on = 'payment_type_id') \
                [['trip_id', 'VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag',
                'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                'congestion_surcharge', 'Airport_fee']]

    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
        "passenger_count_dim":passenger_count_dim.to_dict(orient="dict"),
        "trip_distance_dim":trip_distance_dim.to_dict(orient="dict"),
        "rate_code_dim":rate_code_dim.to_dict(orient="dict"),
        "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
        "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
