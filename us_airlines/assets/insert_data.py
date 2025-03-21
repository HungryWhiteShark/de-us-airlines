from dagster import asset



airline_csv = 'airline_dataset/airlines.csv'
airport_csv = 'airline_dataset/airports.csv'
cancelcode_csv = 'airline_dataset/cancellation_codes.csv'
flight_csv = 'airline_dataset/flights.csv'



@asset(
    required_resource_keys={'mysql_io_manager'},
    compute_kind='MySQL',
    description='Import data of airlines from csv to MySQL DB',
    group_name='import_data_csv'
)
def get_airline_csv(context):
    table_name = 'airlines'
    
    for obj in context.resources.mysql_io_manager.load_input(context, airline_csv):
        context.resources.mysql_io_manager.handle_output(context, obj, table_name)
    

@asset(
    required_resource_keys={'mysql_io_manager'},
    compute_kind='MySQL',
    description='Import data of airports from csv to MySQL DB',
    group_name='import_data_csv'
)
def get_airport_csv(context):
    table_name = 'airports'
    for obj in context.resources.mysql_io_manager.load_input(context, airport_csv):
        context.resources.mysql_io_manager.handle_output(context, obj, table_name)


@asset(
    required_resource_keys={'mysql_io_manager'},
    compute_kind='MySQL',
    description='Import data of cancel codes from csv to MySQL DB',
    group_name='import_data_csv'
)
def get_cancelcode_csv(context):
    table_name = 'cancellation_codes'
    for obj in context.resources.mysql_io_manager.load_input(context, cancelcode_csv):
        context.resources.mysql_io_manager.handle_output(context, obj, table_name)



@asset(
    required_resource_keys={'mysql_io_manager'},
    compute_kind='MySQL',
    description='Import data of flights from csv to MySQL DB',
    group_name='import_data_csv'
)
def get_flight_csv(context):
    table_name = 'flights'
    dtype = {   
            'SCHEDULED_DEPARTURE': 'string',           
            'DEPARTURE_TIME': 'string',
            'WHEELS_OFF': 'string',
            'WHEELS_ON': 'string',    
            'SCHEDULED_ARRIVAL': 'string',
            'ARRIVAL_TIME': 'string' }
    
    for obj in context.resources.mysql_io_manager.load_input(context, flight_csv, dtype):
        context.resources.mysql_io_manager.handle_output(context, obj, table_name)


