# libraries imports here

# function that retry and abort on attempts to do something
def retry_abort(max_retries=3):
    for attempt in range(1, max_retries + 1):
        if attempt <= max_retries:
            pass # try again
        else:
            pass # abort

# function that queries the location used in the nearby search
def query_lat_lon():
    pass