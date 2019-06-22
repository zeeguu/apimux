# Python API Multiplexer

This library provides an easy way to manage multiple equivalent
API services. It takes as an input a list of APIs and exposes
an endpoint which can be used to fetch the results from the
registered APIs.

## How to use *apimux*
1. Implement the specific API starting from the abstract class
[BaseThirdPartyAPIService](/apimux/api_base.py#L6).

Example:
```
class FakeAPI(BaseThirdPartyAPIService):
    def __init__(self):
        super(FakeAPI, self).__init__(name='Fake API Name')

    def get_result(self, data):
        # Process the data parameter and execute the request.
        # This is specific to each API.
        result = 42  # FakeAPI returns 42 as a result
        return result
```

2. (Optional) Create a config file to overwrite the default
parameters. More details can be found in the [config file](/apimux/config.ini).

3. Create the [APIMultiplexer](/apimux/mux.py#L16) object.

Example:
```
api_mux = APIMultiplexer(
    api_list=[FakeAPI()],
    config_filepath="/opt/apimux_config.ini")
```

You can add/remove new APIs dynamically by calling the
[ADD](/apimux/mux.py#L485) or [REMOVE](/apimux/mux.py#L509) methods.

4. Fetch the data from the registered APIs by calling
[get_next_results](/apimux/mux.py#L259-L260).

Example:
```
data = ["Return", "42"]
# Now we fetch the result from one of the APIs
result = api_mux.get_next_results(data, number_of_results=1)  # 42
# To fetch the results from all registered APIs
all_results = api_mux.get_next_results(data, number_of_results=-1)
```
**Note**: The function will block until it completes. The results
are fetched in parallel when multiple results are requested. When
the number of results is passed, *apimux* will fetch the responses
from the APIs sorted by the average response times of the previous
requests (i.e. calling with ``number_of_results=1`` will return
the response from the fastest API).

A concrete example which shows how to use apimux can be found in
[Zeeguu-API](https://github.com/zeeguu-ecosystem/Zeeguu-API/blob/master/zeeguu_api/api/translator.py).
