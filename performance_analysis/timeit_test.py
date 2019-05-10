import argparse
import timeit

from apimux.api_base import BaseThirdPartyAPIService
from apimux.mux import APIMultiplexer


class NoopAPI(BaseThirdPartyAPIService):
    def __init__(self, number=0):
        super(NoopAPI, self).__init__(name='No operation API - %s' % number)

    def get_result(self, data):
        return "noop"


parser = argparse.ArgumentParser()
parser.add_argument('--number-of-apis',
                    help="Number of APIs to initialize for the test.",
                    type=int,
                    default=1)
parser.add_argument('--number-of-calls',
                    help="Number of calls to apimux.get_next_results",
                    type=int,
                    default=1000)
parser.add_argument('--number-of-results',
                    help="Number of results to fetch from apimux",
                    type=int,
                    default=-1)
args = parser.parse_args()


api_list = []
for i in range(0, args.number_of_apis):
    api_list.append(NoopAPI(number=i))

apimux = APIMultiplexer(api_list=api_list,
                        config_filepath="./apimux_sample.ini")


def wrapper(func, *args, **kwargs):
    def wrapped():
        return func(*args, **kwargs)
    return wrapped


data = None  # No data required to be processed
number_of_results = args.number_of_results
wrapped = wrapper(apimux.get_next_results,
                  data=data,
                  number_of_results=number_of_results)

if __name__ == '__main__':
    result = timeit.timeit(wrapped, number=args.number_of_calls)
    print("loops: %s, apis %s, results %s, \t%s\t%s" % (
          args.number_of_calls,
          args.number_of_apis,
          args.number_of_results,
          result / args.number_of_calls,
          result))
