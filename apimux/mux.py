from concurrent.futures import ThreadPoolExecutor, TimeoutError, CancelledError
import numpy
import operator
from threading import Thread, Lock
import time
from timeit import default_timer as timer

from apimux.log import logger
from apimux.constants import PERIODIC_CHECK, MAX_WORKERS
from apimux.rwlock import ReadWriteLock


PERCENTILE = 90
MAX_HISTORY_RTIME = 100
# The response time for an API will be the minimum between MAX_WAIT_TIME and
# the value obtained by calculating the percentile of all response times so far
# MAX_WAIT_TIME = 1000  # ms
MAX_WAIT_TIME = 0


class APIMultiplexer(object):
    def __init__(self, api_list=[], enable_periodic_check=False,
                 ignore_slow_apis=False):
        logger.debug("Initializing the APIMultiplexer class")
        self._dynamic_api_registering = True
        self._locks = {}
        self._api_list = {}
        self._locks["_api_priority"] = Lock()
        self._api_priority = {}
        self._locks["_api_response_times"] = Lock()
        self._api_response_times = {}
        self._locks["_percentile_map"] = Lock()
        self._percentile_map = {}

        self._futures_not_finished = {}
        self._locks["_futures_not_finished"] = Lock()

        # Additional locks
        self._locks["add_remove_api"] = ReadWriteLock()

        if len(api_list) == 0:
            # No APIs passed, enabling dynamic registering/unregistering
            # of new APIs with the MAX_WORKERS number of threads
            self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        else:
            self._executor = ThreadPoolExecutor(max_workers=len(api_list))
            for x in api_list:
                # Registering all APIs passed as parameters
                self.register_new_api(x)
            self._dynamic_api_registering = False

        # Queue new requests and wait for them even when the API is slow
        self._ignore_slow_apis = ignore_slow_apis

        if enable_periodic_check:
            # Periodic check thread
            self._periodic_check_thread = Thread(
                target=self._periodic_priority_check, args=())
            self._periodic_check_thread.setDaemon(True)
            self._periodic_check_thread.start()

    def _time_function(self, f, data):
        start = timer()
        result = f(data)
        end = timer()
        elapsed_ms = (end - start) * 1000
        return (result, elapsed_ms)

    def _get_result(self, api, data):
        try:
            result, response_time = self._time_function(api.get_result, data)
            self._process_response_time_api(response_time, api)
            return result
        except Exception as e:
            logger.warning("API raised exception %s" % e)
            logger.info("Resetting priority for API '%s'" % api.name)
            with self._locks["_api_priority"]:
                self._api_priority[api.name] = -1
        return None

    def _get_fastest_apiname(self):
        with self._locks["_percentile_map"]:
            sorted_percentile = sorted(self._percentile_map.items(),
                                       key=operator.itemgetter(1))
        fastest_api = sorted_percentile.pop()[0]
        logger.debug("Fastest API available: %s" % fastest_api)
        return fastest_api.name

    def get_result(self, data):
        """Gets the first result from the best API."""
        with self._locks["_percentile_map"]:
            sorted_percentile = sorted(self._percentile_map.items(),
                                       key=operator.itemgetter(1))
        logger.debug("Sorted by response time median: %s" % sorted_percentile)
        with self._locks["add_remove_api"]:
            api = self._api_list.get(sorted_percentile.pop()[0])
        logger.info("Chosen API for request: %s" % api.name)

        result = self._get_result(api, data)
        while result is None and len(sorted_percentile) > 0:
            with self._locks["add_remove_api"]:
                api = self._api_list.get(sorted_percentile.pop()[0])
            logger.info("Reconfiguring API for request: %s" % api.name)
            result = self._get_result(api, data)

        return result

    def get_next_result(self, data):
        """Gets the next result from the next best API."""
        return "fake next"

    def _remove_pending_api_request(self, future):
        logger.warning("API responded with: %s" % future.result())
        with self._locks["_futures_not_finished"]:
            fut = self._futures_not_finished.pop(future, None)
        if fut is None:
            logger.warning("Tried to remove inexistent pending request")

    def get_all_results(self, data):
        """Gets the result from all APIs."""
        returned_data = []
        with self._locks["add_remove_api"]:
            future_to_api = {}
            for apiname in self._api_list:
                if self._ignore_slow_apis:
                    with self._locks["_futures_not_finished"]:
                        if apiname in self._futures_not_finished.values():
                            continue
                api = self._api_list.get(apiname)
                future_to_api[self._executor.submit(
                    self._get_result, api, data)] = apiname
            logger.debug("Futures to api map: %s" % future_to_api)
            has_results = False
            second_loop = False
            while not has_results:
                for future in future_to_api:
                    apiname = future_to_api[future]
                    data = None
                    with self._locks["_percentile_map"]:
                        timeout_result = self._percentile_map.get(
                            apiname, None)
                    if timeout_result is not None:
                        timeout_result += timeout_result * 0.25
                        if timeout_result > MAX_WAIT_TIME:
                            timeout_result = MAX_WAIT_TIME
                            timeout_result /= len(self._api_list)
                        timeout_result /= 1000
                    if second_loop:
                        # Iterate as quickly as possible to fetch a valid
                        # result to return on second loop
                        timeout_result = 0.01
                    try:
                        if not second_loop:
                            # Ignore logs on second loop
                            logger.debug("Waiting for result from %s for %s "
                                         "sec" % (apiname, timeout_result))
                        data = future.result(timeout=timeout_result)
                        if MAX_WAIT_TIME > 0:
                            logger.debug("Marking has_results to true")
                            has_results = True
                        elif len(self._api_list) == len(returned_data):
                            if not has_results:
                                logger.debug("Marking has_results to true. "
                                             "Fetched data from all APIs.")
                            has_results = True
                        response_to_add = (apiname, data)
                        if response_to_add not in returned_data:
                            returned_data.append(response_to_add)
                    except (TimeoutError, CancelledError) as e:
                        if not second_loop:
                            logger.warning("%s generated an exception: %s"
                                           % (apiname, type(e)))
                            logger.warning("Ignoring API '%s' until it "
                                           "finishes current request"
                                           % apiname)
                        with self._locks["_futures_not_finished"]:
                            if self._futures_not_finished.get(future, None):
                                # Do not add the request again if it already
                                # exists
                                continue
                            self._futures_not_finished[future] = apiname
                        future.add_done_callback(
                            self._remove_pending_api_request)
                    except Exception as e:
                        logger.warning("Unknown exception occured: %s" % e)
                    else:
                        if second_loop and MAX_WAIT_TIME > 0:
                            logger.info(
                                "Received: '%s' from '%s'" % (data, apiname))
                    # TODO: Should the API appear with no data in the response?
                    # returned_data[apiname] = data
                second_loop = True
            logger.debug("Futures to api map: %s" % future_to_api)
        logger.debug("Returning data: %s" % returned_data)
        return returned_data

    def register_new_api(self, api):
        """Registers new API and adds it to the list for monitoring."""
        logger.info("New API to register: %s" % api.name)
        if not self._dynamic_api_registering:
            raise Exception("Dynamic registration of APIs is disabled")
        # with self._locks["add_remove_api"]:
        self._locks["add_remove_api"].acquire_write()
        if self._api_list.get(api.name, None) is not None:
            raise Exception("API already exists in the list")
        self._api_list[api.name] = api
        self._locks["add_remove_api"].release_write()
        # All APIs start from 0 initially, this will be automatically
        # reconfigured based on the performance of the APIs.
        with self._locks["_api_priority"]:
            self._api_priority[api.name] = 0
            # self._api_priority[api.name]["latency"] = 0
        with self._locks["_api_response_times"]:
            self._api_response_times[api.name] = []
        with self._locks["_percentile_map"]:
            self._percentile_map[api.name] = None
        logger.info("New list: %s" % self._api_list.keys())

    def remove_api(self, api):
        """Removes API from the backend list."""
        logger.info("Removing API: %s" % api.name)
        if not self._dynamic_api_registering:
            raise Exception("Dynamic removing of APIs is disabled")
        # with self._locks["add_remove_api"]:
        self._locks["add_remove_api"].acquire_write()
        removed_api = self._api_list.pop(api.name, None)
        self._locks["add_remove_api"].release_write()
        if removed_api is not None:
            logger.debug("Removed API")
        else:
            logger.debug("Tried to remove API which is "
                         "not present in the list")
            return
        with self._locks["_api_priority"]:
            self._api_priority.pop(api.name, None)
        with self._locks["_api_response_times"]:
            self._api_response_times.pop(api.name, None)
        with self._locks["_percentile_map"]:
            self._percentile_map.pop(api.name, None)
        logger.info("New list: %s" % self._api_list.keys())

    def _process_response_time_api(self, response_time, api):
        logger.info("%s: response time %sms"
                    % (api.name, response_time))
        with self._locks["_api_response_times"]:
            if len(self._api_response_times[api.name]) > MAX_HISTORY_RTIME:
                # Remove from the history once it reaches max limit
                self._api_response_times[api.name].pop(0)
            self._api_response_times[api.name] += [response_time]
            # Sorted returns a new cloned list
            np_array = numpy.array(sorted(self._api_response_times[api.name]))
        p = numpy.percentile(np_array, PERCENTILE)
        with self._locks["_percentile_map"]:
            self._percentile_map[api.name] = p
        logger.debug("%s percentile result: %s" % (PERCENTILE, p))
        if response_time > p:
            logger.debug("Decreasing priority for %s" % api.name)
            with self._locks["_api_priority"]:
                if self._api_priority[api.name] > 0:
                    self._api_priority[api.name] -= 1
        else:
            logger.debug("Increasing priority: %s"
                         % self._api_priority[api.name])
            if self._api_priority[api.name] < 1000:
                self._api_priority[api.name] += 1
        logger.debug("New priority: %s" % self._api_priority[api.name])

    def _periodic_priority_check(self):
        while True:
            logger.debug("Starting periodic priority check")
            with self._locks["add_remove_api"]:
                logger.debug("API list: %s" % self._api_list.keys())
                for key in self._api_list:
                    api = self._api_list.get(key)
                    logger.debug("Performing priority check on API: %s"
                                 % api.name)
                    try:
                        response_time = api.check()
                        self._process_response_time_api(response_time, api)
                    except NotImplementedError:
                        # Ignore NotImplementedError, the user has decided
                        # to stop the periodic priority check for the
                        # response time of this API
                        logger.debug("API %s has no priority check method "
                                     "implemented." % api.name)
                        pass
            logger.debug("End of periodic priority check")
            time.sleep(PERIODIC_CHECK)
