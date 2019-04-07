from concurrent.futures import ThreadPoolExecutor, TimeoutError, CancelledError
import numpy
import operator
from threading import Thread, Lock
import time
from timeit import default_timer as timer

from apimux.log import logger
from apimux.constants import PERIODIC_CHECK
from apimux.rwlock import ReadWriteLock


PERCENTILE = 90
MAX_HISTORY_RTIME = 100
# The response time for an API will be the minimum between MAX_WAIT_TIME and
# the value obtained by calculating the percentile of all response times so far
# MAX_WAIT_TIME = 1000  # ms
MAX_WAIT_TIME = 0


class APIMultiplexer(object):
    def __init__(self, api_list=[], enable_periodic_check=False,
                 ignore_slow_apis=False, a_b_testing=False):
        logger.debug("Initializing the APIMultiplexer class")
        self._locks = {}
        self._locks["_api_list"] = ReadWriteLock()
        self._api_list = {}
        self._locks["_api_priority"] = Lock()
        self._api_priority = {}
        self._locks["_api_response_times"] = Lock()
        self._api_response_times = {}
        self._locks["_percentile_map"] = Lock()
        self._percentile_map = {}

        self._futures_not_finished = {}
        self._locks["_futures_not_finished"] = Lock()

        if len(api_list) > 0:
            for x in api_list:
                # Registering all APIs passed as parameters
                self.register_new_api(x)

        # Queue new requests and wait for them even when the API is slow
        self._ignore_slow_apis = ignore_slow_apis
        # Whether it should enable A/B testing or not
        self._a_b_testing = a_b_testing
        if a_b_testing:
            logger.debug("A/B testing enabled!")
            self._current_order = []
            self._locks["_current_order"] = Lock()

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

    def _shift_current_order(self):
        """Shifts the current API order and returns the first API"""
        with self._locks["_current_order"]:
            if len(self._current_order) == 0:
                with self._locks["_api_list"]:
                    self._current_order = [x for x in self._api_list]
            logger.debug("Initial order: %s" % self._current_order)
        logger.debug("Current a/b order: %s" % self._current_order)
        apiname = self._current_order.pop(0)
        self._current_order.append(apiname)
        logger.debug("New a/b order: %s" % self._current_order)
        with self._locks["_api_list"]:
            first_api = self._api_list.get(apiname)
        return first_api

    def get_top_result(self, data):
        """Gets the first result from the best API."""
        chosen_api = None
        current_retries = 0
        result = None
        with ThreadPoolExecutor(max_workers=1) as executor:
            if self._a_b_testing:
                chosen_api = self._shift_current_order()
            else:
                with self._locks["_percentile_map"]:
                    sorted_percentile = sorted(self._percentile_map.items(),
                                               key=operator.itemgetter(1))
                logger.debug(
                    "Sorted by response time median: %s" % sorted_percentile)
                with self._locks["_api_list"]:
                    chosen_api = self._api_list.get(sorted_percentile.pop()[0])

            logger.info("Chosen API for request: %s" % chosen_api.name)
            future = executor.submit(self._get_result, chosen_api, data)
            # Call will block until the result is fetched
            result = future.result()
            while result is None:
                # Continue fetching from next API
                if self._a_b_testing:
                    if current_retries > len(self._current_order):
                        break
                    current_retries += 1
                    chosen_api = self._shift_current_order()
                elif len(sorted_percentile) > 0:
                    with self._locks["_api_list"]:
                        chosen_api = self._api_list.get(
                            sorted_percentile.pop()[0])
                logger.info(
                    "Reconfiguring API for request: %s" % chosen_api.name)
                future = executor.submit(self._get_result, chosen_api, data)
                result = future.result()

        return [(chosen_api.name, result)]

    def get_next_result(self, data):
        """Gets the next result from the next best API."""
        return "fake next"

    def _remove_pending_api_request(self, future):
        logger.warning("API responded with: %s" % future.result())
        with self._locks["_futures_not_finished"]:
            fut = self._futures_not_finished.pop(future, None)
        if fut is None:
            logger.warning("Tried to remove inexistent pending request")

    def _get_all_results(self, data, executor):
        returned_data = []
        with self._locks["_api_list"]:
            future_to_api = {}
            for apiname in self._api_list:
                if self._ignore_slow_apis:
                    with self._locks["_futures_not_finished"]:
                        if apiname in self._futures_not_finished.values():
                            continue
                api = self._api_list.get(apiname)
                future_to_api[executor.submit(
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
                        if not second_loop and MAX_WAIT_TIME > 0:
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
        if self._a_b_testing:
            a_b_returned_data = []
            with self._locks["_current_order"]:
                if len(self._current_order) == 0:
                    self._current_order = [x[0] for x in returned_data]
                    logger.debug("Initial order: %s" % self._current_order)
            for x in self._current_order:
                a_b_returned_data += [
                    y for y in returned_data if y[0] == x]
            # Shift the current order to the left
            logger.debug("Current a/b order: %s" % self._current_order)
            self._current_order.append(self._current_order.pop(0))
            logger.debug("New a/b order: %s" % self._current_order)
            logger.debug("Returning data: %s" % a_b_returned_data)
            return a_b_returned_data
        logger.debug("Returning data: %s" % returned_data)
        return returned_data

    def get_all_results(self, data):
        """Gets the result from all APIs."""
        returned_data = []
        executor = ThreadPoolExecutor(max_workers=len(self._api_list))
        try:
            returned_data = self._get_all_results(data, executor)
            return returned_data
        finally:
            # Ask the executor to shutdown, the resources
            # associated with the executor will be freed when all
            # pending futures are done executing.
            logger.debug("Sending signal to executor to shutdown once all"
                         "futures are done executing: %s" % executor)
            executor.shutdown(wait=False)

    def register_new_api(self, api):
        """Registers new API and adds it to the list for monitoring."""
        logger.info("New API to register: %s" % api.name)
        # with self._locks["_api_list"]:
        self._locks["_api_list"].acquire_write()
        if self._api_list.get(api.name, None) is not None:
            raise Exception("API already exists in the list")
        self._api_list[api.name] = api
        self._locks["_api_list"].release_write()
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
        # with self._locks["_api_list"]:
        self._locks["_api_list"].acquire_write()
        removed_api = self._api_list.pop(api.name, None)
        self._locks["_api_list"].release_write()
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
            with self._locks["_api_list"]:
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
