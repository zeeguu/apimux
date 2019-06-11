from concurrent.futures import ThreadPoolExecutor
import numpy
import operator
import random
import sys
from threading import Thread, Lock
import time
from timeit import default_timer as timer
import traceback

from apimux.log import logger
from apimux.rwlock import ReadWriteLock
from apimux import config


class APIMultiplexer(object):
    def __init__(self, api_list=[], config_filepath='config.ini'):
        """
        Parameters
        ----------
        api_list : list
            List of objects that are implementing the default class
            BaseThirdPartyAPIService.

        """
        apimux_cfg = config.parse_config(config_filepath)
        self.PERCENTILE = apimux_cfg.getint("PERCENTILE")
        self.MAX_HISTORY_RTIME = apimux_cfg.getint("MAX_HISTORY_RTIME")
        self.MAX_WAIT_TIME = apimux_cfg.getint("MAX_WAIT_TIME")
        self._PERIODIC_CHECK_INTERVAL = apimux_cfg.getint("PERIODIC_CHECK")

        logger.debug("Initializing the APIMultiplexer class")
        # Locks used to prevent multi-threading issues
        self._locks = {}
        # ReadWriteLock allows multiple readers and only one writer
        self._locks["_api_list"] = ReadWriteLock()
        self._api_list = {}
        self._locks["_api_response_times"] = Lock()
        self._api_response_times = {}
        self._locks["_percentile_map"] = Lock()
        self._percentile_map = {}
        self._futures_not_finished = {}
        self._locks["_futures_not_finished"] = Lock()

        # Registering all APIs passed as parameters
        if len(api_list) > 0:
            for x in api_list:
                self.register_new_api(x)

        self._ignore_slow_apis = apimux_cfg.getboolean("ignore_slow_apis")
        self._slow_multiplied = apimux_cfg.getfloat("slow_multiplied")
        self._exploration_coefficient = apimux_cfg.getint(
            "exploration_coefficient")

        # Whether it should enable round robing or not
        self._round_robin = apimux_cfg.getboolean("round_robin")
        if self._round_robin:
            logger.info("Round robin enabled!")
            # Disable exploration if round robin is enabled
            self._exploration_coefficient = 0
        elif self._exploration_coefficient > 0:
            logger.info("Exploration with percentage %s enabled!"
                        % self._exploration_coefficient)
        self._current_order = []
        self._locks["_current_order"] = Lock()

        if apimux_cfg.getboolean("enable_periodic_check"):
            # Starting a background thread which will run periodically
            # the 'check' method if implemented by the user for an API
            self._periodic_check_thread = Thread(
                target=self._periodic_check, args=())
            self._periodic_check_thread.setDaemon(True)
            self._periodic_check_thread.start()

    @property
    def _round_robin_list(self):
        if self._current_order:
            return self._current_order
        with self._locks["_current_order"]:
            with self._locks["_api_list"]:
                self._current_order = [x for x in self._api_list]
        return self._current_order

    def _time_function(self, f, data):
        """
        Helper to measure the response time of function f.

        Parameters
        ----------
        f : function
            The function for which the response time will be measured.
        data : object
            The object which will be passed to the call to function f.

        Returns
        -------
        tuple
            Returns a tuple containing the result of the function f
            and the time it took to retrieve the result.

        """
        start = timer()
        result = f(data)
        end = timer()
        elapsed_ms = (end - start) * 1000
        return (result, elapsed_ms)

    def _get_result(self, api, data):
        """
        Gets the result from an API.

        Notes
        -----
            Returns None if the api raises an exception.

        Parameters
        ----------
        api : BaseThirdPartyAPIService
            The API object which implements the class BaseThirdPartyAPIService.
        data : object
            The object which will be sent to the method get_result of the
            api parameter.

        Returns
        -------
        object
            The result of calling get_result of the api object.

        """
        try:
            result, response_time = self._time_function(api.get_result, data)
            self._process_response_time_api(response_time, api)
            return result
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.warning("API raised the exception: %s"
                           % traceback.format_exception(exc_type,
                                                        exc_value,
                                                        exc_traceback))
        return None

    def _shift_current_order(self):
        """
        Updates the current order of API list used for round robin.

        Returns
        -------
        BaseThirdPartyAPIService
            Returns the first API after the list is shifted to the left with
            one position.

        """
        logger.debug("Current round robin order: %s" % self._round_robin_list)
        first_apiname = self._round_robin_list.pop(0)
        self._round_robin_list.append(first_apiname)
        logger.debug("New round robin order: %s" % self._round_robin_list)

        with self._locks["_api_list"]:
            first_api = self._api_list.get(first_apiname)

        return first_api

    def _should_explore(self):
        """
        Helper function to decide if the request should use round-robin.

        Round-robin is used to explore other APIs instead of picking the
        fastest API all the time. It is based on self._exploration_coefficient.

        Notes
        -----
            Can be disabled if the value of exploration coefficient is set
            to -1 in the config file.

        Returns
        -------
        boolean
            Returns true with self._exploration_coefficient chance, otherwise
            false.

        """
        chance = random.randint(1, 100)
        if chance <= self._exploration_coefficient:
            return True
        return False

    def _get_fastest_api(self, sp_list=[]):
        """
        Returns the fastest API so far.

        The result is based on the sp_list. If sp_list is empty list, it
        will fetch the current order of APIs based on response time. This
        will be used as a context for the subsequent calls to this function.

        Notes
        -----
            Round robin mode doesn't require sp_list parameter.

        Parameters
        ----------
        sp_list : list
            Previous list of API names sorted based on the percentile result.
            It should be empty list if it's the first call.
            It contains the sorted list with the response time of each API
            with respect to the value set in the config file for PERCENTILE.

        Returns
        -------
        tuple
            Contains the API object and the list which needs to be passed to
            the subsequent calls to this function.

        """
        if self._round_robin:
            return self._shift_current_order(), None

        if self._should_explore() and sp_list:
            logger.debug("This request will try to explore with round-robin")
            new_api = self._shift_current_order()
            logger.debug("Picking API %s" % new_api.name)
            # Remove the API that was picked from the list of remainings APIs
            new_sp_list = [x for x in sp_list if x[0] != new_api.name]
            return self._shift_current_order(), new_sp_list

        if sp_list:
            with self._locks["_api_list"]:
                return self._api_list.get(sp_list.pop(0)[0]), sp_list

        with self._locks["_percentile_map"]:
            sp_list = sorted(self._percentile_map.items(),
                             key=operator.itemgetter(1))

        logger.debug("Sorted by response time median: %s" % sp_list)
        with self._locks["_api_list"]:
            fastest_api = self._api_list.get(sp_list.pop(0)[0])
        return fastest_api, sp_list

    def _prepare_get_next_results(self, number_of_results, exclude_services):
        max_number_of_results = len(self._api_list) - len(exclude_services)
        if number_of_results == -1:
            requested_results = max_number_of_results
        else:
            requested_results = min(number_of_results, max_number_of_results)
        allowed_failed_futures = max_number_of_results - requested_results

        executor = ThreadPoolExecutor(max_workers=requested_results)

        return allowed_failed_futures, requested_results, executor

    def get_next_results(self, data, number_of_results,
                         exclude_services=[], exclude_results=[]):
        """
        Retrieve the next N results from the registered APIs.

        This function retrieves the next "number_of_results" using the
        list which contains the fastest APIs by average response time.

        Notes
        -----
            If self.MAX_WAIT_TIME is greater than 0, this method will try
            to return within the specified time as long as there is at least
            one result to return. If the running time passes the specified
            time it will still wait for at least one result from the APIs.

            If self.MAX_WAIT_TIME is 0, this method will wait as long as it's
            needed to fetch the required number of results.

        Parameters
        ----------
        data : dict
            Contains specific implementation of the objects that implement
            BaseThirdPartyAPIService class.
        number_of_results : int
            Number of results that will be fetched. Pass -1 if you wish to
            retrieve the results from all registered APIs. If the number
            passed is greater than the number of registered APIs, it will
            work in the same way as passing -1.
        exclude_services : list of strings
            List of strings containing the service names which should be
            excluded for processing the request. It will be used to filter
            the APIs from the list of fastest APIs.
        exclude_results : list
            List of results used to filter out the returned results. It is
            particulary useful on the subsequent calls to get_next_results
            when you want to exclude the results received from previous
            requests.

        Returns
        -------
        list of tuples
            Returns a list of tuples containing the API name and the result
            fetched from that API using the method get_result from
            BaseThirdPartyAPIService. The type of the result it's specific
            to the implementation of the developer for the function get_result.

        """
        results = []
        sp_list = []
        future_to_api = {}
        failed_futures = 0
        failed_futures_lock = Lock()

        allowed_failed_futures, requested_results, executor = (
            self._prepare_get_next_results(number_of_results,
                                           exclude_services))

        def register_result(future):
            # Appends the result from the future to the final list that will
            # be returned

            # The future is ignored if it was cancelled.
            if not future.cancelled():
                nonlocal failed_futures
                nonlocal results

                future_exception = future.exception()
                if future_exception:
                    with failed_futures_lock:
                        failed_futures += 1
                    logger.warning("API %s raised exception %s" % (
                        future_to_api[future]['name'], future_exception))
                elif future.result() is not None:
                    results.append((future_to_api[future]['name'],
                                    future.result()))
                else:
                    # The API returned an invalid result, mark the future
                    # as failed and continue fetching from the next one.
                    with failed_futures_lock:
                        failed_futures += 1
            # Remove the future from the map
            future_to_api.pop(future, None)

        def launch_future(api, data, executor):
            future = executor.submit(self._get_result, api, data)
            future_to_api[future] = {"name": api.name,
                                     "start_time_ms": timer()}
            future.add_done_callback(register_result)

        def replace_failed_future(sp_list, data, exclude_services, executor,
                                  elapsed_ms=None):
            """
            Helper that replaces failed futures with new ones.

            This function is used when any of the current API requests fail
            and the number of results requested cannot be met. It launches
            a new future requesting a result from another API to make sure
            it meets the number of results desired.

            Notes
            -----
                The parameter elapsed_ms is required only if
                self.MAX_WAIT_TIME > 0.

            Parameters
            ----------
            sp_list : list of APIs
                The list returned by self._get_fastest_api
            data : object
                The object which will be sent to the method get_result of the
                api parameter.
            exclude_services : list of strings
                The API names which should be excluded from the result.
            executor : ThreadPoolExecutor
                The ThreadPoolExecutor object that will process the future.
            elapsed_ms : int
                How much time has elapsed since it started sending requests.

            Returns
            -------
            object
                The result of calling get_result of the api object.

            """
            nonlocal failed_futures
            nonlocal allowed_failed_futures
            with failed_futures_lock:
                if (allowed_failed_futures > 0 and
                        failed_futures > 0 and sp_list):
                    api, sp_list = self._get_fastest_api(sp_list=sp_list)
                    if api.name in exclude_services:
                        return
                    max_timeout = self._get_max_api_timeout(api.name)
                    if elapsed_ms:
                        if not (elapsed_ms + max_timeout) < self.MAX_WAIT_TIME:
                            # Too late to launch new futures
                            allowed_failed_futures = 0
                    launch_future(api, data, executor)
                    failed_futures -= 1
                    allowed_failed_futures -= 1

        def cancel_slow_apis():
            # Cancels the requests currently in progress if the elapsed time
            # so far is greater than the average response time of
            # self.PERCENTILE percentage of requests plus delta
            # self._slow_multiplied.
            nonlocal failed_futures
            for future in future_to_api.keys():
                api_details = future_to_api[future]
                elapsed_ms = api_details['start_time_ms'] - timer()
                if elapsed_ms > self._get_max_api_timeout(api_details['name']):
                    with failed_futures_lock:
                        failed_futures += 1
                    future.cancel()

        try:
            current_requests_sent = 0
            while True:
                api, sp_list = self._get_fastest_api(sp_list=sp_list)
                if api.name in exclude_services:
                    continue
                logger.debug("Launching future: %s" % api)
                launch_future(api, data, executor)
                current_requests_sent += 1
                if current_requests_sent == requested_results:
                    break

            if self.MAX_WAIT_TIME > 0:
                start_time = timer()
                while len(results) < requested_results:
                    elapsed_ms = (timer() - start_time) * 1000
                    if (elapsed_ms > self.MAX_WAIT_TIME and
                            len(results) > 0) or allowed_failed_futures == 0:
                        break
                    # Launch a new future if we have any failed futures.
                    replace_failed_future(
                        sp_list, data, exclude_services, executor, elapsed_ms)
                    # Cancel slow APIs
                    cancel_slow_apis()
                    time.sleep(0.01)

                # Maximum wait time has passed here, cancel all futures and
                # return as soon as possible
                for future in future_to_api.keys():
                    future.cancel()
            else:
                while len(results) < requested_results:
                    # Launch a new future if we have any failed futures.
                    replace_failed_future(
                        sp_list, data, exclude_services, executor)
                    if len(sp_list) == 0:
                        break
                    time.sleep(0.01)
        finally:
            # When self.MAX_WAIT_TIME > 0 all futures will be already done
            # executing or cancelled here which allows the executor to free
            # the resources immediately.
            # When self.MAX_WAIT_TIME == 0 the executor will wait as long as
            # it's required for the futures to respond.
            executor.shutdown(wait=True)

        return results

    def _get_max_api_timeout(self, apiname):
        """
        Returns the maximum expected response time for an API.

        The maximum expected response time for an API is computed using
        the percentile defined by the user multiplied with some configurable
        delta.

        Parameters
        ----------
        apiname : string
            The name of the API.

        Returns
        -------
        int
            The maximum expected response time including the additional delta.

        """
        with self._locks["_percentile_map"]:
            timeout_result = self._percentile_map.get(apiname, None)
        return timeout_result * self._slow_multiplied

    def register_new_api(self, api):
        """
        Registers new API and adds it to the internal list.

        Parameters
        ----------
        api : object
            The object implementing BaseThirdPartyAPIService class.

        """
        logger.info("New API to register: %s" % api.name)
        self._locks["_api_list"].acquire_write()
        if self._api_list.get(api.name, None) is not None:
            raise Exception("API already exists in the list")
        self._api_list[api.name] = api
        self._locks["_api_list"].release_write()
        # All APIs start from 0 initially, this will be automatically
        # reconfigured based on the performance of the APIs.
        with self._locks["_api_response_times"]:
            self._api_response_times[api.name] = []
        with self._locks["_percentile_map"]:
            self._percentile_map[api.name] = 0
        logger.info("New list: %s" % self._api_list.keys())

    def remove_api(self, api):
        """
        Removes the API from the internal list.

        Parameters
        ----------
        api : object
            The object implementing BaseThirdPartyAPIService class.

        """
        logger.info("Removing API: %s" % api.name)
        self._locks["_api_list"].acquire_write()
        removed_api = self._api_list.pop(api.name, None)
        self._locks["_api_list"].release_write()
        if removed_api is not None:
            logger.debug("Removed API")
        else:
            logger.debug("Tried to remove API which is "
                         "not present in the list")
            return
        with self._locks["_api_response_times"]:
            self._api_response_times.pop(api.name, None)
        with self._locks["_percentile_map"]:
            self._percentile_map.pop(api.name, None)
        logger.info("New list: %s" % self._api_list.keys())

    def _process_response_time_api(self, response_time, api):
        """
        Analyses the response time of an API.

        This function is always called on the response time of an API in
        order to update the internal state with the average response time.

        Parameters
        ----------
        response_time : float
            Elapsed time in milliseconds.
        api : object
            The API which had the response time passed above.

        """
        logger.info("%s: response time %sms" % (api.name, response_time))
        with self._locks["_api_response_times"]:
            if (len(self._api_response_times[api.name]) >
                    self.MAX_HISTORY_RTIME):
                # Remove from the history once it reaches max limit
                self._api_response_times[api.name].pop(0)
            self._api_response_times[api.name] += [response_time]
            # Sorted returns a new cloned list
            np_array = numpy.array(sorted(self._api_response_times[api.name]))

        # Compute the response time of self.PERCENTILE percentage of requests
        p = numpy.percentile(np_array, self.PERCENTILE)
        with self._locks["_percentile_map"]:
            self._percentile_map[api.name] = p
        logger.debug("%s - %s percentile result: %s"
                     % (api.name, self.PERCENTILE, p))

    def _periodic_check(self):
        """
        Periodic check for the health and performance of the APIs.

        This runs in a background thread to check the health and performance
        of the third party APIs. It is useful in situations where there is
        not enough traffic all the time required to have an up to date list
        of response times.

        """
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
                        # to not implement the periodic check for the
                        # response time of this API
                        logger.debug("API %s has no priority check method "
                                     "implemented." % api.name)
            logger.debug("End of periodic priority check")
            time.sleep(self._PERIODIC_CHECK_INTERVAL)
