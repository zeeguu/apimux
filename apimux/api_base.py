from abc import ABC, abstractmethod

from apimux.log import logger


class BaseThirdPartyAPIService(ABC):
    def __init__(self, name=None):
        super(BaseThirdPartyAPIService, self).__init__()
        self.name = name if name is not None else self.__class__.__name__
        logger.debug("Class initialized %s" % self.name)

    @abstractmethod
    def get_result(self, data):
        """
        Returns the result from the API.

        The user is expected to fill in all the details about this method
        with specific details about the API.

        Notes
        -----
            If the answer is considered to be invalid, this method should
            return None as a result.

        Parameters
        ----------
        data : dict
            Contains specific implementation of the objects that implement
            BaseThirdPartyAPIService class.

        Returns
        -------
        user defined type
            Returns anything the user expects to receive.

        """
        pass

    def check(self):
        """
        Runs a simple check to verify the response time of the API.

        This method should send a simple request to the API to test the
        current response time of the API.

        Notes
        -----
            This method should return False if the API is not expected to
            have a check method.

        Returns
        -------
        int
            The response time for the request.

        """
        raise NotImplementedError()
