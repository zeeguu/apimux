from abc import ABC, abstractmethod

from apimux.log import logger


class BaseThirdPartyAPIService(ABC):
    def __init__(self, name=None):
        super(BaseThirdPartyAPIService, self).__init__()
        self.name = name if name is not None else self.__class__.__name__
        logger.debug("Class initialized %s" % self.name)

    @abstractmethod
    def get_result(self, data):
        """Returns the result from the API. The user is expected to
        fill in all the details about this."""
        pass

    def check(self):
        """Simple method used for periodic healthcheck of the API response time.
        Return false if not implemented."""
        raise NotImplementedError()
