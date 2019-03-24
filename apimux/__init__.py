import sys

from apimux.log import logger

logger.debug("==== API Multiplexer imported ====")
major = sys.version_info.major
minor = sys.version_info.minor
micro = sys.version_info.micro
logger.debug("Running Python version %s.%s.%s" % (major, minor, micro))
