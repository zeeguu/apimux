import configparser

from apimux.log import logger


def parse_config(config_path):
    """
    Parses the config present at config_path.

    Parameters
    ----------
    config_path : string
        The path pointing to the location where the config file is stored.

    Returns
    -------
    configparser.SectionProxy
        Returns the DEFAULT section from the config file.

    """
    cfg = configparser.ConfigParser()
    if not cfg.read(config_path):
        # cfg.read returns empty list of the config file does not exist
        logger.warning("Could not find the config file at %s" % config_path)
        cfg['DEFAULT'] = {'round_robin': 'false',
                          'ignore_slow_apis': 'false',
                          'enable_periodic_check': 'false',
                          'slow_multiplied': '0.5',
                          'PERCENTILE': '90',
                          'MAX_HISTORY_RTIME': '5000',
                          'MAX_WAIT_TIME': '0',
                          'exploration_coefficient': '0'}
    return cfg['DEFAULT']
