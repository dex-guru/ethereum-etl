import logging
from logging import config

from ethereumetl.config.envs import envs


class AddAttrsFilter(logging.Filter):
    """
    A logging filter that adds extra attributes to log records.
    """

    def __init__(self, attrs: dict):
        super().__init__()
        self.attrs = attrs

    def filter(self, record):
        for key, value in self.attrs.items():
            setattr(record, key, value)
        return True


def logging_basic_config(filename=None):
    format = "%(asctime)s - %(name)s [%(levelname)s] - %(message)s"
    if filename is not None:
        logging.basicConfig(level=logging.INFO, format=format, filename=filename)
    elif 'logstash' in envs.LOG_HANDLERS:
        cfg = dict(
            disable_existing_loggers=False,
            version=1,
            formatters={
                "simple": {
                    "format": (
                        "%(asctime)s"
                        " - %(filename)s:%(lineno)s:%(funcName)s"
                        " - %(levelname)s"
                        " - %(message)s"
                    )
                },
                "logstash": {"()": "logstash_formatter.LogstashFormatterV1"},
            },
            handlers={
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "simple",
                    "stream": "ext://sys.stdout",
                },
                "logstash": {
                    "level": envs.LOGSTASH_LOGGING_LEVEL,
                    "class": "logstash_async.handler.AsynchronousLogstashHandler",
                    "transport": "logstash_async.transport.TcpTransport",
                    "formatter": "logstash",
                    "host": envs.LOGSTASH_HOST,
                    "port": envs.LOGSTASH_PORT,
                    "database_path": None,
                    "event_ttl": 30,  # sec
                    "filters": ["add_attrs"],
                },
            },
            filters={
                "add_attrs": {
                    "()": AddAttrsFilter,
                    "attrs": {
                        "chain_id": envs.CHAIN_ID,
                        "service_name": envs.SERVICE_NAME,
                    },
                },
            },
            loggers={
                "logstash": {
                    "handlers": ["logstash"],
                    "level": envs.LOGSTASH_LOGGING_LEVEL,
                    "propagate": False,
                },
            },
            root={
                "handlers": envs.LOG_HANDLERS,
                "level": envs.LOGGING_LEVEL,
            },
        )
        config.dictConfig(cfg)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

    logging.getLogger("ethereum_dasm.evmdasm").setLevel(logging.ERROR)
