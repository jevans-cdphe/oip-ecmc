import atexit
import datetime as dt
import json
import logging.config
import logging.handlers
import pathlib
import queue
from typing import Optional, Union

import yaml

from . import const


def get_logger(
    logger_name: str,
    log_level: str,
    log_path: pathlib.Path,
) -> logging.Logger:
    log_path.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(logger_name)
    config_file = pathlib.Path(__file__).absolute().parent / 'log_config.yaml'

    with config_file.open() as f_in:
        config = yaml.load(f_in, Loader=yaml.Loader)

    config['handlers']['file_json']['filename'] = log_path / f'{logger_name}.jsonl'
    logging.config.dictConfig(config)
    logging.basicConfig(level=log_level)
    
    return logger


def _resolve_handlers(l):
    if not isinstance(l, logging.config.ConvertingList):
        return l

    # Indexing the list performs the evaluation.
    return [l[i] for i in range(len(l))]


def _resolve_queue(q):
    if not isinstance(q, logging.config.ConvertingDict):
        return q
    if '__resolved_value__' in q:
        return q['__resolved_value__']

    cname = q.pop('class')
    klass = q.configurator.resolve(cname)
    props = q.pop('.', None)
    kwargs = {k: q[k] for k in q if logging.config.valid_ident(k)}
    result = klass(**kwargs)
    if props:
        for name, value in props.items():
            setattr(result, name, value)

    q['__resolved_value__'] = result
    return result


class QueueListenerHandler(logging.handlers.QueueHandler):

    def __init__(
        self,
        handlers,
        respect_handler_level=False,
        auto_run=True,
        queue=queue.Queue(-1),
    ):
        queue = _resolve_queue(queue)
        super().__init__(queue)
        handlers = _resolve_handlers(handlers)
        self._listener = logging.handlers.QueueListener(
            self.queue,
            *handlers,
            respect_handler_level=respect_handler_level)
        if auto_run:
            self.start()
            atexit.register(self.stop)

    def start(self):
        self._listener.start()

    def stop(self):
        self._listener.stop()

    def emit(self, record):
        return super().emit(record)


class MyJSONFormatter(logging.Formatter):
    def __init__(
        self,
        *,
        fmt_keys: Optional[dict[str, str]] = None,
    ):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in const.LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


class NonErrorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> Union[bool, logging.LogRecord]:
        return record.levelno <= logging.INFO