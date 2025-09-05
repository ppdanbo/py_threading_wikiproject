import logging
import logging.handlers
import os
import sys
from datetime import datetime

def _ensure_log_dir(path):
    os.makedirs(path, exist_ok=True)

def setup_logging(
    level=logging.DEBUG,
    log_dir_name="logs",
    log_file_name="app.log",
    log_format=None,
    datefmt="%Y-%m-%dT%H:%M:%S",
):
  
   # place logs in the project root directory (parent of utils)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    log_dir = os.path.join(project_root, log_dir_name)
    # base_dir = os.path.dirname(__file__)
    # log_dir = os.path.join(base_dir, log_dir_name)
    _ensure_log_dir(log_dir)
    log_path = os.path.join(log_dir, log_file_name)

    # create a timestamp for this run
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

    # if caller provided a pattern with {timestamp}, use it; otherwise append timestamp before extension
    if log_file_name and "{timestamp}" in log_file_name:
        processed_name = log_file_name.format(timestamp=timestamp)
    else:
        name, ext = os.path.splitext(log_file_name or "app.log")
        processed_name = f"{name}_{timestamp}{ext or '.log'}"
    log_path = os.path.join(log_dir, processed_name)

    # default format: ISO-like timestamp with milliseconds, module:lineno, thread, message
    if log_format is None:
        log_format = (
            "%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - "
            "%(filename)s:%(lineno)d - %(threadName)s - %(message)s"
        )

    formatter = logging.Formatter(log_format, datefmt=datefmt)

    root = logging.getLogger()
    root.setLevel(level)

    # avoid adding duplicate handlers if setup_logging is called multiple times
    if root.handlers:
        return

    # Rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Console/stderr handler
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Reduce verbosity of noisy third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("chardet").setLevel(logging.WARNING)

    # Capture uncaught exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # default handler for KeyboardInterrupt
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.getLogger(__name__).exception(
            "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
        )

    sys.excepthook = handle_exception
   