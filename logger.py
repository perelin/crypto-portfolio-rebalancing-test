import sys
from logbook import Logger, StreamHandler, ERROR, WARNING, CRITICAL, INFO
StreamHandler(sys.stdout).push_application()
log = Logger(__name__, level=INFO)