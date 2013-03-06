# __init__.py
__version__ = '0.1dev'

# LOG_FMT = '%(asctime)s: %(name)s - %(levelname)s: %(message)s'
LOG_FMT = '%(asctime)s (%(name)s) [%(levelname)s]: %(message)s'

import sys
import logging


def setup_logging(logger_name):
	logger = logging.getLogger(logger_name)
	logger.setLevel(logging.DEBUG)

	formatter = logging.Formatter(fmt=LOG_FMT)

	err_handler = logging.StreamHandler(sys.__stderr__)
	err_handler.addFilter(StdErrFilter())
	err_handler.setFormatter(formatter)
	logger.addHandler(err_handler)

	out_handler = logging.StreamHandler(sys.__stdout__)
	out_handler.addFilter(StdOutFilter())
	out_handler.setFormatter(formatter)
	logger.addHandler(out_handler)

	return logger

class StdErrFilter(logging.Filter):
	def filter(self,record):
		return 1 if record.levelno >= 30 else 0

class StdOutFilter(logging.Filter):
	def filter(self,record):
		# To show DEBUG
		#return 1 if record.levelno <= 20 else 0
		return 1 if record.levelno <= 20 and record.levelno > 10 else 0 

