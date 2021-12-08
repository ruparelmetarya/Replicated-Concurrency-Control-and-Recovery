"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""
import logging

from constants import FILE_LOG_FORMAT


class Logger:
    @staticmethod
    def get_logger(name):
        file_formatter = logging.Formatter(FILE_LOG_FORMAT)

        file_handler = logging.FileHandler("log.txt")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)

        logger = logging.getLogger(name)
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)

        return logger
