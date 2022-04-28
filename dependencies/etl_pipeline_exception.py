"""
ETLPipelineException : Custom Exception class
"""


class ETLPipelineException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
