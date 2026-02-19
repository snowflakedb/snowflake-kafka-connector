class Error(Exception):
    """Base class for other exceptions"""
    pass


class ResetAndRetry(Error):
    """Raised when we want to reset the retry count"""

    def __init__(self, msg=""):
        self.msg = msg


class RetryableError(Error):
    """Raised when we can retry"""

    def __init__(self, msg=""):
        self.msg = msg


class NonRetryableError(Error):
    """Raised when we cannot retry"""

    def __init__(self, msg=""):
        self.msg = msg
