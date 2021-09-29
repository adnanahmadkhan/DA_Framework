# An exception designed to be raised AFTER a breaker has been tripped,
# informing the caller that the reason execution aborted was a breaker
# being tripped.  
#
class BreakerTrippedException(Exception):

    def __init__(self, reason=None):
        self.reason = reason

# An exception designed to be raised DURING a breaker trip, effectively
# aborting all routines.  The difference between this an the
# BreakerTrippedException is this exception is designed to be used DURING
# the process of aborting, where as the "Tripped" exception is designed
# to be used as a final "wrap-up" summary exception to exit.
#
class BreakerTrippingException(Exception):
    pass

