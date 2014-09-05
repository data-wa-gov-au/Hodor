import functools
import traceback

class QueryTooExpensive(Exception):
  pass

class BackendError(Exception):
  pass

class QPSTooLow(Exception):
  pass

class TableTooLarge(Exception):
  pass

class InternalServerError(Exception):
  pass

class NoContent(Exception):
  pass

# For logging exceptions from multithreaded pools
# http://stackoverflow.com/a/25384934
def trace_unhandled_exceptions(func):
    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            print 'Exception in '+func.__name__
            traceback.print_exc()
    return wrapped_func
