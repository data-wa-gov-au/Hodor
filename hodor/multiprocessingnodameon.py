import multiprocessing
import multiprocessing.pool
import time

"""
Sub-classing multiprocessing Process and Pool to let
us create non-daemonised procesess for creating child
threads in child threads.

Courtesy of http://stackoverflow.com/a/8963618
"""
class NoDaemonProcess(multiprocessing.Process):
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class NoDaemonPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess
