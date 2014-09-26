# Copyright 2012 by Jeff Laughlin Consulting LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys
import json
import multiprocessing
from time import sleep
from hodor.exceptions import *
from apiclient.errors import HttpError
from socket import error as socket_error
from hodor.cli import Context

def gme_exc_handler(tries_remaining, exception, delay, args):
    """GME exception handler; retries 403 errors (access token expired) and prints a warning to stderr.

    tries_remaining: The number of tries remaining.
    exception: The exception instance which was raised.
    delay: The length of time we're sleeping for.
    args: A tuple of the arguments passed to the calling function.
    """

    # By convention Hodor always passes Context as the
    # first argument to anything that utilises retries.
    ctx = args[0]

    # Refresh expired access tokens
    if isinstance(exception, HttpError):
      if exception.resp.status == 401:
        print "Token Expired, Reauthenticating..."

        if not isinstance(ctx, Context):
          raise Exception("Error: Could not find Context object. Calling method must pass Context as the first argument.")

        ctx.refresh_services()

      # Files uploads return a 204 No Content "error" that needs to be handled farther up.
      elif exception.resp.status == 204:
        raise NoContent()

      # Allow these fatal errors to bubble up - there's nowt we can do about them here.
      elif exception.resp.status in [403, 500, 503, 410]:
        content = json.loads(exception.content)
        if content['error']['errors'][0]['reason'] == 'queryTooExpensive':
          raise QueryTooExpensive("Query too expensive '%s'" % (content['error']['message']))
        elif content['error']['errors'][0]['reason'] == 'backendError':
          # raise BackendError("GME backend error '%s'" % (content['error']['message']))

          # content['error']['message'] == "Service is unavailable. Retry."
          # HTTP 503 or 410

          # content['error']['message'] == "Deadline exceeded."
          # HTTP 410

          print content
          if content['error']['message'] == "Deadline exceeded.":
            raise BackendError("GME backend error '%s'" % (content['error']['message']))
          else:
            pass
        elif content['error']['errors'][0]['reason'] == 'tableTooLarge':
          raise TableTooLarge("Table too large '%s'" % (content['error']['message']))
        elif content['error']['errors'][0]['reason'] == 'internalError':
          raise InternalServerError("%s" % (content['error']['message']))
        elif content['error']['errors'][0]['reason'] == 'rateLimitExceeded':
          pass # Retry 403 Rate Limit Exceeded (for uploads only)

      # Retry non-fatal errors like "server didn't respond in time", GME's random "internal server error", or rate limit exceeded errors
      elif exception.resp.status not in [410, 429, 500]:
        raise exception

    if isinstance(ctx, Context):
      ctx.log("%s, %d tries remaining, sleeping for %s seconds" % (exception, tries_remaining, delay))
    else:
      if getattr(exception, 'content', None) is not None:
        content = json.loads(exception.content)
        message = content['error']['message'] + " " + content['error']['errors'][0]['reason']
        print >> sys.stderr, "Caught '%s' (%s), %d tries remaining, sleeping for %s seconds" % (message, exception.resp.status, tries_remaining, round(delay, 2))
      else:
        raise exception


def example_exc_handler(tries_remaining, exception, delay):
    """Example exception handler; prints a warning to stderr.

    tries_remaining: The number of tries remaining.
    exception: The exception instance which was raised.
    """
    print >> sys.stderr, "Caught '%s', %d tries remaining, sleeping for %s seconds" % (exception, tries_remaining, delay)


def retries(max_tries, delay=1, backoff=1.1, exceptions=(Exception, HttpError, socket_error), hook=gme_exc_handler):
    """Function decorator implementing retrying logic.

    delay: Sleep this many seconds * backoff * try number after failure
    backoff: Multiply delay by this factor after each failure
    exceptions: A tuple of exception classes; default (Exception,)
    hook: A function with the signature myhook(tries_remaining, exception);
          default None

    The decorator will call the function up to max_tries times if it raises
    an exception.

    By default it catches instances of the Exception class and subclasses.
    This will recover after all but the most fatal errors. You may specify a
    custom tuple of exception classes with the 'exceptions' argument; the
    function will only be retried if it raises one of the specified
    exceptions.

    Additionally you may specify a hook function which will be called prior
    to retrying with the number of remaining tries and the exception instance;
    see given example. This is primarily intended to give the opportunity to
    log the failure. Hook is not called after failure if no retries remain.
    """
    def dec(func):
        def f2(*args, **kwargs):
            mydelay = delay
            tries = range(max_tries)
            tries.reverse()
            for tries_remaining in tries:
                try:
                   return func(*args, **kwargs)
                except exceptions as e:
                    if tries_remaining > 0:
                        if hook is not None:
                            hook(tries_remaining, e, mydelay, args)
                        sleep(mydelay)
                        mydelay = mydelay * backoff
                    else:
                        raise e
                else:
                    break
        return f2
    return dec
