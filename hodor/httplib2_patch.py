# Source: https://gist.github.com/apg/1014614
# http://hg.python.org/cpython/file/2.7/Lib/httplib.py

# e.g.
# import httplib2
# h = httplib2.Http()
# start = time.time()
# response, content = h.request("http://bitworking.org/")
# end = time.time()
# pp(response)
# print "ttfb = %ss" % (response.get('x---stop-time') - start)
# print "ttlb = %ss" % (end - start)
# print "dlt = %ss" % ((end - start) - (response.get('x---stop-time') - start))

# postproc = request.postproc
# def _postproc(response, content):
#   print "_postproc"
#   print request_start_time
#   return postproc(response, content)
# request.postproc = _postproc

#### MONKEY PATCH for time to first byte.
# neither httplib, nor httplib2 provide a way to get time to first
# byte received. Luckily, Python is monkey patchable.

import time
import httplib
import apiclient.model

httplib__HTTPResponse__read_status = httplib.HTTPResponse._read_status
httplib__HTTPResponse_begin = httplib.HTTPResponse.begin
httplib__HTTPConnection__send_output = httplib.HTTPConnection._send_output

apiclient__BaseModel__response = apiclient.model.BaseModel.response

def perf__read_status(self):
    b = self.fp.read(1)
    # this is our first byte, mark it's time
    self._stop_time = time.time()
    # write back the byte we read to the internal buffer so that it can
    # be used for the status line.
    self.fp._rbuf.write(b)
    return httplib__HTTPResponse__read_status(self)

def perf_begin(self):
    resp = httplib__HTTPResponse_begin(self)
    self.msg.addheader("x---stop-time", self._stop_time)
    return resp

httplib.HTTPResponse._read_status = perf__read_status
httplib.HTTPResponse.begin = perf_begin

def perf__send_output(self, message_body=None):
    # httplib2 gives us access to the connection object
    # within the Http object (it stores it in a dict).
    # Because of this, we can get direct access to this
    # attribute.
    return httplib__HTTPConnection__send_output(self, message_body)

httplib.HTTPConnection._send_output = perf__send_output


#### MONKEY PATCH for Google Client APIs
# Patch around execute() in APIClient to return headers as well as content
def perf_response(self, resp, content):
  return (resp, apiclient__BaseModel__response(self, resp, content))

apiclient.model.BaseModel.response = perf_response
