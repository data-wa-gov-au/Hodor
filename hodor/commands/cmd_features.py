import click
from pprintpp import pprint as pp
from shapely.geometry import box
from apiclient.errors import HttpError
import time
import json
import os
import multiprocessing
from retries import retries
from hodor.cli import pass_context

@click.group()
@pass_context
def cli(ctx):
  pass

# request.add_response_callback(cb)
def cb(resp):
  pp(resp)

def bbox2poly(bbox):
  """Transform a BBOX into a GME-style WKT Polygon.

  Parameters
  ----------
  bbox : list
    A bounding box in the traditional order of [minx, miny, maxx, maxy]
  """
  return box(*bbox).wkt

def bbox2quarters(bbox):
  """Split a BBOX into four equal quarters.

    Parameters
    ----------
    bbox : list
      A bounding box in the traditional order of [minx, miny, maxx, maxy]
  """
  delta_x = (bbox[2] - bbox[0]) / 2
  delta_y = (bbox[3] - bbox[1]) / 2

  return [
    [bbox[0],           bbox[1],           bbox[0] + delta_x, bbox[1] + delta_y], # SW
    [bbox[0] + delta_x, bbox[1],           bbox[2],           bbox[1] + delta_y], # SE
    [bbox[0] + delta_x, bbox[1] + delta_y, bbox[2],           bbox[3]],           # NE
    [bbox[0],           bbox[1] + delta_y, bbox[0] + delta_x, bbox[3]]            # NW
  ]

lock = None
def initialize_lock(l):
   global lock
  #  print "initialize_lock"
   lock = l

# Unless we're just fucking up the writing of the json? Try on a small area
# @TODO Store the features in memory? Will only work for non-huge requests...

@cli.command()

@click.argument('table-id', type=str)
@pass_context
def list(ctx, table_id):
  """Retrieve features from a vector table."""
  minrequiredqps = 10
  temp_bboxes = 'temp-bboxes.json'
  temp_features = 'temp-features.json'

  # with open(temp_features) as f:
  #   features = json.load(f)
  # print len(features)
  # exit()

  # table = ctx.service.tables().get(id=table_id, fields='bbox').execute()
  # pp(table)

  def getviablebboxes(bbox):
    @retries(5)
    def query(polygon):
      return ctx.service.tables().features().list(
                  id=table_id, maxResults=1,
                  select="gx_id",
                  intersects=polygon
      ).execute()

    bboxes = bbox2quarters(bbox) # Split to at least four separate bboxes

    valid_bboxes = []
    i = 999
    # for i, bbox in enumerate(bboxes):
    for bbox in bboxes:
    # i = 0
    # while bboxes:
      try:
        # bbox = bboxes.pop(0)
        # print bbox
        polygon = box(*bbox)
        response = query(polygon)
        print "#%s AQPS: %s, Features: %s (%s/%s)" % (i, response['allowedQueriesPerSecond'], len(response['features']), i, len(bboxes))

        if len(response['features']) == 0:
          print "## Discarding"
          # del bboxes[i]
        elif response['allowedQueriesPerSecond'] < minrequiredqps:
          raise Exception("QPS too low.")

        valid_bboxes.append(bbox)
        time.sleep(0.5)
      except (HttpError, Exception) as err:
        # print err
        msg = err.message
        if 'content' in err:
          msg = err.content
        print "## Splitting (%s)" % (msg)
        b2 = bbox2quarters(bbox)
        # print "## Adding"
        # pp(b2)
        bboxes.extend(b2)
        # del bboxes[i]
      # i += 1
    return valid_bboxes

  # Dummy bboxes
  # bbox = [112.92112388804973, -35.180887282568534, 129.0019274186415, -13.745958064667725] # Most of WA
  bbox = [113.423171,-35.276339,124.134841,-31.094568] # The South West and inland a bit
  # bbox = [114.331765,-35.098718,119.6876,-33.028397] # The South West
  # bbox = [114.779758,-34.526107,117.457675,-33.49019] # Margaret River - Augusta
  # bbox = [114.779758, -34.008148500000004, 116.1187165, -33.49019] # Margaret River

  if os.path.isfile(temp_bboxes):
    print "Retrieved bboxes from cache"
    with open(temp_bboxes) as f:
      bboxes = json.load(f)
  else:
    bboxes = getviablebboxes(bbox)

    # Cache bboxes to speed up subsequent runs
    with open(temp_bboxes, 'w') as f:
      json.dump(bboxes, f)

  # pp(bboxes)
  print len(bboxes)
  # exit()

  # Nuke any existing cached features
  if os.path.isfile(temp_features):
    os.remove(temp_features)

  chunks = [(bboxes[i:i+5], ctx, table_id, temp_features, i) for i in range(0, len(bboxes), 5)]

  start_time = time.time()

  lock = multiprocessing.Lock()
  pool = multiprocessing.Pool(processes=3, initializer=initialize_lock, initargs=(lock,))
  stuff = pool.map(getFeatures, chunks)
  pool.close()
  pool.join()

  elapsed_secs = time.time() - start_time
  ctx.log("Done in %ss" % (elapsed_secs))

def getFeatures(blob):
  global lock
  # print lock
  # exit()
  # print **kwargs

  @retries(10, delay=0.25, backoff=0.25)
  def list(request):
    return request.execute()

  bboxes, ctx, table_id, temp_features, start_index = blob

  # @TODO Does Python have a pool type thing that we can push used QPS to
  #         that would reset every second?
  # @TODO Give up on 'Deadline exceeded'
  # @TODO Track the number of pages and time elapsed for each resultset
  # @TODO Track page views consumed
  # @TODO Make reobtaining tokens work for multi threaded code
  # @TODO Confirm 'Deadline exceeded' errors by removing threading and asking for lots of features
  # @TODO Can we measure request wait time vs send time?
  # @TODO Work out why some valid bboxes are returning no features?

  pid = multiprocessing.current_process().pid
  if pid not in ctx.thread_safe_services:
    ctx.log("## Get New Service %s ##" % (pid))
    ctx.thread_safe_services[pid] = ctx.get_authenticated_service(ctx.RW_SCOPE)

  thread_start_time = time.time()
  for bbox in bboxes:
    print ""
    print bbox
    resource = ctx.thread_safe_services[pid].tables().features()

    request = resource.list(
                id=table_id, maxResults=1000,
                # select="gx_id",
                intersects=box(*bbox)
    )

    requests = 0
    while request != None:
      try:
        requests += 1
        start_time = time.time()

        # print dir(request)
        # print bbox
        response = list(request)
        # print len(response['features'])

        lock.acquire()
        with open(temp_features, 'a') as f:
          json.dump(response['features'], f)
        lock.release()

        # Obey GME's QPS limits
        response_time = time.time() - start_time
        nap_time = max(0, 1.3 - response_time)

        if len(response['features']) == 0:
          # print "Got no features"
          print "Got no features for [%s]" % (', '.join(str(v) for v in bbox))

        if nap_time > 0:
          ctx.log("pid %s retrieved %s features in %ss (%s requests). Napping for %ss." % (pid, len(response['features']), round(response_time, 2), requests, round(nap_time, 2)))
          # ctx.log("pid %s retrieved %s features in %ss (page=%s). Napping for %ss." % (pid, len(response['features']), round(response_time, 2), requests, round(nap_time, 2)))
          time.sleep(nap_time)
        else:
          ctx.log("pid %s retrieved %s features in %ss (%s requests)" % (pid, len(response['features']), round(response_time, 2), requests))
          # ctx.log("pid %s retrieved %s features in %ss (page=%s)." % (pid, len(response['features']), round(response_time, 2)), requests)

        request = resource.list_next(request, response)
      except HttpError, err:
        # For 'Deadline exceeded' errors
        ctx.log("Got '%s' for [%s] after %s requests" % (json.loads(err.content)['error']['message'], ', '.join(str(v) for v in bbox), requests))
        # print request.uri
        # print bbox
        # print err.resp.status
        # print json.loads(err.content)['error']['message']
        # print json.loads(err.content)['error']['errors'][0]['reason']
        # print ""
        request = None

  thread_elapsed_time = time.time() - thread_start_time
  ctx.log("pid %s finished after %ss" % (pid, round(thread_elapsed_time, 2)))
  # print "Fin"
  # print "Fin pid %s\n" % (pid)
