import click
from pprintpp import pprint as pp
from shapely.geometry import box as bbox2poly
from apiclient.errors import HttpError
import time
import json
import os
import multiprocessing
import random
from math import ceil
from hodor.exceptions import QueryTooExpensive, BackendError, QPSTooLow
from multiprocessing import Manager, Pool
from retries import retries
from hodor.cli import pass_context

# @TODO Understand multiprocessing, Pool, and Manager better

@click.group()
@pass_context
def cli(ctx):
  pass

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


@cli.command()
@click.option('-where', type=str,
              help='An SQL-like query to run alongside the spatial constraint')
@click.option('-bbox', type=str,
              help='A bounding box describing the area to confine the query to')
@click.argument('table-id', type=str)
@click.argument('outfile', type=click.File(mode='w'))
@pass_context
def list(ctx, where, bbox, table_id, outfile):
  """Retrieve features from a vector table.

  Parameters
  ----------
  ctx: Context
    A Click Context object.
  table_id: int
    The GME vector tableId to query
  """

  def get_viable_bboxes(bbox, pkey):
    """Calculate the bounding boxes within a given area
      that it's viable to query given GME's known limits.

    Parameters
    ----------
    bbox: list
      A bounding box in the traditional order of [minx, miny, maxx, maxy]
    """
    @retries(10, delay=0.25, backoff=0.25)
    def features_list(polygon, pkey):
      request_start_time = time.time()
      response = ctx.service.tables().features().list(
                  id=table_id, maxResults=1,
                  select=pkey,
                  intersects=polygon
      ).execute()

      # Obey GME's QPS limits
      request_elapsed_time = time.time() - request_start_time
      nap_time = max(0, 1.3 - request_elapsed_time)
      time.sleep(nap_time)

      return response

    untestedbboxes = bbox2quarters(bbox) # Split the input into at least four separate bounding boxes
    viablebboxes = []
    while untestedbboxes:
      try:
        bbox = untestedbboxes.pop(0)
        response = features_list(bbox2poly(*bbox), pkey)

        if response['allowedQueriesPerSecond'] < minrequiredqps:
          raise QPSTooLow("Query too expensive.")

        if len(response['features']) > 0:
          viablebboxes.append(bbox)
          ctx.log("%s viable bounding boxes, %s remaining to test" % (len(viablebboxes), len(untestedbboxes)))

      except (QueryTooExpensive, QPSTooLow) as e:
        ctx.vlog("%s got error '%s', splitting." % (bbox, e))
        untestedbboxes.extend(bbox2quarters(bbox))

    # Shuffle to distribute the expensive queries across the threads
    random.shuffle(viablebboxes)
    return viablebboxes

  # Init config
  minrequiredqps = 10
  processes = 10

  # Retrieve the primary key column
  table = ctx.service.tables().get(id=table_id, fields="schema,bbox").execute()
  pkey = table['schema']['primaryKey']

  # Fetch bounding boxes
  ctx.log("Calculating viable bounding boxes (this can take awhile)...")
  if bbox is not None:
    bbox = [float(i) for i in bbox.split(", ")]
  else:
    bbox = table['bbox']
  bboxes = get_viable_bboxes(bbox, pkey)
  ctx.log("Querying %s bounding boxes..." % (len(bboxes)))

  # Init for multiprocessing
  manager = Manager()
  featurestore = manager.list()
  pkeystore = manager.list()

  # Chunk up the bounding boxes
  chunk_size = int(ceil(len(bboxes) / float(processes)))
  if chunk_size < processes:
    chunk_size = 1
  chunks = [(
    ctx, bboxes[i:i+chunk_size], where, table_id,
    featurestore, pkey, pkeystore
  ) for i in range(0, len(bboxes), chunk_size)]

  # Loot All Of The Things!
  start_time = time.time()

  pool = multiprocessing.Pool(processes=processes)
  pool.map(getFeatures, chunks)
  pool.close()
  pool.join()

  elapsed_secs = time.time() - start_time

  features = {
    "type": "FeatureCollection",
    "features": [f for sublist in featurestore for f in sublist]
  }
  json.dump(features, outfile)

  ctx.log("Got %s features in %smins" % (len(features["features"]), round(elapsed_secs / 60, 2)))


def getFeatures(args):
  """Sub-process to retrieve all of the features for a given chunk of
    bounding boxes.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  bboxes : list
    A list of lists of bounding boxes to query.
  table_id : int
    The GME tableId to query.
  featurestore : Manager.List()
    The master Manager().List() object to store retrieved features to.
  """
  @retries(10, delay=0.25, backoff=0.25)
  def features_list(request):
    return request.execute()

  ctx, bboxes, where, table_id, featurestore, pkey, pkeystore = args

  pid = multiprocessing.current_process().pid
  if pid not in ctx.thread_safe_services:
    ctx.log("## pid %s getting a new token... ##" % (pid))
    ctx.thread_safe_services[pid] = ctx.get_authenticated_service(ctx.RW_SCOPE)

  thread_start_time = time.time()
  while bboxes:
    features = []

    bbox = bboxes.pop(0)
    resource = ctx.thread_safe_services[pid].tables().features()
    request = resource.list(
      id=table_id, maxResults=1000,
      intersects=bbox2poly(*bbox),
      where=where
    )

    page_counter = 0
    resultset_start_time = time.time()
    while request != None:
      try:
        page_counter += 1

        request_start_time = time.time()
        response = features_list(request)
        request_elapsed_time = time.time() - request_start_time

        # De-dupe returned features
        cleaned_features = []
        for f in response['features']:
          if f['properties'][pkey] not in pkeystore:
            pkeystore.append(f['properties'][pkey])
            cleaned_features.append(f)
        features += cleaned_features

        # Obey GME's QPS limits
        nap_time = max(0, 1 - request_elapsed_time)
        time.sleep(nap_time)

        request = resource.list_next(request, response)
      except BackendError as e:
        # For 'Deadline exceeded' errors
        ctx.log("Got error '%s' for [%s] after %s pages and %ss. Splitting and trying again." %
                  (e, ', '.join(str(v) for v in bbox), page_counter, time.time() - resultset_start_time))
        request = None
        features = []
        bboxes.extend(bbox2quarters(bbox)) # Split and append to the end
        break
    else:
      # Add features to master store
      featurestore.append(features)
      ctx.log("pid %s retrieved %s features from %s pages in %ss" % (pid, len(features), page_counter, round(time.time() - resultset_start_time, 2)))

  thread_elapsed_time = time.time() - thread_start_time
  ctx.log("pid %s finished chunk in %smins" % (pid, round(thread_elapsed_time / 60, 2)))
