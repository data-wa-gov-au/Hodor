import click
import time
import json
import os
import multiprocessing
import random
import tablib
from pprintpp import pprint as pp
from shapely.geometry import box as bbox2poly
from apiclient.errors import HttpError
from hodor.gme import bbox2quarters, get_viable_bboxes
from math import ceil
from hodor.exceptions import BackendError
from multiprocessing import Manager, Pool, current_process
from retries import retries
from hodor.cli import pass_context
from hodor.gme import obey_qps

@click.group()
@pass_context
def cli(ctx):
  pass


@cli.command()
@click.option('-where', type=str,
              help='An SQL-like query to run alongside the spatial constraint.')
@click.option('-bbox', type=str,
              help='A bounding box describing the area to confine the query to. If not supplied the whole data set is considered.')
@click.option('--debug/--no-debug', default=False,
              help='Toggles debugging mode to write a logfile of the raw /features/list URIs to. Defaults to not.')
@click.option('--bbox-cache/--no-bbox-cache', default=True,
              help='Toggles whether to cache bounding boxes on first run. Defaults to caching.')
@click.option('--minimum-qps', default=10,
              help='The minimum QPS (Queries/Second) to attain on queryable areas. Defaults to 10.')
@click.option('--num-processes', default=10,
              help='The number of concurrent threads to run whilst querying. Defaults to 10 and is loosely correlated wit --minimum-qps.')
@click.argument('table-id', type=str)
@click.argument('outfile', type=click.File(mode='w'))
@pass_context
def list(ctx, where, bbox, debug, bbox_cache, minimum_qps, num_processes, table_id, outfile):
  """Retrieve features from a vector table.

  Parameters
  ----------
  ctx: Context
    A Click Context object.
  where : string
    An optional parameter specifying a GME SQL-like querying language for tables.
  bbox : string
    An optional parameter specifying the bounding box to query.
  debug : boolean
    Controls the debugging that dumps the queries run against GME to a CSV file.
  bbox_cache : boolean
    Controls whether we try to read bounding boxes from our local cache.
  minimum_qps : int
    The minimum QPS (Queries/Second) to attain on queryable areas.
  num_processes : int
    The number of concurrent threads to run whilst querying.
  table_id: int
    The GME vector tableId to query
  outfile : Click.File
    A Click.File object to write the features to. Is used as the basis for any other files generated (e.g. debugging)
  """

  # Retrieve the primary key column
  table = ctx.service().tables().get(id=table_id, fields="schema,bbox").execute()
  pkey = table['schema']['primaryKey']

  # Fetch bounding boxes
  bboxes = None
  bbox_cache_file = os.path.splitext(outfile.name)[0] + "-bbox-cache.json"

  if bbox is None:
    bbox = table['bbox']
  else:
    bbox = [float(i) for i in bbox.split(",")]

  if bbox_cache and os.path.isfile(bbox_cache_file):
    ctx.log("Retrieving cached bounding boxes...")
    with open(bbox_cache_file) as f:
      bbox_cache_json = json.load(f)

    if bbox_cache_json["bbox"] == bbox:
      bboxes = bbox_cache_json["viable_bboxes"]
    else:
      ctx.log("Cache mismatch for %s, skipping cache." % (bbox_cache_file))

  if bboxes is None:
    ctx.log("Calculating viable bounding boxes (this can take awhile)...")
    bboxes = get_viable_bboxes(ctx, table_id, minimum_qps, bbox, pkey)

    if bbox_cache:
      with open(bbox_cache_file, "w") as f:
        json.dump({
          "bbox": bbox,
          "viable_bboxes": bboxes
        }, f)
  ctx.log("Querying %s bounding boxes..." % (len(bboxes)))

  # Init for multiprocessing
  manager = Manager()
  feature_store = manager.dict()
  debug_store = manager.list()

  # Chunk up the bounding boxes
  chunk_size = int(ceil(len(bboxes) / float(num_processes)))
  if chunk_size < num_processes:
    chunk_size = 1
  chunks = [(
    ctx, bboxes[i:i+chunk_size], where, table_id,
    feature_store, pkey,
    debug, debug_store,
    minimum_qps, num_processes / minimum_qps
  ) for i in range(0, len(bboxes), chunk_size)]

  # Loot All Of The Things!
  start_time = time.time()

  pool = multiprocessing.Pool(processes=num_processes)
  pool.map(get_features, chunks)
  pool.close()
  pool.join()

  elapsed_secs = time.time() - start_time

  # Write features to disk as a GeoJSON blob
  start = time.time()
  features = {
    "type": "FeatureCollection",
    "features": [feature_store[key] for key in feature_store.keys()]
  }
  ctx.log("JSON formatted in %ss" %(round(time.time() - start, 2)))
  start = time.time()
  json.dump(features, outfile)
  ctx.log("JSON written in %ss" %(round(time.time() - start, 2)))

  # Dump debug information to CSV
  if debug:
    stats = tablib.Dataset(headers=('response_code', 'date', 'feature_count', 'time_to_first_byte_secs', 'time_to_last_byte_secs', 'bbox', 'page_num', 'request'))
    for v in debug_store:
      stats.append(v)
    with open(os.path.splitext(outfile.name)[0] + "-debug.csv", "w") as f:
      f.write(stats.csv)

  ctx.log("Got %s features in %smins" % (len(features["features"]), round(elapsed_secs / 60, 2)))


def get_features(args):
  def get_all_features(ctx, bboxes, where, table_id, feature_store, pkey, debug, debug_store, qps, qps_share):
    """Sub-process to retrieve all of the features for a given chunk of
      bounding boxes.

    Parameters
    ----------
    ctx : Context
      A Click Context object.
    bboxes : list
      A list of lists of bounding boxes to query.
    where : string
      A string describing GME's SQL-lite querying syntac.
    table_id : int
      The GME tableId to query.
    feature_store : Manager.dict()
      The master Manager().dict() object to store retrieved features to.
    pkey : string
      The name of the primary key column in the data source.
    debug : boolean
      Toggles debug mode to load httplib2 monkey patching to record request info.
    debug_store : Manager.list()
      The master Manager().list() object to store request details to for debugging.
    qps : int
      The allowed QPS. Refer to hodor.gme.obey_qps().
    qps_share : int
      Each thread's share of the QPS. Refer to hodor.gme.obey_qps().
    """
    @obey_qps(qps=qps, share=qps_share)
    @retries(10, delay=0.25, backoff=0.25)
    def features_list(request, debug_store=None):
      if debug:
        headers, response = request.execute()

        debug_store.append((
          headers['status'],
          headers['date'],
          len(response['features']) if headers['status'] == "200" else 0,
          headers.get('x---stop-time') - request_start_time,
          (request_elapsed_time),
          ', '.join(str(v) for v in bbox),
          page_counter,
          request.uri
        ))
      else:
        response = request.execute()
      return response

    if debug:
      import hodor.httplib2_patch

    thread_start_time = time.time()
    while bboxes:
      features = []

      bbox = bboxes.pop(0)
      resource = ctx.service(ident=current_process().ident).tables().features()
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

          if debug:
            response = features_list(request, debug_store)
          else:
            response = features_list(request)
          features += response['features']

          request = resource.list_next(request, response)
        except BackendError as e:
          # For 'Deadline exceeded' errors
          ctx.log("pid %s got error '%s' for [%s] after %s pages and %ss. Discarded %s features. Splitting and trying again." %
                    (pid, e, ', '.join(str(v) for v in bbox), page_counter, time.time() - resultset_start_time, len(features)))

          request = None
          features = []
          page_counter = 0
          bboxes.extend(bbox2quarters(bbox)) # Split and append to the end
          break
      else:
        # Add new features to the master store
        for f in features:
          if f['properties'][pkey] not in feature_store:
            feature_store[f['properties'][pkey]] = f

        ctx.log("pid %s retrieved %s features from %s pages in %ss" % (pid, len(features), page_counter, round(time.time() - resultset_start_time, 2)))

    thread_elapsed_time = time.time() - thread_start_time
    ctx.log("pid %s finished chunk in %smins" % (pid, round(thread_elapsed_time / 60, 2)))

  get_all_features(*args)
