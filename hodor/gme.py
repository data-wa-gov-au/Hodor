import time
import random
import os
from retries import retries
from hodor.exceptions import *
from shapely.geometry import box as bbox2poly
from apiclient.http import MediaFileUpload
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing.dummy import current_process

class Asset:
  TABLE = 1
  RASTER = 2
  RASTER_COLLECTION = 3
  LAYER = 4
  MAP = 5
  PROJECT = 6

def get_asset_resource(service, type):
  """Gets the correct base resource for an asset.

  Parameters
  ----------
  service : apiclient.discovery.Resource
    A GME API Client discovery resource.
  type : int
    A GME asset type defined by the Asset class.
  """
  return {
    Asset.TABLE: service.tables(),
    Asset.RASTER: service.rasters(),
    Asset.RASTER_COLLECTION: service.rasterCollections(),
    Asset.LAYER: service.layers(),
    Asset.MAP: service.maps(),
    Asset.PROJECT: service.projects()
  }[type]


def upload_files_multithreaded(ctx, asset_id, asset_type, filepaths, chunk_size=-1):
  """Upload a given set of files to an asset simultaneously.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  asset_id : str
    The Id of a valid raster or vector asset.
  asset_type : int
    A GME asset type defined by the Asset class.
  filepaths : arr
    An array of absolute paths to the files.
  chunk_size : int
    The size of each upload chunk (must be a multiple of 256KB). Defaults to -1 (native Python streaming)
  """
  pool = ThreadPool(len(filepaths))
  for filepath in filepaths:
    print "begin %s" % (filepath)
    pool.apply_async(upload_file_worker, args=(ctx, asset_id, asset_type, filepath, chunk_size,)).wait(timeout=1)
  pool.close()
  pool.join()


@trace_unhandled_exceptions
def upload_file_worker(ctx, asset_id, asset_type, filepath, chunk_size):
  print "upload_file_worker %s" % (filepath)
  """Upload a given file to an asset in its own thread as
  part of upload_files_multithreaded().

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  asset_id : str
    The Id of a valid raster or vector asset.
  asset_type : int
    A GME asset type defined by the Asset class.
  filepath : str
    The absolute path to the file.
  chunk_size : int
    The size of each upload chunk (must be a multiple of 256KB). Defaults to -1 (native Python streaming)
  """
  @retries(1000)
  def next_chunk(ctx, request):
    return request.next_chunk()

  ctx.log("Begun uploading %s" % (os.path.basename(filepath)))
  start_time = time.time()

  media = MediaFileUpload(filepath, chunksize=chunk_size, resumable=True)
  if not media.mimetype():
    media = MediaFileUpload(filepath, mimetype='application/octet-stream', chunksize=chunk_size, resumable=True)

  resource = get_asset_resource(ctx.service(ident=current_process().ident), asset_type)
  request = resource.files().insert(id=asset_id, filename=os.path.basename(filepath), media_body=media)
  response = None
  while response is None:
    try:
      start_time_chunk = time.time()
      progress, response = next_chunk(ctx, request)
      # Dodgy math is dodgy
      # if progress:
      #   Mbps = ((chunk_size / (time.time() - start_time_chunk)) * 0.008 * 0.001)
      #   ctx.log("%s%% (%s/Mbps)" % (round(progress.progress() * 100), round(Mbps, 2)))
    except NoContent as e:
      # Files uploads return a 204 No Content "error" that actually means it's finished successfully.
      response = ""

  ctx.log("Finished uploading %s (%s mins)" % (os.path.basename(filepath), round((time.time() - start_time) / 60, 2)))


def upload_file(ctx, asset_id, asset_type, filepath, chunk_size=-1):
  """Upload a given file to an asset.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  asset_id : str
    The Id of a valid raster or vector asset.
  asset_type : str
    The type of asset being represented. Possible values: table, raster
  filepath : str
    The absolute path to the file.
  chunk_size : int
    The size of each upload chunk (must be a multiple of 256KB). Defaults to -1 (native Python streaming)
  """
  @retries(1000)
  def next_chunk(ctx, request):
    return request.next_chunk()

  ctx.log("Begun uploading %s" % (os.path.basename(filepath)))
  start_time = time.time()

  media = MediaFileUpload(filepath, chunksize=chunk_size, resumable=True)
  if not media.mimetype():
    media = MediaFileUpload(filepath, mimetype='application/octet-stream', chunksize=chunk_size, resumable=True)

  resource = ctx.service().tables() if asset_type == "vector" else ctx.service().rasters()
  request = resource.files().insert(id=asset_id, filename=os.path.basename(filepath), media_body=media)
  response = None
  while response is None:
    try:
      start_time_chunk = time.time()
      progress, response = next_chunk(ctx, request)
      # Dodgy math is dodgy
      if progress:
        Mbps = ((chunk_size / (time.time() - start_time_chunk)) * 0.008 * 0.001)
        ctx.log("%s%% (%s/Mbps)" % (round(progress.progress() * 100), round(Mbps, 2)))
    except NoContent as e:
      # Files uploads return a 204 No Content "error" that actually means it's finished successfully.
      response = ""

  ctx.log("Finished uploading %s (%s mins)" % (os.path.basename(filepath), round((time.time() - start_time) / 60, 2)))


def upload_file_init(ctx, asset_id, asset_type, filepath):
  """Upload the first 256KB of a given file to an asset.
  This forces it into an "uploading" state which prevents processing from
  occurring until all files are uploaded.

  Built as an experiment and abandoned in favour of multithreaded uploading.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  asset_id : str
    The Id of a valid raster or vector asset.
  asset_type : str
    The type of asset being represented. Possible values: table, raster
  filepath : str
    The absolute path to the file.
  """
  @retries(1000)
  def next_chunk(ctx, request):
    return request.next_chunk()

  chunk_size = 262144 # 256KB - smallest possible chunk size for resumable upload
  media = MediaFileUpload(filepath, chunksize=chunk_size, resumable=True)
  if not media.mimetype():
    media = MediaFileUpload(filepath, mimetype='application/octet-stream', chunksize=chunk_size, resumable=True)

  resource = ctx.service().tables() if asset_type == "vector" else ctx.service().rasters()
  request = resource.files().insert(id=asset_id, filename=os.path.basename(filepath), media_body=media)

  try:
    next_chunk(ctx, request)
  except NoContent as e:
    pass
  ctx.log("Init uploading %s" % (os.path.basename(filepath)))


def poll_asset_processing(ctx, asset_id, resource):
  """Poll a given asset until its processing stage has completed.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  asset_id : str
    The Id of a valid raster or vector asset.
  resource : apiclient.discovey.Resource
    A GME API Client discovery resource.

  Returns
  -------
  dict
    A dictionary representing the asset in GME.
  """
  @retries((180 * 60) / 10, delay=30, backoff=1)
  def poll(ctx, resource, asset_id):
    response = resource.get(id=asset_id).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    elif response['processingStatus'] == 'ready':
      # Fix for GME's issue where it mistakenly reports 'ready for processing' upon completion.
      process(ctx, resource, asset_id)
      raise Exception("Asset processing status is '%s'. Initiated reprocessing." % (response["processingStatus"]))
    else:
      raise Exception("Asset processing status is '%s'" % (response["processingStatus"]))

  @retries(10)
  def process(ctx, resource, asset_id):
    return resource.process(id=asset_id).execute()

  start_time = time.time()
  response = poll(ctx, resource, asset_id)
  if response["processingStatus"] == "complete":
    ctx.log("Processing complete (%s mins)" % (round((time.time() - start_time) / 60, 2)))
    return response
  elif response["processingStatus"] == "failed":
    ctx.vlog("Processing failed (%s mins)" % (round((time.time() - start_time) / 60, 2)))
  raise Exception("Asset failed to process.")


def poll_layer_publishing(ctx, asset_id):
  """Poll a given layer until its publishing stage has completed.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  asset_id : str
    The Id of a valid raster or vector asset.

  Returns
  -------
  dict
    A dictionary representing the layer in GME.
  """
  @retries((180 * 60) / 10, delay=30, backoff=1)
  def poll(ctx, asset_id):
    response = ctx.service().layers().get(id=asset_id).execute()
    if response['publishingStatus'] == 'published':
      return response
    else:
      raise Exception("Layer publishing status is '%s'" % (response["publishingStatus"]))

  start_time = time.time()
  response = poll(ctx, asset_id)
  ctx.log("Publishing complete (%s mins)" % (round((time.time() - start_time) / 60, 2)))
  return response


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


def get_viable_bboxes(ctx, table_id, minrequiredqps, bbox, pkey):
  """Calculate the bounding boxes within a given area
    that it's viable to query given GME's known limits.

  Parameters
  ----------
  ctx: Context
    A Click Context object.
  table_id: int
    The GME vector tableId to query
  minrequiredqps : int
    The minimum QPS (Queries Per Second) required from GME for a query to be considered viable.
  bbox: list
    A bounding box in the traditional order of [minx, miny, maxx, maxy]
  pkey : string
    The primary key of the table being queried.
  """
  @obey_qps()
  @retries(10, delay=0.25, backoff=0.25)
  def features_list(polygon, pkey):
    request_start_time = time.time()
    return ctx.service().tables().features().list(
                id=table_id, maxResults=1,
                select=pkey,
                intersects=polygon
    ).execute()

  untestedbboxes = bbox2quarters(bbox) # Split the input into at least four separate bounding boxes
  viablebboxes = []
  while untestedbboxes:
    try:
      bbox = untestedbboxes.pop(0)
      response = features_list(bbox2poly(*bbox), pkey)

      if 'allowedQueriesPerSecond' in response and response['allowedQueriesPerSecond'] < minrequiredqps:
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


def getMapLayerIds(map):
  """Extracts the layerIds from a map's contents block.

  Parameters
  ----------
  map : dict
    A map object returned from /maps/get
  """
  def traverse(o):
    for value in o:
      if "contents" in value:
        for subvalue in traverse(value["contents"]):
          yield subvalue

      elif "type" in value and value["type"] == "layer":
        yield value["id"]

  return traverse(map["contents"])


def obey_qps(qps=1, share=1):
  """
  Function decorator for obeying GME's Queries/Second threshold.

  Defaults are set as per GME's limits for free accounts. https://developers.google.com/maps-engine/documentation/limits#free_accounts

  Parameters
  ----------
  qps : int
    The number of queries/second you are permitted to issue for the requests in question. Defaults to 1.
  share : int
    Your share of the qps as a value between 0 and 1. Only relevant if you have other users/threads consuming your qps pool.
  """

  def dec(func):
    def dec2(*args, **kwargs):
      start_time = time.time()
      response = func(*args, **kwargs)
      elapsed_time = time.time() - start_time

      if elapsed_time < (1 / (qps * share)):
        time.sleep((1 / (qps * share) - elapsed_time))
      return response
    return dec2
  return dec
