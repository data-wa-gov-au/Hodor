import time
import random
import os
from retries import retries
from hodor.exceptions import *
from shapely.geometry import box as bbox2poly
from apiclient.http import MediaFileUpload


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
  @retries(5)
  def next_chunk(ctx, request):
    return request.next_chunk()

  ctx.log("Begun uploading %s" % (os.path.basename(filepath)))
  start_time = time.time()

  media = MediaFileUpload(filepath, chunksize=chunk_size, resumable=True)
  if not media.mimetype():
    media = MediaFileUpload(filepath, mimetype='application/octet-stream', chunksize=chunk_size, resumable=True)

  resource = ctx.service.tables() if asset_type == "vector" else ctx.service.rasters()
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


def getResourceForAsset(resource, type):
  """Gets the correct base resource for any asset based on its type.

  Parameters
  ----------
  resource : apiclient.discovey.Resource
    A GME API Client discovery resource.
  type : string
    A GME asset type, typically returned from a type-agnostic resource (typically /assets/list).
    Possible values: maps, layers, tables, rasters, rasterCollections.
  """
  return {
    "map": resource.maps(),
    "layer": resource.layers(),
    "table": resource.tables(),
    "raster": resource.rasters(),
    "rasterCollection": resource.rasterCollections()
  }[type]
