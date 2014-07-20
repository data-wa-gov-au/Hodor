import click
from pprintpp import pprint as pp
from shapely.geometry import box
from apiclient.errors import HttpError
import time
import json
from retries import retries
from hodor.cli import pass_context

@click.group()
@pass_context
def cli(ctx):
  pass

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

@cli.command()

@click.argument('table-id', type=str)
@pass_context
def list(ctx, table_id):
  """Retrieve features from a vector table."""
  # table = ctx.service.tables().get(id=table_id, fields='bbox').execute()
  # pp(table)

  # bbox = [112.92112388804973, -35.180887282568534, 129.0019274186415, -13.745958064667725] # Most of WA
  bbox = [113.423171,-35.276339,124.134841,-31.094568] # The South West and inland a bit
  # bbox = [114.331765,-35.098718,119.6876,-33.028397] # The South West
  # bbox = [114.779758,-34.526107,117.457675,-33.49019] # Margaret River - Augusta
  # bbox = [114.779758, -34.008148500000004, 116.1187165, -33.49019] # Margaret River

  bboxes = bbox2quarters(bbox)

  for i, bbox in enumerate(bboxes):
    polygon = box(*bbox)

    try:
      response = ctx.service.tables().features().list(
                  id=table_id, maxResults=1000,
                  select="gx_id",
                  intersects=polygon
      ).execute()
      print "AQPS: %s, Features: %s (%s/%s)" % (response['allowedQueriesPerSecond'], len(response['features']), i, len(bboxes))
      time.sleep(0.5)
    except HttpError, err:
      if err.resp.status == 403:
        print json.loads(err.content)["error"]["message"]
        bboxes.extend(bbox2quarters(bbox))
        del bbox
      else:
        raise err
  return
  # request.add_response_callback(cb)

  response = request.execute()
  response['features'] = len(response['features'])
  pp(response)
  return



  print table_id
  return
  # Generate some fake updates
  updates = []
  request = None

  # See for a better structure: https://developers.google.com/api-client-library/python/guide/pagination
  while True:
    if request is None:
      request = ctx.service.tables().features().list(
                  id=table_id, maxResults=1000,
                  intersects="CIRCLE(116 -32, 5000)"
      )
      response = request.execute()
      updates += response["features"]
    else:
      request = ctx.service.tables().features().list_next(request, response)
      if request is None:
        break
      response = request.execute()
      updates += response["features"]

    for f in response["features"]:
      for p in f["properties"]:
        if f["properties"][p].isnumeric():
          f["properties"][p] = str(f["properties"][p])
        elif p != "gx_id":
          f["properties"][p] += "-" + str(random.randrange(1, 100000))
    print "%s (%s)" % (len(updates), updates[-1]["properties"]["gx_id"])

  updates = {
    "name": "updates.json",
    "type": "FeatureCollection",
    "crs": {"type": "name", "properties": {"name":" EPSG:4283"}},
    "features":  updates
  }

  with open(os.path.join(payloaddir, "updates.json"), "w") as f:
    json.dump(updates, f)
