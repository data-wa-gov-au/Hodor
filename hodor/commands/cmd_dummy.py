import click
import os
import json
import random
from pprintpp import pprint as pp
from retries import retries
from hodor.cli import pass_context

@click.group(short_help="Create dummy data in Google Maps Engine")
@pass_context
def cli(ctx):
  pass


@cli.command()
@pass_context
def tag_all_rasters(ctx):
  import httplib2
  h = httplib2.Http()
  request_uri = "https://www.googleapis.com/mapsengine/exp2/rasters?projectId=06151154151057343427&fields=nextPageToken,rasters/id,rasters/tags"
  next_page_token = ""

  while next_page_token is not None:
    response, content = h.request(
      request_uri + "&pageToken=" + next_page_token, "GET",
      headers={
        "Authorization": "Bearer " + ctx.access_token,
        "Content-type": "application/json"
      }
    )

    if response['status'] == "200":
      # Tag and untag all rasters to trigger GME to switch them to the new ACL system
      content = json.loads(content)
      for r in content['rasters']:
        patch = ctx.service.rasters().patch(id=r['id'], body={
          "tags": r['tags'] + ["hodor-patch"]
        }).execute()
        patch = ctx.service.rasters().patch(id=r['id'], body={
          "tags": r['tags']
        }).execute()
        ctx.log("%s patched OK" % (r['id']))

      next_page_token = content['nextPageToken'] if 'nextPageToken' in content else None
    else:
      raise Exception("Got a non-200 response")


@cli.command()
@click.option('--table-id', type=str)
@click.argument('payloaddir', type=click.Path(exists=True, file_okay=False, resolve_path=True))
@pass_context
def updates(ctx, table_id, payloaddir):
  """Generate a set of fake updatse from an existing datasource."""
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
