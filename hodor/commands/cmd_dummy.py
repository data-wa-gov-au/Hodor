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
