import click
import os
import json
import random
from pprintpp import pprint as pp
from retries import retries
from hodor.cli import pass_context

@click.group(short_help="A collection of random tools for doing things with Google Maps Engine")
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
@click.option('--layer-id', type=str)
@click.argument('outfile', type=click.File(mode='w'))
@pass_context
def displayrules2html(ctx, layer_id, outfile):
  """Generate a simple HTML representation of the display rules for a layer."""
  def common2html(rule):
    return """%s
Zoom Levels: %s - %s""" % (rule['name'], rule['zoomLevels']['min'], rule['zoomLevels']['max'])

  def polygonOptions2html(polygonOptions):
    return """Fill: %s (opacity=%s)
Stroke: %s (opacity=%s), width %s""" % (polygonOptions['fill']['color'], round(polygonOptions['fill']['opacity'], 2), polygonOptions['stroke']['color'], round(polygonOptions['stroke']['opacity'], 2), polygonOptions['stroke']['width'])

  def filters2html(filters):
    filter_str = ""
    for f in filters:
      filter_str += "%s %s %s" % (f['column'], f['operator'], f['value'])
    return filter_str

  rules_str = ""
  layer = ctx.service.layers().get(id=layer_id, fields='style').execute()
  for rule in layer['style']['displayRules']:
    rules_str += common2html(rule) + "\n"
    rules_str += polygonOptions2html(rule['polygonOptions']) + "\n"
    rules_str += filters2html(rule['filters']) + "\n\n"

  outfile.write(rules_str)


@cli.command()
@click.option("-projectId", type=str, help="The GME projectId to query.")
@click.option("--creator-email", type=str, help="An email address representing the user who created the assets.")
@click.argument('outfile', type=click.File(mode='w'))
@pass_context
def dumprastermosaiclayers(ctx, projectid, creator_email, outfile):
  resource = ctx.service.layers()
  request = resource.list(projectId=projectid, creatorEmail=creator_email, fields='layers/id,layers/name,layers/datasourceType')
  outfile_stats = tablib.Dataset(headers=('id', 'name', 'num_layers'))

  while request != None:
    response = request.execute()

    for l in response["layers"]:
      if l["datasourceType"] == "image":
        layer = resource.get(id=l['id'], fields='datasources').execute()
        if len(layer['datasources']) > 1:
          outfile_stats.append([l['id'], l['name'], len(layer['datasources'])])

    request = resource.list_next(request, response)

  outfile_stats = outfile_stats.sort('num_layers', reverse=True)
  with open(outfile.name, "w") as f:
    f.write(outfile_stats.csv)
