import click
import json
import os
import time
import httplib2
import re
from urllib import quote_plus
from pprintpp import pprint as pp
from retries import retries
from hodor.cli import pass_context

@click.group(short_help="For performing faux table replace in GME whereby datasources an be updated without altering assetIds.")
@pass_context
def cli(ctx):
  pass


@cli.command()
# @TOOO Make this click.File afterall. Intention being that AE has a bunch of .bat files at the top level
# that he points at individual jobfiles and > pipes output to a /logs dir
@click.argument('jobstore', type=click.Path(exists=True, file_okay=False, resolve_path=True))
@pass_context
def runjob(ctx, jobstore):
  """
    And on the pedestal these words appear:
    'My name is Ozymandias, king of kings:
    Look on my works, ye Mighty, and despair!'
    Nothing beside remains. Round the decay
    Of that colossal wreck, boundless and bare
    The lone and level sands stretch far away.
      http://en.wikipedia.org/wiki/Ozymandias
  """

  config = {
    "projectId": "09372590152434720789",
    "custodian" : "lgate",
    "title" : "admin_lga",
    "dateStamp" : "{Date}"
    # "partNumber" : "1",
    # "partCount" : "2"
  }

  datasource_name_part = config["title"] + "_" + config['custodian']
  if "partNumber" in config and "partCount" in config:
    datasource_name_part += "_" + config["partNumber"] + "_of_" + config["partCount"]

  h = httplib2.Http()
  resp, content = h.request(
    "https://www.googleapis.com/mapsengine/search_tt/assets?type=table&search=" +\
        quote_plus((datasource_name_part.replace("_", " "))) + "&projectId=" + config["projectId"], "GET",
    headers={
      "Authorization": "Bearer " + ctx.access_token
    }
  )

  table = None
  if resp['status'] != "200":
    raise Exception("Received status '" + resp['status'] + "' from search. Gave up.")
  else:
    valid_tables = []
    for t in json.loads(content)["assets"]:
      m = re.search("^(" + datasource_name_part + "_[0-9]{8})$", t["name"])
      if m and "archive" not in t["tags"] and t["processingStatus"] == "complete":
        valid_tables.append(t)
        pp(t)

    if len(valid_tables) != 1:
      raise Exception("Found " + str(len(valid_tables)) + " matching tables. Gave up - we require a single matching table.")
    else:
      table = valid_tables[0]

    # Step 1 - Create an empty placeholder table with the same schema
    createPlaceholderTable(ctx, table)



def createPlaceholderTable(ctx, table):
  @retries(10)
  def get_table_schema(table):
    return ctx.service.tables().get(id=table['id'], fields="schema").execute()["schema"]

  @retries(10)
  def create_table(config):
    pp(config)
    return ctx.service.tables().create(body=config).execute()

  schema = get_table_schema(table)

  placeholder_table = create_table({
    "projectId": table["projectId"],
    "name": table["name"] + "_placeholder",
    "draftAccessList": "Map Editors",
    "schema": {
      "columns": [v for v in schema["columns"] if v["name"] != "gx_id"],
      "primaryKey": schema["primaryKey"] if schema["primaryKey"]  != "gx_id" else None
    }
  })
  ctx.log("Created empty placeholder table " + placeholder_table["id"])




@cli.command()
@click.argument('layer-id', type=str)
@click.argument('table-id', type=str)
@pass_context
def patch(ctx, layer_id, table_id):
  patch = {
    "datasources": [
      {"id": table_id}
    ]
  }
  h = httplib2.Http()

  resp, content = h.request(
    "https://www.googleapis.com/mapsengine/exp2/layers/" + layer_id, "PATCH",
    body=json.dumps(patch),
    headers={
      "Authorization": "Bearer " + ctx.access_token,
      "Content-type": "application/json"
    }
  )

  if resp["status"] != "204":
    raise Exception("Failed patching layer " + layer_id)
  else:
    ctx.log("Layer successfully patched.")


@cli.command()
@click.option('--chunk-size', default=10485760,
              help='File chunk size in bytes. Defaults to using streaming to send files.')
@click.option('--asset-processing-wait', default=90,
              help='Length of time in minutes we should wait for an asset to process before giving up. Defaults to 90 minutes.')
@click.argument('table-id', type=str)
@click.argument('payload-dir', type=click.Path(exists=True, file_okay=False))
@pass_context
def replace(ctx, table_id, payload_dir, chunk_size, asset_processing_wait):
  ctx.chunk_size = chunk_size
  ctx.processing_timeout_mins = asset_processing_wait

  @retries((ctx.processing_timeout_mins * 60) / 10, delay=10, backoff=1)
  def poll_asset_processing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Asset processing status is '%s'" % (response["processingStatus"]))

  # Fetch the payload files
  config = {}
  for (dirpath, dirnames, filenames) in os.walk(payload_dir):
    config['files'] = [{'filename': i} for i in filenames]

  # Upload the payload files
  start_time = time.time()
  for i in config['files']:
    ctx.upload_file(os.path.join(payload_dir, i['filename']), table_id, ctx.service.tables())
  ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

  # Poll until asset has processed
  start_time = time.time()
  response = poll_asset_processing(ctx, ctx.service.tables(), table_id)
  if response["processingStatus"] == "complete":
    ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    return response
  elif response["processingStatus"] == "failed":
    ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    raise Exception("Asset failed to process")


@cli.command()
@click.argument('layer-id', type=str)
@pass_context
def reprocess_layer(ctx, layer_id):
  @retries(100, delay=10, backoff=1)
  def poll_asset_processing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Asset processing status is '%s'" % (response["processingStatus"]))

  ctx.service.layers().process(id=layer_id).execute()
  ctx.log("Layer processesing begun")

  # Poll until asset has processed
  start_time = time.time()
  response = poll_asset_processing(ctx, ctx.service.layers(), layer_id)
  if response["processingStatus"] == "complete":
    ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    return response
  elif response["processingStatus"] == "failed":
    ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    raise Exception("Asset failed to process")


@cli.command()
@click.argument('layer-id', type=str)
@pass_context
def republish_layer(ctx, layer_id):
  @retries(100, delay=10, backoff=1)
  def poll_asset_processing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Asset processing status is '%s'" % (response["processingStatus"]))

  ctx.service.layers().publish(id=layer_id).execute()
  ctx.log("Layer publishing begun")

  # Poll until asset has processed
  start_time = time.time()
  response = poll_asset_processing(ctx, ctx.service.layers(), layer_id)
  if response["processingStatus"] == "complete":
    ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    return response
  elif response["processingStatus"] == "failed":
    ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    raise Exception("Asset failed to process")
