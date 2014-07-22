import click
import json
import os
import time
import httplib2
from pprintpp import pprint as pp
from retries import retries
from hodor.cli import pass_context

@click.group(short_help="Testing faux table replace in GME")
@pass_context
def cli(ctx):
  pass


@cli.command()
@click.argument('project-id', type=str)
@click.argument('table-id', type=str)
@pass_context
def create_dummy(ctx, project_id, table_id):
  @retries(10)
  def create_dummy_table(config):
    return ctx.service.tables().create(body=config).execute()

  table = ctx.service.tables().get(id=table_id, fields="name,schema").execute()
  dummy_table = create_dummy_table({
    "projectId": project_id,
    "name": table["name"] + "_dummy",
    "draftAccessList": "Map Editors",
    "schema": {
      "columns": [v for v in table["schema"]["columns"] if v["name"] != "gx_id"]
    }
  })
  ctx.log("Created empty dummy table " + dummy_table["id"])


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
