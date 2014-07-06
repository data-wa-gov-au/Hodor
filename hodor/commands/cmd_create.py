import click
import json
import os
import time

from os import walk
from retries import retries
from hodor.cli import pass_context

@click.group(short_help="Create assets in Google Maps Engine")
# https://google-api-python-client.googlecode.com/hg/docs/epy/apiclient.http.MediaFileUpload-class.html
@click.option('--chunk-size', default=-1,
              help='File chunk size in kilobytes. Defaults to using streaming where available.')
@click.option('--asset-processing-wait', default=90,
              help='Length of time in minutes we should wait for an asset to process before giving up. Defaults to 90 minutes.')
@pass_context
def cli(ctx, chunk_size, asset_processing_wait):
  ctx.chunk_size = chunk_size
  ctx.processing_timeout_mins = asset_processing_wait


@cli.command()
@click.option('--mosaic-id',
              help="The raster mosaic to add the newly created raster to.")
@click.option('--process-mosaic/--no-process-mosaic', default=False,
              help="Whether the given raster mosaic should be processed after the raster is added. Defaults to false.")
@click.argument('configfile', type=click.File('r'))
@pass_context
def raster(ctx, mosaic_id, process_mosaic, configfile):
  """Create a new raster asset in Google Maps Engine"""
  @retries((ctx.processing_timeout_mins * 60) / 10, delay=10, backoff=1)
  def poll_asset_processing(ctx, mosaic_id):
    response = ctx.service.rasterCollections().get(id=mosaic_id).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Asset processing status is '%s'" % (response["processingStatus"]))

  # Create new raster asset
  asset = uploader(ctx, ctx.service.rasters(), configfile)

  # Optionally, add it to an existing raster mosaic
  if mosaic_id is not None:
    ctx.service.rasterCollections().rasters().batchInsert(id=mosaic_id, body={"ids":[asset["id"]]}).execute()
    ctx.log("Asset '%s' added to mosaic '%s'" % (asset["id"], mosaic_id))

    if process_mosaic == True:
      ctx.service.rasterCollections().process(id=mosaic_id).execute()
      ctx.log("Mosaic '%s' processing started" % (mosaic_id))

      # Poll until asset has processed
      start_time = time.time()
      response = poll_asset_processing(ctx, mosaic_id)
      if response["processingStatus"] == "complete":
        ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
        return response
      elif response["processingStatus"] == "failed":
        ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
        raise Exception("Asset failed to process")


@cli.command()
@click.argument('configfile', type=click.File('r'))
@pass_context
def vector(ctx, configfile):
  """Create a new vector asset in Google Maps Engine"""
  uploader(ctx, ctx.service.tables(), configfile)


def uploader(ctx, resource, configfile):
  @retries(10)
  def create_asset(resource, config):
    return resource.upload(body=config).execute()

  @retries((ctx.processing_timeout_mins * 60) / 10, delay=10, backoff=1)
  def poll_asset_processing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Asset processing status is '%s'" % (response["processingStatus"]))

  config = json.load(configfile)

  # Fetch payload files
  config['files'] = {}
  payloaddir = os.path.dirname(configfile.name) + "/payload"
  for (dirpath, dirnames, filenames) in walk(payloaddir):
    config['files'] = [{'filename': i} for i in filenames]
    break

  # Create asset and upload payload files
  response = create_asset(resource, config)
  ctx.log("Asset '%s' created with id %s" % (response['name'], response['id']))

  # Upload the payload files
  start_time = time.time()
  for i in config['files']:
    ctx.upload_file(os.path.join(payloaddir, i['filename']), response['id'], resource)
  ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

  # Poll until asset has processed
  start_time = time.time()
  response = poll_asset_processing(ctx, resource, response['id'])
  if response["processingStatus"] == "complete":
    ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    return response
  elif response["processingStatus"] == "failed":
    ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    raise Exception("Asset failed to process")
