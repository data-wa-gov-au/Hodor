import click
import json
import os
import time

from os import walk
from apiclient.errors import HttpError
from retries import retries, gme_exc_handler
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
  ctx.asset_processing_wait = asset_processing_wait * 60 * 60


@cli.command()
@click.argument('configfile', type=click.File('r'))
@pass_context
def raster(ctx, configfile):
  """Create a new raster asset in Google Maps Engine"""
  uploader(ctx, ctx.service.rasters(), configfile)


@cli.command()
@click.argument('configfile', type=click.File('r'))
@pass_context
def vector(ctx, configfile):
  """Create a new vector asset in Google Maps Engine"""
  uploader(ctx, ctx.service.tables(), configfile)


def uploader(ctx, resource, configfile):
  @retries(10, exceptions=(HttpError), hook=gme_exc_handler)
  def create_asset(resource, config):
    return resource.upload(body=config).execute()

  @retries(100, delay=2)
  def poll_asset_processing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      ctx.log("Status of asset is '%s', waiting." % (response["processingStatus"]))
      raise Exception("Hodor Hodor Hodor! Asset failed to process in time!")

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
