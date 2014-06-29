import click
import json
import os
import time

from os import walk
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
  config = json.load(configfile)

  # Fetch payload files
  config['files'] = {}
  payloaddir = os.path.dirname(configfile.name) + "/payload"
  for (dirpath, dirnames, filenames) in walk(payloaddir):
    config['files'] = [{'filename': i} for i in filenames]
    break

  # Create asset and upload payload files
  response = resource.upload(body=config).execute()
  ctx.log("Asset '%s' created with id %s" % (response['name'], response['id']))
  start_time = time.time()
  for i in config['files']:
    ctx.upload_file(os.path.join(payloaddir, i['filename']), response['id'], resource)
  ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

  # Poll until asset has processed
  start_time = time.time()
  processingStatus = "processing"
  while processingStatus == "processing":
    response = resource.get(id=response['id']).execute()
    ctx.log("Status of asset is '%s', waiting." % (response["processingStatus"]))

    if response["processingStatus"] == "complete":
      ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
      return response
    elif response["processingStatus"] == "failed":
      ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
      raise Exception("Asset failed to process in time.")
    else:
      if time.time() - start_time >= ctx.asset_processing_wait:
        raise Exception("Hodor Hodor Hodor!\nGiving up waiting on '%s'" % (response['id']))
      else:
        time.sleep(10)
