import click
import json
import os
import time
from pprintpp import pprint as pp
from retries import retries
from hodor.cli import pass_context

@click.group(short_help="Utilities for bulk ingesting data.")
@pass_context
def cli(ctx):
  pass

@cli.command()
@click.argument('configfile', type=click.File('r'))
@pass_context
def raster(ctx, configfile):
  @retries(10)
  def create_asset(config):
    return ctx.service.rasters().upload(body=config).execute()

  # Create asset and upload payload files
  config = json.load(configfile)

  payloaddir = os.path.join(os.path.dirname(configfile.name), "payload")
  for (dirpath, dirnames, filenames) in os.walk(payloaddir):
    for filename in filenames:
      config['name'] = filename
      config['files'] = [{'filename': filename}]

      # Create asset
      response = create_asset(config)
      ctx.log("Raster '%s' created with id %s" % (filename, response['id']))

      # Upload payload file
      start_time = time.time()
      ctx.upload_file(os.path.join(payloaddir, filename), response['id'], ctx.service.rasters())
