import click
import json
import os
import time
from hodor.gme import upload_file, poll_asset_processing, poll_layer_publishing
from pprintpp import pprint as pp
from os import walk
from retries import retries
from hodor.cli import pass_context

# @TODO Move uploader() and create_vector_layers() to gme.py
# @TODO Document all functions according to the NumPy standard
# @TODO Make the two create entry points accept:
#   * A single config.json file with a payload directory
#   * A directory with sub(-sub)-directories containing config.json and payload directories.
#   * A directory with a single config.json acting as a template for multiple assets (this deprecates the bulk-load functionality)
# @TODO Implement multithreading processing:
#   * Using Pool.apply_async()
#   * Just continue to dump logs to stdout at the moment
#   * How to handle multithreading uploading?
#   * We only need ctx's methods - so don't pass, just import?

@click.group(short_help="Create assets in Google Maps Engine")
# https://google-api-python-client.googlecode.com/hg/docs/epy/apiclient.http.MediaFileUpload-class.html
@click.option('--chunk-size', default=10485760,
              help='File chunk size in bytes. Defaults to chunks of 10mb. Pass -1 to use native streaming in Python instead.')
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
@click.option('--layer-id',
              help="The raster layer to add the newly created layer to. Not compatible with mosaic options.")
@click.argument('configfile', type=click.File('r'))
@pass_context
def raster(ctx, mosaic_id, process_mosaic, layer_id, configfile):
  """Create a new raster asset in Google Maps Engine"""

  # Create new raster asset
  raster = hodor_uploader(ctx, "raster", configfile)

  # Optionally, add it to an existing raster mosaic
  if mosaic_id is not None:
    ctx.service.rasterCollections().rasters().batchInsert(id=mosaic_id, body={"ids": [raster["id"]]}).execute()
    ctx.log("Asset '%s' added to mosaic '%s'" % (raster["id"], mosaic_id))

    if process_mosaic == True:
      ctx.service.rasterCollections().process(id=mosaic_id).execute()
      ctx.log("Mosaic '%s' processing started" % (mosaic_id))

      # Poll until raster has processed
      poll_asset_processing(ctx, mosaic_id, ctx.service.rasterCollections())

  # Optionally, add it to an existing layer
  elif layer_id is not None:
    layer = ctx.service.layers().get(id=layer_id).execute()

    if layer['datasourceType'] != "image":
      raise Exception("Layer datasourceType is not 'image'.")
    if len(layer['datasources']) >= 100:
      raise Exception("The GME API currently only allows us to patch layers <= 100 datasources. Sad Keanu :(")

    ctx.service.layers().patch(id=layer_id, body={
      "datasources": layer['datasources'] + [raster["id"]]
    }).execute()
    ctx.log("Asset '%s' added to layer '%s'" % (raster["id"], layer_id))


@cli.command()
@click.option('--layer-configfile', type=click.File('r'),
              help="An optional JSON configuration file containing layer(s) to create from the new table.")
@click.argument('configfile', type=click.File('r'))
@pass_context
def vector(ctx, layer_configfile, configfile):
  """Create a new vector asset in Google Maps Engine"""
  @retries(10)
  def create_layer(ctx, config):
    return ctx.service.layers().create(body=config, process=True).execute()

  table = hodor_uploader(ctx, "vector", configfile)

  if layer_configfile:
    config = json.load(layer_configfile)

    for layer in config["layers"]:
      layer["projectId"] = config["projectId"]
      layer["datasources"] = [{"id": table["id"]}]

      # Patch layers.json config with the optional styleFile and infoWindowFile parameters
      if "styleFile" in layer:
        styleFile = os.path.join(os.path.dirname(configfile.name), layer["styleFile"])
        if os.path.exists(styleFile):
          with open(styleFile) as f:
            layer["style"] = json.load(f)
          del layer["styleFile"]

      if "infoWindowFile" in layer:
        if "style" in layer:
          infoWindowFile = os.path.join(os.path.dirname(configfile.name), layer["infoWindowFile"])
          if os.path.exists(infoWindowFile):
            with open(infoWindowFile) as f:
              layer["style"]["featureInfo"]["content"] = f.read()
        del layer["infoWindowFile"]

      # Create layer
      layer = create_layer(ctx, layer)
      ctx.log("Layer '%s' created with id %s" % (layer['name'], layer['id']))

      # Poll until asset has processed
      poll_asset_processing(ctx, layer['id'], ctx.service.layers())


def hodor_uploader(ctx, asset_type, configfile):
  """
  Core uploader functionality for anything that adopts the
  Hodor-esque configstore system.

  Parameters
  ----------
  ctx : Context
    A Click Context object
  asset_type : str
    The GME asset type represented. Possible values: vector, raster
  configstore : Click.File | Click.Path
    A pointer to the store of one or more asset configuration files.

  Returns
  -------
  dict
    An object representing the asset in GME.
  """
  @retries(10)
  def create_asset(ctx, resource, config):
    return resource.upload(body=config).execute()

  # Init resource
  resource = ctx.service.tables() if asset_type == "vector" else ctx.service.rasters()

  # Fetch payload files
  config = json.load(configfile)
  payloaddir = os.path.join(os.path.dirname(os.path.abspath(configfile.name)), "payload")
  if os.path.isdir(payloaddir):
    for (dirpath, dirnames, filenames) in walk(payloaddir):
      config['files'] = [{'filename': f} for f in filenames if f != ".DS_Store"]
      break
  else:
  # Backwards compatibility for Aaron who was supplying the files in the config already
    payloaddir = os.path.dirname(configfile.name)

  # Create skeleton asset
  response = create_asset(ctx, resource, config)
  ctx.log("Table '%s' created with id %s" % (response['name'], response['id']))

  # Upload the payload files
  start_time = time.time()
  for i in config['files']:
    upload_file(ctx, response['id'], asset_type, os.path.join(payloaddir, i['filename']), ctx.chunk_size)
  ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

  # Poll until asset has processed
  return poll_asset_processing(ctx, response['id'], resource)
