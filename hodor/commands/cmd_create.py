import click
import json
import os
import time
from pprintpp import pprint as pp
from os import walk
from retries import retries
from hodor.cli import pass_context

@click.group(short_help="Create assets in Google Maps Engine")
# https://google-api-python-client.googlecode.com/hg/docs/epy/apiclient.http.MediaFileUpload-class.html
@click.option('--chunk-size', default=10485760,
              help='File chunk size in bytes. Defaults to using streaming to send files.')
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
              help="The raster layer to add the newly created layer to.")
@click.argument('configfile', type=click.File('r'))
@pass_context
def raster(ctx, mosaic_id, process_mosaic, layer_id, configfile):
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
    ctx.service.rasterCollections().rasters().batchInsert(id=mosaic_id, body={"ids": [asset["id"]]}).execute()
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

  # Optionally, add it to an existing layer
  if layer_id is not None:
    layer = ctx.service.layers().get(id=layer_id).execute()

    if layer['datasourceType'] != "image":
      raise Exception("Layer datasourceType is not 'image'.")
    if len(layer['datasources']) >= 100:
      raise Exception("The GME API currently only allows us to patch layers <= 100 datasources.")

    ctx.service.layers().patch(id=layer_id, body={
      "datasources": layer['datasources'] + [asset["id"]]
    }).execute()
    ctx.log("Asset '%s' added to layer '%s'" % (asset["id"], layer_id))


@cli.command()
@click.option('--layer-configfile', type=click.File('r'),
              help="An optional JSON configuration file containing layer(s) to create from the new table.")
@click.argument('configfile', type=click.File('r'))
@pass_context
def vector(ctx, layer_configfile, configfile):
  """Create a new vector asset in Google Maps Engine"""
  asset = uploader(ctx, ctx.service.tables(), configfile)

  if layer_configfile:
    create_vector_layers(ctx, asset["id"], layer_configfile)


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
  payloaddir = os.path.join(os.path.dirname(configfile.name), "payload")
  if os.path.isdir(payloaddir):
    for (dirpath, dirnames, filenames) in walk(payloaddir):
      config['files'] = [{'filename': f} for f in filenames if f != ".DS_Store"]
      break
  else:
  # Backwards compatibility for Aaron who was supplying the files in the config already
    payloaddir = os.path.dirname(configfile.name)

  # Create asset and upload payload files
  response = create_asset(resource, config)
  ctx.log("Table '%s' created with id %s" % (response['name'], response['id']))

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


def create_vector_layers(ctx, assetId, configfile):
  @retries(10)
  def create_layer(ctx, config):
    return ctx.service.layers().create(body=config, process=True).execute()

  @retries((ctx.processing_timeout_mins * 60) / 10, delay=10, backoff=1)
  def poll_asset_processing(ctx, assetId):
    response = ctx.service.layers().get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Asset processing status is '%s'" % (response["processingStatus"]))

  config = json.load(configfile)

  for layer in config["layers"]:
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
    layer["projectId"] = config["projectId"]
    layer["datasources"] = [{"id": assetId}]
    asset = create_layer(ctx, layer)
    ctx.log("Layer '%s' created with id %s" % (asset['name'], asset['id']))

    # Poll until asset has processed
    start_time = time.time()
    response = poll_asset_processing(ctx, asset['id'])
    if response["processingStatus"] == "complete":
      ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    elif response["processingStatus"] == "failed":
      ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
      raise Exception("Asset failed to process")
