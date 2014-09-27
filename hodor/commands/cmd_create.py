import click
import json
import os
import time
from hodor.gme import *
from pprintpp import pprint as pp
from os import walk
from retries import retries
from hodor.cli import pass_context

# @TODO Implement multithreading processing:
#   * Using Pool.apply_async()
#   * Just continue to dump logs to stdout at the moment
#   * How to handle multithreading uploading?
#   * We only need ctx's methods - so don't pass, just import?

@click.group(short_help="Create assets in Google Maps Engine")
# https://google-api-python-client.googlecode.com/hg/docs/epy/apiclient.http.MediaFileUpload-class.html
@click.option('--chunk-size', default=20971520,
              help='File chunk size in bytes. Defaults to chunks of 20mb. Pass -1 to use native streaming in Python instead.')
@click.option('--asset-processing-wait', default=90,
              help='Length of time in minutes we should wait for an asset to process before giving up. Defaults to 90 minutes.')
@pass_context
def cli(ctx, chunk_size, asset_processing_wait):
  ctx.chunk_size = chunk_size
  ctx.processing_timeout_mins = asset_processing_wait


@cli.command()
@click.option('--mosaic-id',
              help="The raster mosaic to add the newly created raster(s) to.")
@click.option('--process-mosaic/--no-process-mosaic', default=False,
              help="Whether the given raster mosaic should be processed after the raster(s) are added. Defaults to false.")
@click.option('--layer-id',
              help="The raster layer to add the newly created raster(s) to. Not compatible with mosaic options.")
@click.argument('configstore', type=click.Path(exists=True, resolve_path=True))
@pass_context
def raster(ctx, mosaic_id, process_mosaic, layer_id, configstore):
  """
  Create a new raster asset in Google Maps Engine

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  configstore : Click.Path
    A pointer to the store of one or more asset configuration files.
  """

  rasters = hodor_uploader(ctx, Asset.RASTER, configstore)

  for raster in rasters:
    # Optionally, add it to an existing raster mosaic
    if mosaic_id is not None:
      ctx.service().rasterCollections().rasters().batchInsert(id=mosaic_id, body={"ids": [raster["id"]]}).execute()
      ctx.log("Asset '%s' added to mosaic '%s'" % (raster["id"], mosaic_id))

      if process_mosaic == True:
        ctx.service().rasterCollections().process(id=mosaic_id).execute()
        ctx.log("Mosaic '%s' processing started" % (mosaic_id))

        # Poll until raster has processed
        poll_asset_processing(ctx, mosaic_id, ctx.service().rasterCollections())

    # Optionally, add it to an existing layer
    elif layer_id is not None:
      layer = ctx.service().layers().get(id=layer_id).execute()

      if layer['datasourceType'] != "image":
        raise Exception("Layer datasourceType is not 'image'.")
      if len(layer['datasources']) >= 100:
        raise Exception("The GME API currently only allows us to patch layers <= 100 datasources. Sad Keanu :(")

      ctx.service().layers().patch(id=layer_id, body={
        "datasources": layer['datasources'] + [raster["id"]]
      }).execute()
      ctx.log("Asset '%s' added to layer '%s'" % (raster["id"], layer_id))


@cli.command()
@click.argument('configstore', type=click.Path(exists=True, resolve_path=True))
@pass_context
def vector(ctx, configstore):
  """
  Create a new vector asset in Google Maps Engine

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  configstore : Click.Path
    A pointer to the store of one or more asset configuration files.
  """

  vectors = hodor_uploader(ctx, Asset.TABLE, configstore)


def hodor_uploader(ctx, asset_type, configstore):
  """
  Core uploader functionality for anything that adopts the
  Hodor-esque configstore system.

  Parameters
  ----------
  ctx : Context
    A Click Context object
  asset_type : int
    A GME asset type defined by the Asset class.
  configstore : Click.Path
    A pointer to the store of one or more asset configuration files.

  Returns
  -------
  list
    A list of assetIds for the assets created.
  """
  @retries(10)
  def create_asset(ctx, resource, config):
    return resource.upload(body=config).execute()

  resource = get_asset_resource(ctx.service(), asset_type)

  # Process all available configfiles
  asset_configs = hodor_config_builder(configstore)
  asset_ids = []
  for asset_dir, configs in asset_configs.iteritems():
    payloaddir = os.path.join(asset_dir, "payload")

    for config in configs:
      # Create skeleton asset
      response = create_asset(ctx, resource, config)
      ctx.log("Asset '%s' created with id %s" % (response['name'], response['id']))

      # Upload each of the payload files in a separate thread
      filepaths = [os.path.join(payloaddir, f['filename']) for f in config['files']]
      start_time = time.time()
      upload_files_multithreaded(ctx, response['id'], asset_type, filepaths, chunk_size=ctx.chunk_size)
      ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

      # Poll until asset has processed
      poll_asset_processing(ctx, response['id'], resource)

      # Optionally, create any layers the user has provided config for.
      if os.path.isfile(os.path.join(asset_dir, "layers.json")):
        layer_creator(ctx, response, os.path.join(asset_dir, "layers.json"))

      asset_ids.append(response["id"])
  return asset_ids


def hodor_config_builder(configstore):
  """
  Manages initialisation of the Hodor-esque config system.

  Parameters
  ----------
  configstore : Click.Path
    A pointer to the store of one or more asset configuration files.

  Returns
  -------
  dict
    An object representing the config files used to create the asset(s).
  {
    '/path/to/asset1': [{..asset config...}],
    '/path/to/asset2': [{..asset config...}]
  }
  """

  # Fetch all configfiles in the configstore
  configfiles = []
  if os.path.basename(configstore) == "config.json":
    # Just a single config.json file
    configfiles.append(configstore)
  else:
    # Recursively walk the directory tree finding all config.json files
    for (dirpath, dirnames, filenames) in walk(configstore):
      for f in filenames:
        if f == "config.json":
          configfiles.append(os.path.join(dirpath, f))

  # Create GME asset config blocks for all available assets
  # in the available configfiles
  assets = {}
  for cf in configfiles:
    with open(cf, "r") as f:
      config = json.load(f)

    # Handle bulk loads where the use has multiple files of the same
    # type in their payload dir. e.g.
    # /payload
    #   file1.tif
    #   file1.tfw
    #   file2.tif
    #   file2.tfw
    # Would be treated as two assets: file1 and file2
    if "name" in config and config["name"] == "{fileName}":
      # Group all payload files by their base filename.
      # NB: This fails for files with two extensions e.g. file.tif.tfw
      payloaddir = os.path.join(os.path.dirname(cf), "payload")
      grouped_payload = {}
      for (dirpath, dirnames, filenames) in walk(payloaddir):
        for f in filenames:
          basename = os.path.splitext(f)[0]
          if basename not in grouped_payload:
            grouped_payload[basename] = []
          grouped_payload[basename].append(f)
        break

      for name, payload in grouped_payload.iteritems():
        asset_config = config.copy()
        asset_config["name"] = name
        asset_config["files"] = [{'filename': f} for f in payload]

        asset_dir = os.path.dirname(cf)
        if asset_dir not in assets:
          assets[asset_dir] = []
        assets[asset_dir].append(asset_config)

    # Just a regular single asset upload
    else:
      # Backwards compatibility for Aaron who is supplying the files in the config already
      if "files" not in config:
        payloaddir = os.path.join(os.path.dirname(cf), "payload")
        for (dirpath, dirnames, filenames) in walk(payloaddir):
          config['files'] = [{'filename': f} for f in filenames if f != ".DS_Store"]
          break
      assets[os.path.dirname(cf)] = [config]
  return assets


def layer_creator(ctx, asset, configfile):
  """Creates layers from an asset according to user-supplied config.

  ctx : Context
    A Click Context object.
  asset : dict
    A vector or raster asset in GME.
  configfile : str
    Path to a layers.json file.
  """
  @retries(10)
  def create_layer(ctx, config):
    return ctx.service().layers().create(body=config, process=True).execute()

  @retries(10)
  def publish_layer(ctx, layer_id):
    return ctx.service().layers().publish(id=layer_id).execute()

  with open(configfile, "r") as f:
    config = json.load(f)

  for layer in config["layers"]:
    layer["projectId"] = asset["projectId"]
    layer["datasources"] = [{"id": asset["id"]}]

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
            if "featureInfo" not in layer["style"]:
              layer["style"]["featureInfo"] = {}
            layer["style"]["featureInfo"]["content"] = f.read()
      del layer["infoWindowFile"]

    # Create layer
    layer = create_layer(ctx, layer)
    ctx.log("Layer '%s' created with id %s" % (layer['name'], layer['id']))

    # Process and publish
    poll_asset_processing(ctx, layer['id'], ctx.service().layers())
    publish_layer(ctx, layer['id'])
    poll_layer_publishing(ctx, layer['id'])
