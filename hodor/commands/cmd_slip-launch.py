import click
import os
import json
import random
import tablib
from pprintpp import pprint as pp
from retries import retries
from hodor.cli import pass_context
from apiclient.errors import HttpError
from hodor.gme import getMapLayerIds, getResourceForAsset
from hodor.exceptions import InternalServerError

@click.group(short_help="A collection of random tools for getting things ready for the SLIP launch.")
@pass_context
def cli(ctx):
  pass


@cli.command()
@click.argument('outfile', type=click.File(mode='ra'))
@pass_context
def make_rasters_searchable(ctx, outfile):
  resource = ctx.service().assets()
  request = resource.list(projectId="09372590152434720789", fields="nextPageToken,assets/id,assets/name,assets/type,assets/tags")

  taggedAssets = tablib.Dataset(headers=('assetId', 'type', 'name'))
  taggedAssets.csv = outfile.read()

  while request != None:
    response = request.execute()
    for asset in response["assets"]:
      temp = taggedAssets["assetId"] # This is somehow required to make tablib return a list. We can't evaluate it in an if statement =/
      if asset["id"] in temp:
        continue

      # Skip vector table layers
      if asset["type"] == "layer":
        try:
          layer = ctx.service().layers().get(id=asset["id"], fields="datasourceType").execute()
        except HttpError as e:
          pp(asset)
          raise e

        if "datasourceType" not in layer:
          continue
        if layer["datasourceType"] == "table":
          continue

      if asset["type"] in ["raster", "rasterCollection", "layer"]:
        # pp(asset)
        # pp(asset["tags"])
        if "notsearchable" not in asset["tags"]:
          continue
        del asset["tags"][asset["tags"].index("notsearchable")]
        # pp(asset["tags"])

        assetResource = getResourceForAsset(ctx.service(), asset["type"])
        assetResource.patch(id=asset["id"], body={
          "tags": asset["tags"]
        }).execute()

        taggedAssets.append([asset["id"], asset["type"], asset["name"]])
        with open(outfile.name, "w") as f:
          f.write(taggedAssets.csv)

        ctx.log("%s (%s - %s) made searchable" % (asset["id"], asset["name"], asset["type"]))

    request = resource.list_next(request, response)


@cli.command()
@click.argument('outfile', type=click.File(mode='ra'))
@click.argument('errfile', type=click.File(mode='ra'))
@pass_context
def tag_all_notapproved(ctx, outfile, errfile):
  def erroredAsset():
    erroredAssets.append([asset["id"], asset["type"], asset["name"]])
    with open(errfile.name, "w") as f:
      f.write(erroredAssets.csv)

  resource = ctx.service().assets()
  request = resource.list(projectId="09372590152434720789", fields="nextPageToken,assets/id,assets/name,assets/type,assets/tags")

  taggedAssets = tablib.Dataset(headers=('assetId', 'type'))
  taggedAssets.csv = outfile.read()

  erroredAssets = tablib.Dataset(headers=('assetId', 'type', 'name'))
  erroredAssets.csv = errfile.read()

  while request != None:
    response = request.execute()
    for asset in response["assets"]:
      if asset["type"] in ["raster", "rasterCollection"]:
        continue

      temp = taggedAssets["assetId"] # This is somehow required to make tablib return a list. We can't evaluate it in an if statement =/
      if asset["id"] in temp:
        continue

      if "notsearchable" in asset["tags"]:
        continue
      asset["tags"].append("notsearchable")

      if len(asset["tags"]) <= 25:
        assetResource = getResourceForAsset(ctx.service(), asset["type"])

        try:
          assetResource.patch(id=asset["id"], body={
            "tags": asset["tags"]
          }).execute()

          taggedAssets.append([asset["id"], asset["type"]])
          with open(outfile.name, "w") as f:
            f.write(taggedAssets.csv)

          # ctx.log("%s (%s) patched" % (asset["id"], asset["name"]))
        except HttpError as e:
          erroredAsset()
          ctx.log("%s (%s) failed patching due to %s" % (asset["id"], asset["name"], e.message))
      else:
        erroredAsset()
        ctx.log("%s (%s) failed patched due to too many tags" % (asset["id"], asset["name"]))

    request = resource.list_next(request, response)


@cli.command()
@click.argument('csvfile', type=click.File('r'))
@pass_context
def remove_tag_from_asset(ctx, csvfile):
  assets = tablib.Dataset(headers=('assetId'))
  assets.csv = csvfile.read()

  for a in assets:
    assetId, assetType, layerName = a

    asset = ctx.service().assets().get(id=assetId, fields="id,type,tags").execute()
    if "notsearchable" not in asset["tags"]:
      continue

    del asset["tags"][asset["tags"].index("notsearchable")]

    assetResource = getResourceForAsset(ctx.service(), asset["type"])
    assetResource.patch(id=asset["id"], body={
      "tags": asset["tags"]
    }).execute()
    ctx.log("%s made searchable" % (asset["id"]))

@cli.command()
@click.argument('outfile', type=click.File(mode='w'))
@pass_context
def get_map_layers_and_tables(ctx, outfile):
  # locateMapId = "09372590152434720789-00913315481290556980"
  sandboxMapId = "09372590152434720789-00440247219122458144"
  map = ctx.service().maps().get(id=sandboxMapId).execute()
  assets = tablib.Dataset(headers=('assetId', 'type', 'layer_name'))

  for layerId in getMapLayerIds(map):
    layer = ctx.service().layers().get(id=layerId, fields="name,datasources,datasourceType").execute()
    assets.append([layerId, "layer", layer["name"]])
    if layer["datasourceType"] == "table":
      for table in layer["datasources"]:
        assets.append([table["id"], "table", layer["name"]])

  with open(outfile.name, "w") as f:
    f.write(assets.csv)

@cli.command()
@click.argument('outfile', type=click.File(mode='w'))
@pass_context
def get_locate_public_tables(ctx, outfile):
  locateMapId = "09372590152434720789-00913315481290556980"
  locate = ctx.service().maps().get(id=locateMapId).execute()
  assets = tablib.Dataset(headers=('assetId', 'type', 'layer_name'))

  @retries(2)
  def getPermissions(table):
    return ctx.service()(version="exp2").tables().permissions().list(id=table["id"]).execute()

  for layerId in getMapLayerIds(locate):
    layer = ctx.service().layers().get(id=layerId, fields="name,datasources,datasourceType").execute()
    if layer["datasourceType"] == "table":
      for table in layer["datasources"]:
        for p in getPermissions(table)["permissions"]:
          if p["type"] == "anyone" and p["role"] == "viewer" and p["discoverable"] is True:
            assets.append([table["id"], "table", layer["name"]])

  with open(outfile.name, "w") as f:
    f.write(assets.csv)


@cli.command()
@click.argument('infile', type=click.File(mode='r'))
@pass_context
def tag_locate_wfs1_service(ctx, infile):
  assets = tablib.Dataset()
  assets.csv = infile.read()

  for t in assets:
    tableId, assetType, layerName = t
    table = ctx.service().tables().get(id=tableId, fields="id,name,tags").execute()

    # Remove trailing whitespace
    for k, tag in enumerate(table["tags"]):
      table["tags"][k] = tag.strip()

    # Strip WFS tags
    tags = []
    for k, tag in enumerate(table["tags"]):
      if not (tag.startswith("ds:") or tag.startswith("wfs")):
        tags.append(tag)

    # Add WFS tags back in
    dsTag = "ds:" + layerName.strip()[:47]
    dsTag = dsTag.replace("(", "").replace(")", "").replace(" ", "_").replace("-", "")

    tags.append("wfs:Locate")
    tags.append(dsTag)

    ctx.service().tables().patch(id=table["id"], body={
      "tags": tags
    }).execute()
    ctx.log("%s (%s) patched with %s" % (tableId, layerName, dsTag))
