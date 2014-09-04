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
@click.argument('configfile', type=click.File())
@pass_context
def runjob(ctx, configfile):
  """
    And on the pedestal these words appear:
    'My name is Ozymandias, king of kings:
    Look on my works, ye Mighty, and despair!'
    Nothing beside remains. Round the decay
    Of that colossal wreck, boundless and bare
    The lone and level sands stretch far away.
      http://en.wikipedia.org/wiki/Ozymandias
  """

  config = json.load(configfile)

  datasource_name_part = config["title"] + "_" + config['custodian']
  if "partNumber" in config and "partCount" in config:
    datasource_name_part += "_" + config["partNumber"] + "_of_" + config["partCount"]

  response = ctx.service.tables().list(projectId=config["projectId"], search=datasource_name_part.replace("_", " ")).execute()

  valid_tables = []
  for t in response["tables"]:
    m = re.search("^(" + datasource_name_part + "_[0-9]{8})$", t["name"])
    if m and "archive" not in t["tags"] and t["processingStatus"] == "complete":
      valid_tables.append(t)

  if len(valid_tables) != 1:
    raise Exception("Found " + str(len(valid_tables)) + " matching tables. Gave up - we require a single matching table.")
  else:
    table = valid_tables[0]
    layers = ctx.service.tables().parents().list(id=table["id"]).execute()["parents"]
    ctx.log("Found table %s (%s)" % (table["name"], table["id"]))

    # Step 1 - Create a temporary table.
    ctx.log("### Create Temporary Table")
    temp_table_id = createTemporaryTable(ctx, table)

    # Step 2 - Point our target table's layers at the temporary table.
    ctx.log("### Patch Layers")
    patchLayers(ctx, layers, temp_table_id)

    # Step 3 - Upload our payload files to replace the data in our table.
    ctx.log("### Replace Files")
    replaceFiles(ctx, table["id"], os.path.join(os.path.dirname(configfile.name), "payload"))

    # Step 4 - Point our layers back at the proper table.
    ctx.log("### Patch Layers")
    patchLayers(ctx, layers, table["id"])

    # Step 5 - Delete the temporary table.
    ctx.log("### Delete Temporary Table")
    deleteTable(ctx, temp_table_id)

    # Step 6 - Reprocess and republish the layers.
    ctx.log("### Reprocess & Republish Layers")
    reprocessAndRepublishLayers(ctx, layers)

    ctx.log("### Faux Table Replace Complete. Have a nice day.")


def createTemporaryTable(ctx, table):
  """Create an empty table with a schema
  matching the table provided.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  table : dict
    A GME Table object.

  Returns
  -------
  int
    The Id of the temporary table.
  """

  @retries(10)
  def get_table_schema(table):
    return ctx.service.tables().get(id=table['id'], fields="schema").execute()["schema"]

  @retries(10)
  def create_table(config):
    return ctx.service.tables().create(body=config).execute()

  schema = get_table_schema(table)

  table = create_table({
    "projectId": table["projectId"],
    "name": table["name"] + "_ftrtemp",
    "schema": {
      "columns": [v for v in schema["columns"] if v["name"] != "gx_id"],
      "primaryKey": schema["primaryKey"] if schema["primaryKey"]  != "gx_id" else None
    }
  })
  ctx.log("Created temporary table " + table["id"])
  return table["id"]


def patchLayers(ctx, layers, table_id):
  """Perform a patch operation on the layers associated
  with a table to point them at another table.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  layers : list
    The Ids of the layers to patch.
  table_id : int
    The Id of the table the layers will point to.
  """

  @retries(10)
  def patch(ctx, layer_id, body):
    return ctx.service_exp2.layers().patch(id=layer_id, body=body).execute()

  for l in layers:
    patch(ctx, l["id"], {
      "datasources": [
        {"id": table_id}
      ]
    })
    ctx.log("Layer %s successfully patched." % (l["id"]))


def replaceFiles(ctx, table_id, payload_dir):
  """Reupload a given set of files to an existing table for reprocessing.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  table_id : int
    The Id of the table to upload to.
  payload_dir : str
    The path of the payload directory containing the files.
  """

  # @TODO Generic timing code
  # @TODO Generic polling decerator

  @retries((180 * 60) / 10, delay=10, backoff=1)
  def poll_asset_processing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Table processing status is '%s'" % (response["processingStatus"]))

  # Fetch the payload files
  config = {}
  for (dirpath, dirnames, filenames) in os.walk(payload_dir):
    config['files'] = [{'filename': f} for f in filenames if f != ".DS_Store"]
    break

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
  elif response["processingStatus"] == "failed":
    ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
    raise Exception("Table failed to process.")


@retries(10)
def deleteTable(ctx, table_id):
  """Deletes a given table.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  table_id : int
    The Id of the table.
  """

  ctx.service.tables().delete(id=table_id).execute()
  ctx.log("Table %s successfully deleted." % (table_id))


def reprocessAndRepublishLayers(ctx, layers):
  """Reprocess and republish the given set of layers.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  layers : list
    The Ids of the layers to patch.
  """

  @retries(100, delay=5)
  def processLayer(ctx, layer_id):
    return ctx.service.layers().process(id=layer_id).execute()

  @retries(100, delay=5)
  def publishLayer(ctx, layer_id):
    return ctx.service.layers().publish(id=layer_id).execute()

  @retries(100, delay=10, backoff=1)
  def poll_asset_processing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['processingStatus'] in ['complete', 'failed']:
      return response
    else:
      raise Exception("Layer processing status is '%s'" % (response["processingStatus"]))

  @retries(100, delay=10, backoff=1)
  def poll_asset_publishing(ctx, resource, assetId):
    response = resource.get(id=assetId).execute()
    if response['publishingStatus'] == 'published':
      return response
    else:
      raise Exception("Layer publishing status is '%s'" % (response["publishingStatus"]))


  for l in layers:
    processLayer(ctx, l["id"])
    ctx.log("Layer %s processesing begun." % (l["id"]))

    # Poll until asset has processed
    start_time = time.time()
    response = poll_asset_processing(ctx, ctx.service.layers(), l["id"])
    if response["processingStatus"] == "complete":
      ctx.log("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

      publishLayer(ctx, l["id"])
      ctx.log("Layer %s publishing begun." % (l["id"]))

      # Poll until asset has published
      start_time = time.time()
      response = poll_asset_publishing(ctx, ctx.service.layers(), l["id"])
      ctx.log("Publishing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

    elif response["processingStatus"] == "failed":
      ctx.vlog("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
      raise Exception("Layer failed to process")
