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
from hodor.gme import upload_file, poll_asset_processing, poll_layer_publishing

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

  resource = ctx.service.tables()
  request = resource.list(projectId=config["projectId"], search=datasource_name_part.replace("_", " "))
  tables = []

  while request != None:
    response = request.execute()
    tables += response["tables"]
    request = resource.list_next(request, response)

  valid_tables = []
  for t in tables:
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

  # Fetch the payload files
  config = {}
  for (dirpath, dirnames, filenames) in os.walk(payload_dir):
    config['files'] = [{'filename': f} for f in filenames if f != ".DS_Store"]
    break

  # Upload the payload files
  start_time = time.time()
  for i in config['files']:
    upload_file(ctx, table_id, "vector", os.path.join(payload_dir, i['filename']))
  ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

  # Poll until asset has processed
  poll_asset_processing(ctx, table_id, ctx.service.tables())


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

  for l in layers:
    ctx.log("Layer %s processesing begun." % (l["id"]))
    processLayer(ctx, l["id"])
    poll_asset_processing(ctx, l["id"], ctx.service.layers())

    ctx.log("Layer %s publishing begun." % (l["id"]))
    publishLayer(ctx, l["id"])
    poll_layer_publishing(ctx, l["id"])


@cli.command()
@click.argument('configdir', type=click.Path())
@pass_context
def testJSON(ctx, configdir):
  """Temporary function to evaluate the correctness of
  our table searching code."""
  from termcolor import colored

  def list_tables(ctx, request):
    time.sleep(1)
    return request.execute()

  configfiles = []
  for (dirpath, dirnames, filenames) in os.walk(configdir):
    configfiles += [os.path.join(dirpath, f) for f in filenames if ".json" in f and "_style.json" not in f]

  for configfile in configfiles:
    with open(configfile, "r") as f:
      config = json.load(f)

    if "title" not in config:
      print colored("%s: Error! Invalid configfile!" % (configfile), 'red')
      continue

    datasource_name_part = config["title"] + "_" + config['custodian']
    if "partNumber" in config and "partCount" in config:
      datasource_name_part += "_" + str(config["partNumber"]) + "_of_" + str(config["partCount"])

    resource = ctx.service.tables()
    request = resource.list(projectId="09372590152434720789", search=datasource_name_part.replace("_", " "))
    tables = []

    while request != None:
      response = list_tables(ctx, request)
      tables += response["tables"]
      request = resource.list_next(request, response)

    valid_tables = []
    for t in tables:
      m = re.search("^(" + datasource_name_part + "_[0-9]{8})$", t["name"])
      if m and "archive" not in t["tags"] and t["processingStatus"] == "complete":
        valid_tables.append(t)

    if len(valid_tables) == 1:
      print "%s: %s" % (configfile, len(valid_tables))
    else:
      print colored("%s: %s" % (configfile, len(valid_tables)), 'red')


@cli.command()
@click.argument('hodordir', type=click.Path(resolve_path=True))
@click.argument('configdir', type=click.Path(resolve_path=True))
@pass_context
def createBATFiles(ctx, hodordir, configdir):
  """Temporary function to create BAT files for running Faux Tabel Replace"""
  configfiles = {}
  for (dirpath, dirnames, filenames) in os.walk(configdir):
    configfiles = [os.path.join(dirpath, f) for f in filenames if ".json" in f and "_style.json" not in f]

  for configfile in configfiles:
    batfile = configfile.replace(".json", ".bat")
    with open(batfile, "w") as f:
      f.write("""@echo off
REM Automatically generated Hodor BAT file.
%s
hodor faux-table-replace runjob %s > %s
""" % (
        os.path.join(hodordir, "venv", "Scripts", "activate.bat"),
        configfile,
        configfile.replace(".json", ".log")
      ))
    exit()
