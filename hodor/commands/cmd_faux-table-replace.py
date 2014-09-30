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
from apiclient.errors import HttpError
from hodor.gme import upload_file, upload_file_init, upload_files_multithreaded, poll_asset_processing, poll_layer_publishing

@click.group(short_help="For performing faux table replace in GME whereby datasources an be updated without altering assetIds.")
@pass_context
def cli(ctx):
  pass


@cli.command()
@click.option('--skip-upload/--no-skip-upload', default=False,
              help="Whether uploading should be skipped. Defaults to false.")
@click.argument('configfile', type=click.File())
@pass_context
def runjob(ctx, skip_upload, configfile):
  """
    And on the pedestal these words appear:
    'My name is Ozymandias, king of kings:
    Look on my works, ye Mighty, and despair!'
    Nothing beside remains. Round the decay
    Of that colossal wreck, boundless and bare
    The lone and level sands stretch far away.
      http://en.wikipedia.org/wiki/Ozymandias
  """
  @retries(100)
  def searchTablesPaging(ctx, request):
    return request.execute()

  @retries(100)
  def getTableParents(ctx, table_id):
    return ctx.service().tables().parents().list(id=table_id).execute()

  def doesTempTableExist(ctx, table, config):
    request = resource.list(projectId=config["projectId"], search=table["name"].replace("_", " "), tags="ftrtemp")
    temp_tables = searchTablesPaging(ctx, request)
    for t in temp_tables["tables"]:
      if t["name"] == table["name"] + "_ftrtemp":
        return t["id"]
    return False

  ctx.log("### Begin Faux Table Replace for %s" % (configfile.name))
  start_time = time.time()
  payload_dir = os.path.join(os.path.dirname(configfile.name), "payload")
  config = json.load(configfile)

  if "title" not in config or "custodian" not in config:
    raise Exception("Invalid JSON configuration file.")

  datasource_name_part = config["title"] + "_" + config['custodian']
  if "partNumber" in config and "partCount" in config:
    datasource_name_full_sans_date = datasource_name_part + "_" + str(config["partNumber"]) + "_of_" + str(config["partCount"])

  resource = ctx.service().tables()
  request = resource.list(projectId=config["projectId"], search=datasource_name_part.replace("_", " "))
  tables = []

  while request != None:
    response = searchTablesPaging(ctx, request)
    tables += response["tables"]
    request = resource.list_next(request, response)

  valid_tables = []
  for t in tables:
    m = re.search("^(" + datasource_name_full_sans_date + "_[0-9]{8})$", t["name"])
    if m and "archive" not in t["tags"]: # and t["processingStatus"] in ["complete", "failed", "processing", "notReady", "ready"]:
      valid_tables.append(t)

  if len(valid_tables) != 1:
    ctx.log("### Error: Found " + str(len(valid_tables)) + " matching tables. Gave up - we require a single matching table.")
  else:
    table = valid_tables[0]
    layers = getTableParents(ctx, table["id"])["parents"]
    ctx.log("Found table %s (%s)" % (table["name"], table["id"]))

    # Validate payload files
    payload_files = []
    for (dirpath, dirnames, filenames) in os.walk(payload_dir):
      payload_files = [f for f in filenames if f != ".DS_Store"]
      break

    if len(payload_files) == 0:
      ctx.log("### Error: No files found in the payload directory.")
      return

    table_files = [f["filename"] for f in table["files"]]

    if set(payload_files) != set(table_files):
      ctx.log("### Error: The files supplied do not match the names/types of the files in the asset.")
      return

    # Step 0 - Recover from failed uploads
    temp_table_id = doesTempTableExist(ctx, table, config)
    if temp_table_id != False:
      ctx.log("### Temporary table already exists. Skipping its creation and patching of layers.")
      layers = getTableParents(ctx, temp_table_id)["parents"]
    else:
      # Step 1 - Create a temporary table.
      ctx.log("### Create Temporary Table")
      temp_table_id = createTemporaryTable(ctx, table)

      # Step 2 - Point our target table's layers at the temporary table.
      ctx.log("### Patch Layers")
      patchLayers(ctx, layers, temp_table_id)

    # Step 3 - Upload our payload files to replace the data in our table.
    if skip_upload == False:
      ctx.log("### Replace Files")
      replaceFiles(ctx, table["id"], payload_dir)
    else:
      ctx.log("### Replace Files - Skipped as per --skip-upload")

    # Step 4 - Point our layers back at the proper table.
    ctx.log("### Patch Layers")
    patchLayers(ctx, layers, table["id"])

    # Step 5 - Delete the temporary table.
    ctx.log("### Delete Temporary Table")
    deleteTable(ctx, temp_table_id)

    # Step 6 - Reprocess and republish the layers.
    ctx.log("### Reprocess & Republish Layers")
    reprocessAndRepublishLayers(ctx, layers)

    ctx.log("### Faux Table Replace Complete (%s mins). Have a nice day." % (round((time.time() - start_time) / 60, 2)))


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

  @retries(100)
  def get_table_schema(ctx, table):
    return ctx.service().tables().get(id=table['id'], fields="schema").execute()["schema"]

  @retries(100)
  def create_table(ctx, config):
    return ctx.service().tables().create(body=config).execute()

  @retries(100)
  def patch_table(ctx, table_id, config):
    return ctx.service().tables().patch(id=table_id, body=config).execute()

  schema = get_table_schema(ctx, table)

  table = create_table(ctx, {
    "projectId": table["projectId"],
    "name": table["name"] + "_ftrtemp",
    "tags": [
      "ftrtemp"
    ],
    "schema": {
      "columns": [v for v in schema["columns"] if v["name"] != "gx_id"],
      "primaryKey": schema["primaryKey"] if schema["primaryKey"]  != "gx_id" else None
    }
  })
  patch_table(ctx, table["id"], {
    "tags": [
      "ftrtemp"
    ]
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

  @retries(100)
  def patch(ctx, layer_id, body):
    return ctx.service(version="exp2").layers().patch(id=layer_id, body=body).execute()

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
  print "replaceFiles"
  # Fetch the payload files
  config = {}
  for (dirpath, dirnames, filenames) in os.walk(payload_dir):
    # config['files'] = [{'filename': f} for f in filenames if f != ".DS_Store"]
    filepaths = [os.path.join(payload_dir, f) for f in filenames if f != ".DS_Store"]
    break

  # Upload the payload files in separate threads
  pp(filepaths)
  start_time = time.time()
  upload_files_multithreaded(ctx, table_id, "vector", filepaths, chunk_size=20971520)
  ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
  exit()

  # # Hacky workaround that doesn't use multi-threading.
  # # Force all asset files into "uploading" state by providing the first 256KB of each file.
  # for i in config['files']:
  #   upload_file_init(ctx, table_id, "vector", os.path.join(payload_dir, i['filename']))
  # ctx.log("Bailing!")
  #
  # # Upload the payload files
  # start_time = time.time()
  # for i in config['files']:
  #   upload_file(ctx, table_id, "vector", os.path.join(payload_dir, i['filename']), chunk_size=20971520)
  # ctx.log("All uploads completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

  # Poll until asset has processed
  poll_asset_processing(ctx, table_id, ctx.service().tables())


def deleteTable(ctx, table_id):
  """Deletes a given table.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  table_id : int
    The Id of the table.
  """

  @retries(100)
  def delete(ctx, table_id):
    return ctx.service().tables().delete(id=table_id).execute()

  try:
    delete(ctx, table_id)
    ctx.log("Table %s successfully deleted." % (table_id))
  except HttpError as e:
    if e.resp.status == 404:
      ctx.log("Table %s has already been deleted." % (table_id))
    else:
      raise e


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
    try:
      return ctx.service().layers().process(id=layer_id).execute()
    except HttpError as e:
      # "The resource is already up to date."
      if e.resp.status == 409:
        print e.content
        ctx.log("### Layer was already processed successfully for some reason!")
        return
      else:
        raise e

  @retries(100, delay=5)
  def publishLayer(ctx, layer_id):
    return ctx.service().layers().publish(id=layer_id).execute()

  for l in layers:
    ctx.log("Layer %s processesing begun." % (l["id"]))
    processLayer(ctx, l["id"])
    poll_asset_processing(ctx, l["id"], ctx.service().layers())

    ctx.log("Layer %s publishing begun." % (l["id"]))
    publishLayer(ctx, l["id"])
    poll_layer_publishing(ctx, l["id"])


@cli.command()
@click.argument('configdir', type=click.Path(resolve_path=True))
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

    resource = ctx.service().tables()
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
  configfiles = []
  for (dirpath, dirnames, filenames) in os.walk(configdir):
    configfiles += [os.path.join(dirpath, f) for f in filenames if ".json" in f and "_style.json" not in f]

  for configfile in configfiles:
    batfile = configfile.replace(".json", ".bat")
    print batfile
    with open(batfile, "w") as f:
      f.write("""@echo off
REM Automatically generated Hodor BAT file.
echo Running %s
cd E:\Sarlaadeeza\Hodor
call venv\\Scripts\\activate.bat
hodor faux-table-replace runjob %s > %s
pause
""" % (
        batfile,
        configfile,
        configfile.replace(".json", ".log")
      ))





@cli.command()
@click.argument("table_id", type=str)
@click.argument("ftrtemp_table_id", type=str)
@pass_context
def rollback_table_patch(ctx, table_id, ftrtemp_table_id):
  """Rollsback the temporary table portion of the Faux Table Replace
  process.

  Parameters
  ----------
  ctx : Context
    A Click Context object.
  table_id : str
    TableId of the production table.
  ftrtemp_table_id : str
    TableId of the temporary faux table replace table.
  """

  @retries(100)
  def getTable(ctx, table_id):
    return ctx.service().tables().get(id=table_id).execute()

  @retries(100)
  def getTableParents(ctx, table_id):
    return ctx.service().tables().parents().list(id=table_id).execute()

  # Lazy validation that they both exist.
  table = getTable(ctx, table_id)
  ftrtable = getTable(ctx, ftrtemp_table_id)

  layers = getTableParents(ctx, ftrtemp_table_id)["parents"]
  if len(layers) == 0:
    raise Exception("Found no layers attached to Faux Table Replace table.")
  else:
    # Step 1 - Point our layers back at the proper table.
    ctx.log("### Patch layers")
    patchLayers(ctx, layers, table_id)

    # Step 2 - Delete the temporary table.
    ctx.log("### Delete Temporary Table")
    deleteTable(ctx, ftrtemp_table_id)

    ctx.log("### Fin")
