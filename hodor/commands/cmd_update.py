import click
import os
import json
import time
import multiprocessing
from retries import retries
from hodor.cli import pass_context
from hodor.gme import obey_qps

# @TODO Work out why it gets new services so often in the threads. Are threads dying? Am I understanding how ctx is being transferred to the threads?
# @TODO Sent request timings back to the parent process to calculate percentiles

# cd Documents/Work/GitHub/Hodor
# . venv/bin/activate
# hodor update --table-id=06151154151057343427-13941782100256261257 test-data/land_address2_incr_20140627_20140711/deltas/

@click.command('update', short_help='Apply a set of changefiles against a vector table asset.')
@click.option('--table-id', type=str)
@click.option('--processes', default=5,
              help='The number of threads to spin up. Defaults to 5 to obey our QPS limit in GME.')
@click.argument('payloaddir', type=click.Path(exists=True, file_okay=False, resolve_path=True))
@pass_context
def cli(ctx, table_id, processes, payloaddir):
  # Apply updates
  batchRequests(ctx, table_id, processes, payloaddir, "batchPatch")

  # Apply deletes

  # Apply additions


def batchRequests(ctx, table_id, processes, payloaddir, operation):
  deltafiles = {"batchPatch": "updates.json", "batchInsert": "adds.json", "batchDelete": "deletes.json"}
  with open(os.path.join(payloaddir, deltafiles[operation])) as f:
    features = json.load(f)
  chunks = [(features["features"][i:i+50], ctx, operation, table_id, i) for i in range(0, len(features["features"]), 50)]

  start_time = time.time()

  pool = multiprocessing.Pool(processes=processes)
  stuff = pool.map(batchRequestsThread, chunks)
  pool.close()
  pool.join()

  elapsed_secs = time.time() - start_time
  ttl_features = len(features["features"])
  features_per_sec = int(ttl_features / elapsed_secs)
  ctx.log("%s features pushed in %s mins (%s features/second)" % ("{:,}".format(ttl_features), round(elapsed_secs / 60, 2), features_per_sec))


def batchRequestsThread(blob):
  @obey_qps()
  @retries(10, delay=0.25, backoff=0.25)
  def request(resource, table_id, chunk):
    resource(id=table_id, body={"features": chunk}).execute()

  chunk, ctx, operation, table_id, start_index = blob

  # Make features GME-safe
  for f in chunk:
    # Ignore geometry for now - we'd have to fix GME's counter-winding geom thing
    del f["geometry"]

    # Fix for GME wanting integers as strings
    for p in f["properties"]:
      if isinstance(f["properties"][p], int):
        f["properties"][p] = str(f["properties"][p])

  start_time = time.time()

  batchOperation = getattr(ctx.service(ident=multiprocessing.current_process().ident).tables().features(), operation)

  start_time = time.time()
  request(batchOperation, table_id, chunk)
  ctx.log("Processed Features %s - %s in %ss." % (start_index, start_index + 50, round(time.time() - start_time, 2)))
