import click
import time
from retries import retries
from hodor.cli import pass_context

@click.group()
@pass_context
def cli(ctx):
  pass

@cli.command('list', short_help='List accessible Google Maps Engine assets.')
@click.option('-projectId', type=str, help='The GME projectId to query. By default all accessible projects are queried.')
@click.option('-type', type=str, help='An asset type restriction.')
@pass_context
def list(ctx, projectid, type):
  resource = ctx.service.assets()
  request = resource.list(projectId=projectid, type=type)

  while request != None:
    request_start_time = time.time()
    response = request.execute()
    request_elapsed_time = time.time() - request_start_time

    # Obey GME's QPS limits
    nap_time = max(0, 1 - request_elapsed_time)
    time.sleep(nap_time)

    for asset in response['assets']:
     click.echo("%s (%s)" % (asset["name"], asset["id"]))

    request = resource.list_next(request, response)
