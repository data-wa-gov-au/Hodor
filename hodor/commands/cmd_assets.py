import click
import time
from retries import retries
from hodor.cli import pass_context
from hodor.gme import obey_qps

@click.group()
@pass_context
def cli(ctx):
  pass


@cli.command('list', short_help='List accessible Google Maps Engine assets.')
@click.option('-projectId', type=str, help='The GME projectId to query. By default all accessible projects are queried.')
@click.option('-type', type=str, help='An asset type restriction.')
@pass_context
def list(ctx, projectid, type):
  @obey_qps()
  def list(request):
    return request.execute()

  resource = ctx.service().assets()
  request = resource.list(projectId=projectid, type=type, fields="nextPageToken,assets/id,assets/name")

  while request != None:
    response = list(request)
    for asset in response['assets']:
     click.echo("%s (%s)" % (asset["name"], asset["id"]))

    request = resource.list_next(request, response)
