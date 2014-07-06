import click
from retries import retries
from hodor.cli import pass_context

@click.command('projects', short_help='List accessible Google Maps Engine projects.')
@pass_context
@retries(2)
def cli(ctx):
  response = ctx.service.projects().list().execute()
  for project in response['projects']:
   click.echo("%s (%s)" % (project["id"], project["name"]))
