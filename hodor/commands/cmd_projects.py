import click
from hodor.cli import pass_context

@click.command('projects', short_help='List accessible Google Maps Engine projects.')
@pass_context
def cli(ctx):
  response = ctx.service.projects().list().execute()
  for project in response['projects']:
   click.echo("%s (%s)" % (project["id"], project["name"]))
