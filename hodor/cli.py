import os
import sys
import logging
import json
import httplib2
import time
import random
import click
import socket
from multiprocessing import current_process
from pprintpp import pprint as pp

import mimetypes
mimetypes.init()
mimetypes.add_type("image/jpeg", ".jp2")
mimetypes.add_type("application/shp", ".shp")
mimetypes.add_type("application/shx", ".shx")
mimetypes.add_type("application/dbf", ".dbf")
mimetypes.add_type("application/prj", ".prj")

from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage as CredentialStorage
from oauth2client.tools import run as run_oauth2
from apiclient.discovery import build as discovery_build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload

CONTEXT_SETTINGS = dict(auto_envvar_prefix='HODOR')

# No handlers could be found for logger "oauth2client.util"
# import logging
# logging.basicConfig(filename='debug.log',level=logging.DEBUG)

class Context(object):

    def __init__(self):
        # Google Maps Engine scopes
        self.RW_SCOPE = 'https://www.googleapis.com/auth/mapsengine'
        self.RO_SCOPE = 'https://www.googleapis.com/auth/mapsengine.readonly'

        # File where we will store authentication credentials after acquiring them
        self.CREDENTIALS_STORE = 'credentials-store.json'
        self.CREDENTIALS_NATIVE_APP = 'oauth.json'
        self.CREDENTIALS_SERVICE_ACC = 'oauth-sa.json'

        self.services = {}

    def log(self, msg, *args):
        """Logs a message to stdout."""
        if args:
            msg %= args
        click.echo(msg, file=sys.stdout)

        if getattr(self, 'logger', None):
          self.logger.info(msg)

    def vlog(self, msg, *args):
        """Logs a message to stderr only if verbose is enabled."""
        if self.verbose:
            self.log(msg, *args)

    def service(self, scope=None, version="v1"):
      if scope is None:
        scope = self.RW_SCOPE

      ident = current_process().ident
      service_hash = "{0},{1}".format(scope, version)

      if ident not in self.services:
        self.services[ident] = {}

      if service_hash not in self.services[ident]:
        self.services[ident][service_hash] = self.get_authenticated_service(scope, version)
      return self.services[ident][service_hash]

    def refresh_services(self):
      ident = current_process().ident
      if ident in self.services:
        for s_hash in self.services[ident]:
          self.services[ident][s_hash] = self.get_authenticated_service(*s_hash.split(","))

    def get_authenticated_service(self, scope, version):
      self.vlog('Authenticating...')

      credential_storage = CredentialStorage(self.CREDENTIALS_STORE)
      credentials = credential_storage.get()

      if credentials is None or credentials.invalid:
        # Service Account
        if self.auth_type == 'service-account':
          with open(self.CREDENTIALS_SERVICE_ACC) as f:
            config = json.load(f)

          credentials = SignedJwtAssertionCredentials(
            service_account_name=config['client_email'],
            private_key=config['private_key'],
            scope=self.RW_SCOPE
          )
        else:
        # Web Flow
          if os.path.isfile(self.CREDENTIALS_NATIVE_APP):
            with open(self.CREDENTIALS_NATIVE_APP) as f:
              config = json.load(f)
          else:
            # This is OK according to Google
            # http://stackoverflow.com/questions/7274554/why-google-native-oauth2-flow-require-client-secret
            config = {
              "installed": {
                "client_id": "75839337166-pc5il9vgrgseopqberqi9pcr4clglcng.apps.googleusercontent.com",
                "client_secret": "OdkKJCeg_ocgu9XO9JjbGSlv"
              }
            }

          flow = OAuth2WebServerFlow(
            client_id=config['installed']['client_id'],
            client_secret=config['installed']['client_secret'],
            scope=scope,
            user_agent='Landgate-Hodor')
          credentials = run_oauth2(flow, credential_storage)

      if credentials is None or credentials.invalid:
        raise Exception("Unable to obtain valid credentials.")
      elif credentials.access_token_expired is True:
        self.vlog("Refreshing access token!")
        credentials.refresh(httplib2.Http())

      self.vlog("Access Token: %s" % credentials.access_token)

      self.vlog('Constructing Google Maps Engine %s service...' % (version))
      http = credentials.authorize(httplib2.Http())
      resource = discovery_build('mapsengine', version, http=http)

      # Fix for the default TCP send buffer being so riciculosuly low on Windows (8192)
      # These lines of code represent two days of work by multiple people.
      if 'https:www.googleapis.com' not in resource._http.connections:
        raise Exception("Unable to locate an open connection to googleapis.com")

      connection = resource._http.connections.get(resource._http.connections.keys()[0]) # https:www.googleapis.com
      self.vlog("Changing TCP send buffer from %s to %s" % (connection.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF), 5242880))
      connection.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 5242880)

      return resource

pass_context = click.make_pass_decorator(Context, ensure=True)
cmd_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                          'commands'))


class HodorCLI(click.MultiCommand):

    def list_commands(self, ctx):
        rv = []
        for filename in os.listdir(cmd_folder):
            if filename.endswith('.py') and \
               filename.startswith('cmd_'):
                rv.append(filename[4:-3])
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        try:
            if sys.version_info[0] == 2:
                name = name.encode('ascii', 'replace')
            mod = __import__('hodor.commands.cmd_' + name,
                             None, None, ['cli'])
        except ImportError as e:
            print e
            return
        return mod.cli


@click.command(cls=HodorCLI, context_settings=CONTEXT_SETTINGS)
@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Enable verbose mode.')
@click.option('--log-file', type=click.Path(dir_okay=False, writable=True, resolve_path=True),
              help='A log file to write output to.')
@click.option('--auth-type', default='web',
              help='The type of OAuth flow to apply. Defaults to web - may also be "service-account"')
@pass_context
def cli(ctx, verbose, log_file, auth_type):
  """A command line interface for Google Maps Engine."""
  # Configure logging level
  ctx.verbose = verbose

  # Configure file logging
  if log_file is not None:
    ctx.logger = logging.getLogger('Hodor')
    ctx.logger.setLevel(logging.INFO)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
    ctx.logger.addHandler(fh)

  ctx.auth_type = auth_type
