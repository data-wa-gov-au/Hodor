import os
import sys
import logging
import json
import httplib2
import time
import random
import click
import socket
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

class Context(object):

    def __init__(self):
        self.verbose = False
        self.retry = 5
        self.home = os.getcwd()

        self.version = "v1"

        # Google Maps Engine scopes
        self.RW_SCOPE = 'https://www.googleapis.com/auth/mapsengine'
        self.RO_SCOPE = 'https://www.googleapis.com/auth/mapsengine.readonly'

        # File where the user confiurable OAuth details are stored.
        self.OAUTH_CONFIG = "oauth.json"
        self.OAUTH_CONFIG_SERVICE_ACC = "oauth-sa.json"

        # File where we will store authentication credentials after acquiring them.
        self.CREDENTIALS_FILE = 'credentials-store.json'

    def log(self, msg, *args):
        """Logs a message to stderr."""
        if args:
            msg %= args
        click.echo(msg, file=sys.stderr)

    def vlog(self, msg, *args):
        """Logs a message to stderr only if verbose is enabled."""
        if self.verbose:
            self.log(msg, *args)

    def get_authenticated_service(self, scope):
      self.vlog('Authenticating...')

      # Service Account
      if self.auth_type == 'service-account':
        with open(self.OAUTH_CONFIG_SERVICE_ACC) as f:
          config = json.load(f)

        credentials = SignedJwtAssertionCredentials(
          service_account_name=config['client_email'],
          private_key=config['private_key'],
          scope=self.RW_SCOPE
        )

        if credentials is None or credentials.invalid:
          raise Exception('Credentials invalid.')
      else:
      # Web Flow
        with open(self.OAUTH_CONFIG) as f:
          config = json.load(f)

        flow = OAuth2WebServerFlow(
          client_id=config['client_id'],
          client_secret=config['client_secret'],
          scope=scope,
          user_agent='Landgate-Hodor/1.0')

        credential_storage = CredentialStorage(self.CREDENTIALS_FILE)
        credentials = credential_storage.get()
        if credentials is None or credentials.invalid:
          credentials = run_oauth2(flow, credential_storage)

        # if credentials.access_token_expired is False:
            # credentials.refresh(httplib2.Http())

      self.vlog('Constructing Google Maps Engine service...')
      http = credentials.authorize(httplib2.Http())
      resource = discovery_build('mapsengine', self.version, http=http)

      self.log("Access Token: %s" % credentials.access_token)
      self.access_token = credentials.access_token # For handcrafted requests to exp2

      # Fix for the default TCP send buffer being so riciculosuly low on Windows (8192)
      # These lines of code represent two days of work by multiple people.
      if 'https:www.googleapis.com' not in resource._http.connections:
        raise Exception("Unable to locate an open connection to googleapis.com")

      connection = resource._http.connections.get(resource._http.connections.keys()[0]) # https:www.googleapis.com
      self.vlog("Changing TCP send buffer from %s to %s" % (connection.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF), 5242880))
      connection.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 5242880)

      return resource


    def upload_file(self, file, id, resource):
      # Retry transport and file IO errors.
      RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)
      chunk_size = chunk_size = getattr(self, 'chunk_size', -1)

      self.log("Uploading file '%s'" % (file))
      start_time = time.time()

      media = MediaFileUpload(file, chunksize=chunk_size, resumable=True)
      if not media.mimetype():
        raise Exception("Could not determine mime-type. Please make lib mimetypes aware of it.")
      request = resource.files().insert(id=id, filename=os.path.basename(file), media_body=media)

      progressless_iters = 0
      response = None
      while response is None:
        error = None
        try:
          start_time_chunk = time.time()
          progress, response = request.next_chunk()
          if progress:
            Mbps = ((chunk_size / (time.time() - start_time_chunk)) * 0.008 * 0.001)
            print "%s%% (%s/Mbps)" % (round(progress.progress() * 100), round(Mbps, 2))
        except HttpError, err:
          # Contray to the documentation GME does't return 201/200 for the last chunk
          if err.resp.status == 204:
            response = ""
          else:
            error = err
            if err.resp.status < 500 and err.resp.status != 410:
              raise
        except RETRYABLE_ERRORS, err:
          error = err

        if error:
          progressless_iters += 1
          self.handle_progressless_iter(error, progressless_iters)
        else:
          progressless_iters = 0

      if 'pbar' in locals():
        del pbar
      self.log("Upload completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

    def handle_progressless_iter(self, error, progressless_iters):
      if progressless_iters > self.retry:
        self.log('Failed to make progress for too many consecutive iterations.')
        raise error

      sleeptime = random.random() * (2**progressless_iters)
      self.log('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
             % (str(error), sleeptime, progressless_iters))
      time.sleep(sleeptime)


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
@click.option('-v', '--verbose', is_flag=True,
              help='Enable verbose mode.')
@click.option('--retry', default=5,
              help='Number of times to retry failed requests before giving up.')
@click.option('--auth-type', default='web',
              help='The type of OAuth flow to apply. Defaults to web - may also be "service-account"')
@pass_context
def cli(ctx, verbose, retry, auth_type):
  """A command line interface for Google Maps Engine."""
  ctx.verbose = verbose
  ctx.retry = retry
  ctx.auth_type = auth_type
  ctx.service = ctx.get_authenticated_service(ctx.RW_SCOPE)
  ctx.thread_safe_services = {}
