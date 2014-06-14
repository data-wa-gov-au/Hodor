import mimetypes
mimetypes.init()
mimetypes.add_type("image/jpeg", ".jp2")
mimetypes.add_type("application/shp", ".shp")
mimetypes.add_type("application/shx", ".shx")
mimetypes.add_type("application/dbf", ".dbf")
mimetypes.add_type("application/prj", ".prj")

import math
import os
import json
import httplib2
import random
import sys
import time
import datetime
import logging

from os import walk

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage as CredentialStorage
from oauth2client.tools import run as run_oauth2
from apiclient.discovery import build as discovery_build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload

# Configure httplib2
# httplib2.debuglevel = 4

# Configure logger
logger = logging.getLogger('hodor')
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("[%(asctime)s]: %(message)s"))
logger.addHandler(console)

# File where the user confiurable OAuth details are stored
OAUTH_CONFIG = "oauth.json"

# File where we will store authentication credentials after acquiring them.
CREDENTIALS_FILE = 'credentials-store.json'

# Message describing how to use the script.
USAGE = """
Usage examples:
  $ python gme_uploader.py --vector Landgate/lgate_cadastre_poly_1/config.json
  $ python gme_uploader.py --raster Landgte/Alkimos/config.json
"""

RW_SCOPE = 'https://www.googleapis.com/auth/mapsengine'
RO_SCOPE = 'https://www.googleapis.com/auth/mapsengine.read_only'

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Number of times to retry failed uploads.
NUM_RETRIES = 5

# Pass -1 to take advantage of streaming large files in Python >= 2.6
# https://google-api-python-client.googlecode.com/hg/docs/epy/apiclient.http.MediaFileUpload-class.html
CHUNKSIZE = -1

# I haven't checked that though, so you an always specify the chunk size to upload here instead.
# Number of bytes to send/receive in each request.
# CHUNKSIZE = 2 * 1024 * 1024

# Length of time we should wait for an asset to process.
ASSET_PROCESSING_TIMEOUT = 90 * 60 * 60

# https://developers.google.com/api-client-library/python/guide/aaa_oauth
def get_authenticated_service(scope):
  logger.debug('Authenticating...')
  with open(OAUTH_CONFIG) as f:
    config = json.load(f)

  flow = OAuth2WebServerFlow(
    client_id=config['client_id'],
    client_secret=config['client_secret'],
    scope=RW_SCOPE,
    user_agent='Landgate-Hodor/1.0')

  credential_storage = CredentialStorage(CREDENTIALS_FILE)
  credentials = credential_storage.get()
  if credentials is None or credentials.invalid:
    credentials = run_oauth2(flow, credential_storage)

  logger.debug('Constructing Google Maps Engine service...')
  http = credentials.authorize(httplib2.Http())
  return discovery_build('mapsengine', 'v1', http=http)

def handle_progressless_iter(error, progressless_iters):
  if progressless_iters > NUM_RETRIES:
    logger.error('Failed to make progress for too many consecutive iterations.')
    raise error

  sleeptime = random.random() * (2**progressless_iters)
  logger.warning('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
         % (str(error), sleeptime, progressless_iters))
  time.sleep(sleeptime)

def print_with_carriage_return(s):
  sys.stdout.write('\r' + s)
  sys.stdout.flush()

def upload_file(file, id, resource):
  logger.info("Uploading file '%s' to asset %s" % (file, id))
  start_time = time.time()

  media = MediaFileUpload(file, chunksize=CHUNKSIZE, resumable=True)
  if not media.mimetype():
    raise Exception("Could not determine mime-type. Please make lib mimetypes aware of it.")
  request = resource.files().insert(id=id, filename=os.path.basename(file), media_body=media)

  progressless_iters = 0
  response = None
  while response is None:
    error = None
    try:
      progress, response = request.next_chunk()
      if progress:
        print_with_carriage_return('Upload %d%%' % (100 * progress.progress()))
    except HttpError, err:
      # Contray to the documentation GME does't return 201/200 for the last chunk
      if err.resp.status == 204:
        response = ""
      else:
        error = err
        if err.resp.status < 500:
          raise
    except RETRYABLE_ERRORS, err:
      error = err

    if error:
      progressless_iters += 1
      handle_progressless_iter(error, progressless_iters)
    else:
      progressless_iters = 0

  logger.info("Upload completed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))

def uploader(assettype, configfile):
  with open(configfile) as f:
    config = json.load(f)

  # Fetch payload files
  config['files'] = {}
  payloaddir = os.path.dirname(configfile) + "/payload"
  for (dirpath, dirnames, filenames) in walk(payloaddir):
    config['files'] = [{'filename': i} for i in filenames]
    break

  # Create and upload asset
  service = get_authenticated_service(RW_SCOPE)
  if assettype == "vector":
    resource = service.tables()
  elif assettype == "raster":
    resource = service.rasters()

  response = resource.upload(body=config).execute()
  logger.info("Asset '%s' created with id %s" % (response['name'], response['id']))
  for i in config['files']:
    upload_file(os.path.join(payloaddir, i['filename']), response['id'], resource)

  # Poll until processed
  start_time = time.time()

  processingStatus = "processing"
  while processingStatus == "processing":
    response = resource.get(id=response['id']).execute()
    logger.info("Status of asset %s is '%s'." % (response['id'], response["processingStatus"]))

    if response["processingStatus"] == "complete":
      logger.info("Processing complete and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
      return response
    elif response["processingStatus"] == "failed":
      logger.info("Processing failed and took %s minutes" % (round((time.time() - start_time) / 60, 2)))
      raise Exception("Asset failed to process in time.")
    else:
      if time.time() - start_time >= ASSET_PROCESSING_TIMEOUT:
        raise Exception("Hodor Hodor Hodor!\nGiving up waiting on '%s'" % (response['id']))
      else:
        time.sleep(10)

if __name__ == '__main__':
  if len(sys.argv) < 3:
    print 'Too few arguments.'
    print USAGE
  if not (sys.argv[1] == "--vector" or sys.argv[1] == "--raster"):
    print "Unrecognised asset type %s" % (sys.argv[1][2:])
  else:
    logger.info("Hodor Hodor Hodor Begin!")
    asset = uploader(sys.argv[1][2:], sys.argv[2])
    logger.info("Hodor Hodor Hodor End!")
    print asset
