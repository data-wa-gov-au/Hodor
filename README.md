# Hodor
A Google Maps Engine asset uploader.

Hodor handles streaming large files, resumable uploads, and retrying failed uploads.

![images](https://cloud.githubusercontent.com/assets/5944358/3281059/148d865e-f490-11e3-830d-a0c33bed1b47.jpeg)

# Installation
Requires Python 2.7 and the Google Client APIs.

```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Note:** On first run Hodor will open your browser and prompt you to authorise him to access Google Maps Engine on your behalf. When the ***The authentication flow has completed.*** message shows you can close the tab and return to your terminal.

## OAuth2
In order to use Hodor you need to setup a ***Native Application*** OAuth client in the [Google Developers Console](https://cloud.google.com/console) and create an ```oauth.json``` file in the same directory as Hodor with your clientId and secret.

```json
{
  "client_id": "{your-client-id}",
  "client_secret": "{your-client-secret}"
}
```

# Usage
Hodor knows about uploading raster and vector assets and takes asset configuration metadata from a JSON file.

```
usage: hodor.py --{asset type} path/to/config.json
Asset Type
  --raster Activates raster upload mode
  --vector Activates vector upload mode

e.g. python hodor.py --raster test-data/Alkimos 1963/config.json
```

Hodor uses the [apiclient.http.MediaFileUpload](https://google-api-python-client.googlecode.com/hg/docs/epy/apiclient.http.MediaFileUpload-class.html) method that comes with streaming for large files and resumable uploads. It will continue to poll GME until your newly created asset has finished processing (within some sensible defaults)

Recommended directory structure:

    agency-name/        -- Name of the custodian agency (e.g. Landgate)
      asset-name/       -- Name of the dataset (e.g. lgate_cadastre_poly_1)
        config.json     -- Store your asset metadata here
        payload/        -- Store your data here (e.g. Shapefiles, JPEG2000)

## config.json
```config.json``` should contain your asset metadata fields in JSON format as defined by Google Maps Engine.

See the GME API documentation for the minimum required fields for [vector](https://developers.google.com/maps-engine/documentation/table-upload) and [raster](https://developers.google.com/maps-engine/documentation/raster-upload) data.

```json
{
  "projectId": "{your-gme-project-id}",
  "name": "Alkimos 1964 Hodor Test",
  "draftAccessList": "Map Editors",
  "attribution": "Landgate",
  "rasterType": "image",
  "description":"This is a description.",
  "tags" : ["hodor"],
  "maskType":"imageMask",
  "acquisitionTime": {
    "start" : "2010-01-24T00:00:00Z",
    "end" : "2010-01-25T00:00:00Z",
    "precision" : "day"
  }
}
```

# Documentation
Some associated documentation for anyone wishing to build on or understand this wee tool.

[Installing the Google Maps Engine API Client Library for Python](https://developers.google.com/api-client-library/python/apis/mapsengine/v1)

[The PyDoc reference for the Google Maps Engine API](https://developers.google.com/resources/api-libraries/documentation/mapsengine/v1/python/latest/)

[APIs Explorer for the Google Maps Engine API](https://developers.google.com/apis-explorer/#p/mapsengine/v1/)

[Google API Discovery Service for GME](https://www.googleapis.com/discovery/v1/apis/mapsengine/v1/rest)

# Credit
Due credit and thanks go to the folks that put together the [Google Cloud Platform code samples](https://code.google.com/p/google-cloud-platform-samples/), particularly the [chunked transfer](https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage) example on which Hodor is based.
