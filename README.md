# Hodor
A command line interface for Google Maps Engine.

Hodor handles streaming large files, resumable uploads, and retrying failed uploads.

![images](https://cloud.githubusercontent.com/assets/5944358/3281059/148d865e-f490-11e3-830d-a0c33bed1b47.jpeg)

# Installation
Requires Python 2.7 (32bit) and the Google Client APIs.

> If you receive the error *"setuptools pip failed with error code 1"* whilst setting up your virtualenv you need to downgrade to virtualenv 1.11.2 due to [this](https://github.com/pypa/virtualenv/issues/524) issue.

```
pip install virtualenv==1.11.2
```

## Linux
```
virtualenv venv
. venv/bin/activate
pip install --editable .
```

## Windows

```
virtualenv venv
venv\Scripts\activate.bat
pip install --editable .
```

> If you receive the error *"error: Unable to find vcvarsall.bat"* whilst Hodor is installing you need to install a C compiler due to [this](http://stackoverflow.com/questions/2817869/error-unable-to-find-vcvarsall-bat) issue.
> Simply download [Visual Studio C++ 2008 Express Edition](http://download.microsoft.com/download/A/5/4/A54BADB6-9C3F-478D-8657-93B3FC9FE62D/vcsetup.exe), open another command prompt, and away you go.


# OAuth2
In order to use Hodor you need to setup a ***Native Application*** OAuth client in the [Google Developers Console](https://cloud.google.com/console) and create an ```oauth.json``` file in the same directory as Hodor with your clientId and secret.

```json
{
  "client_id": "{your-client-id}",
  "client_secret": "{your-client-secret}"
}
```

**Note:** On first run Hodor will open your browser and prompt you to authorise him to access Google Maps Engine on your behalf. When the ***The authentication flow has completed*** message shows you can close the tab and return to your terminal.


# Usage
Hodor knows about:


- Uploading and creating raster and vector assets via ```create raster``` ```create vector```
- Bulk ingest of raster assets via ```bulk-load```
- Listing and searching of large vector tables via ```features list``` (WIP)
- Modifying the contents of vector tables via ```update``` (WIP)

Commands in Hodor are controlled via JSON configuration files and optional command-line flags. To work out what capabilities a certain Hodor command has simply pass ``--help``.

To begin using Hodor, first activate your virtual environment:

**Linux**
```
. venv/bin/activate
```

**Windows**
```
venv/Scripts/activate.bat
```

## Creating Raster & Vector Assets
Hodor can upload raster and vector data to create new assets and, optionally, directly create new layers or append to an existing raster collection.

Hodor currently handles uploading new raster and vector data, creating layers, add to raster collections e.g.

Upload a new raster image:
```
hodor create raster "test-data/Alkimos 1963/config.json"
```

Upload a new vector table and create a layer from it:
```
hodor create vector --layer-configfile=test-data/daa_003_subet/layers.json "test-data/daa_003_subet/config.json"
```

Upload a new raster image and add it to an existing raster mosaic:
```
hodor create raster --mosaic-id=1234-5678 "test-data/Alkimos 1963/config.json"
```

Recommended directory structure:

    agency-name/        -- Name of the custodian agency (e.g. Landgate)
      asset-name/       -- Name of the dataset (e.g. lgate_cadastre_poly_1)
        config.json     -- Store your asset metadata here
        payload/        -- Store your data here (e.g. Shapefiles, JPEG2000)

## Bulk Ingest
Hodor supports bulk ingest of raster assets based on a single JSON confguration file.

```
hodor bulk-load raster "test-data/raster_birds/config.json"
```

In this mode Hodor will create one new raster asset for every file in your ```payload``` directory. Each asset will be based on the template information provided in ```config.json```, with the name the asset being set to the name of the file.

# JSON Config Files
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

## layers.json
```layers.json``` should contain your layer metadata fields in JSON format as defined by Google Maps Engine.

See the [layer create documentation](https://developers.google.com/maps-engine/documentation/layer-create) for the minium required fields.

```json
{
  "projectId": "06151154151057343427",
  "layers": [
    {
      "name": "daa_003_subset layer",
      "datasourceType": "table",
      "draftAccessList": "Map Editors",
      "tags": ["Hodor"],
      "styleFile": "style.json",
      "infoWindowFile": "infoWindow.html"
    }
  ]
}
```

```layers.json``` supports two optional fields:

> *styleFile* is an optional parameter that will use the given file as the layer's style document.
>
> *infoWindowFile* is an optional parameter that will read the given HTML file into the layer's style document.

# Documentation
Some associated documentation for anyone wishing to build on or understand this wee tool.

[Installing the Google Maps Engine API Client Library for Python](https://developers.google.com/api-client-library/python/apis/mapsengine/v1)

[The PyDoc reference for the Google Maps Engine API](https://developers.google.com/resources/api-libraries/documentation/mapsengine/v1/python/latest/)

[APIs Explorer for the Google Maps Engine API](https://developers.google.com/apis-explorer/#p/mapsengine/v1/)

[Google API Discovery Service for GME](https://www.googleapis.com/discovery/v1/apis/mapsengine/v1/rest)

# Credit
Due credit and thanks go to the folks that put together the [Google Cloud Platform code samples](https://code.google.com/p/google-cloud-platform-samples/), particularly the [chunked transfer](https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage) example on which Hodor is based.

And to Armin Ronacher for his fantastic [Click](http://click.pocoo.org) library.
