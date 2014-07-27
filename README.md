# Hodor
A command-line interface for Google Maps Engine.

![images](https://cloud.githubusercontent.com/assets/5944358/3281059/148d865e-f490-11e3-830d-a0c33bed1b47.jpeg)

# Installation
Requires Python 2.7 (32bit).

> **Note:** If you receive a *"setuptools pip failed with error code 1"* error whilst setting up your virtualenv you need to downgrade to virtualenv 1.11.2 due to [this](https://github.com/pypa/virtualenv/issues/524) issue.

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
If you don't already have Python installed check out [ActivePython](http://www.activestate.com/activepython/downloads).

```
virtualenv venv
venv\Scripts\activate.bat
pip install --editable .
```

> **Note:** If you receive the error *"error: Unable to find vcvarsall.bat"* whilst Hodor is installing you need to install a C compiler due to [this](http://stackoverflow.com/questions/2817869/error-unable-to-find-vcvarsall-bat) issue.
> Simply download [Visual Studio C++ 2008 Express Edition](http://download.microsoft.com/download/A/5/4/A54BADB6-9C3F-478D-8657-93B3FC9FE62D/vcsetup.exe), open another command prompt, and away you go.

## Initial Setup
The first time you run Hodor he will open a browser window and prompt you to authorise him to access Google Maps Engine on your behalf. When the ***The authentication flow has completed*** message shows you can close the tab and return to your terminal.


# Authentication
Hodor ships with its own application credentials, so by default you don't need to setup anything else.

However, Hodor's application credentials are limited to 10,000 requests/day, so it is possible for other users of Hodor to exhaust this shared pool.

If you find this occurring (a) let me know, and (b) you can setup your own applications credentials thusly:

1. Go to the [Google Developers Console](https://cloud.google.com/console/project) and **Create a new project**
2. Go to the **APIs & Auth** section and enable the **Google Maps Engine API**
3. Click on **Credentials** and create a new **Installed Application**
4. Create ```oauth.json``` in the directory you have Hodor installed in and include your new clientId and Secret.
```json
{
  "client_id": "your-client-id",
  "client_secret": "your-client-secret"
}
```
5. Remove the existing ```credentials-store.json``` file.


# Using Hodor
Hodor knows about:

- Uploading and creating raster and vector assets, and adding them to layers and raster collections.
- Bulk ingest of raster assets.
- Querying vector tables.
- Modifying the contents of vector tables (WIP).

To work out what capabilities a certain Hodor command has simply pass ``--help`` on the end of the command.

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
Hodor can upload raster and vector data to create new assets and, optionally, directly create new layers or append to an existing raster collection or layer.


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
hodor create raster --mosaic-id={asset-id} "test-data/Alkimos 1963/config.json"
```

Recommended directory structure:

    agency-name/        -- Name of the custodian agency (e.g. Landgate)
      asset-name/       -- Name of the dataset (e.g. lgate_cadastre_poly_1)
        config.json     -- Store your asset metadata here
        payload/        -- Store your data here (e.g. Shapefiles, JPEG2000)

### Bulk Ingest
Hodor supports bulk ingest of raster assets based on a single JSON confguration file.

```
hodor bulk-load raster "test-data/raster_birds/config.json"
```

In this mode Hodor will create one new raster asset for every file in your ```payload``` directory. Each asset will be based on the template information provided in ```config.json```, with the name the asset being set to the name of the file.

### JSON Config Files
#### config.json
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

#### layers.json
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


## Querying Vector Tables
Hodor can query vector tables and export the results to the GeoJSON format, including:

* Limiting the query to a specific bounding box and GME's [SQL-like query language](https://developers.google.com/maps-engine/documentation/read#queries).
* Querying huge areas by splitting the bounding box into manageable chunks.
* Multi-threaded querying to send many requests to GME in parallel for optimised retrieval.

Return all of the features from a vector table:
```
hodor features list {table-id} features.json
```

Return all of the features from a vector table in a given area and with a ```LOCALITY``` of NANNUP:
```
hodor features list -bbox="115.876581, -31.926812, 115.960266, -31.893442" -where="LOCALITY <> 'NANNUP'" {table-id} features.json
```

# Documentation
Some associated documentation for anyone wishing to build on or understand this wee tool.

[Installing the Google Maps Engine API Client Library for Python](https://developers.google.com/api-client-library/python/apis/mapsengine/v1)

[The PyDoc reference for the Google Maps Engine API](https://developers.google.com/resources/api-libraries/documentation/mapsengine/v1/python/latest/)

[APIs Explorer for the Google Maps Engine API](https://developers.google.com/apis-explorer/#p/mapsengine/v1/)

[Google API Discovery Service for GME](https://www.googleapis.com/discovery/v1/apis/mapsengine/v1/rest)

# Credit
Due credit and thanks go to the folks that put together the [Google Cloud Platform code samples](https://code.google.com/p/google-cloud-platform-samples/), particularly the [chunked transfer](https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage) example on which Hodor was originally based.

And to Armin Ronacher for his fantastic [Click](http://click.pocoo.org) library.
