# =================================================================
#
# Authors: Alexandre Roy <alexandre.roy@nrcan-rncan.gc.ca>
#
# Copyright (c) 2023 Alexandre Roy
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import os, logging, json, zipfile, boto3, botocore

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from pygeoapi.util import (get_provider_by_type, to_json)
from pygeoapi.plugin import load_plugin


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'extract',
    'title': {
        'en': 'Extract the data',
        'fr': 'Extrait les données'
    },
    'description': {
        'en': 'This process takes a list of collections and a geometry wkt as input, extracts the records, saves them as geojson in a zip file, stores the zip file to an S3 Bucket, and returns the URL to download the file.',
        'fr': 'Ce processus prend une liste de collections, une géométrie en format wkt, extrait les enregistrements qui intersectent la géométrie, sauvegarde les informations en geojson, sauvegarde le tout dans un zip file dans un Bucket S3, et retourne le chemin URL pour télécharger le fichier.',
    },
    'keywords': ['extract', 'clip zip ship'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'collections': {
            'title': 'An array of collection names to extract records from',
            'description': 'An array of collection names to extract records from',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 99,
            'metadata': None,  # TODO how to use?
            'keywords': ['collections', 'records']
        },
        'geom': {
            'title': 'The geometry as WKT format',
            'description': 'The geometry as WKT format',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['geometry']
        },
        'geom_crs': {
            'title': 'The crs of the input geometry',
            'description': 'The crs of the input geometry',
            'schema': {
                'type': 'integer'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['crs']
        }
    },
    'outputs': {
        'echo': {
            'title': 'The url to the zip file containing the information',
            'description': 'The url to the zip file containing the information',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            "collections": [
                "cdem_mpi__major_projects_inventory_point",
                "cdem_mpi__major_projects_inventory_line",
                "cdem_mpi__cdem"
            ],
            "geom": "POLYGON((-72.3061 45.3656, -72.3061 45.9375, -71.7477 45.9375, -71.7477 45.3656, -72.3061 45.3656))",
            "geom_crs": 4326
        }
    }
}


class ExtractProcessor(BaseProcessor):
    """Extract Processor"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.extract.ExtractProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        # Read the input params
        geom = data['geom']
        geom_crs = data['geom_crs']
        colls = data['collections']

        # For each collection to query
        content = {}
        files = []
        for c in colls:
            # Read the configuration for it
            c_conf = self.processor_def['collections'][c]

            # Get the collection type by its providers
            c_type = ExtractProcessor._get_collection_type_from_providers(c_conf['providers'])

            # Get the provider by type
            provider_def = get_provider_by_type(c_conf['providers'], c_type)

            # Load the plugin
            p = load_plugin('provider', provider_def)

            # If the collection has a provider of type feature
            if c_type == "feature":
                # Query using the provider logic
                res = p.query(offset=0, limit=10,
                              resulttype='results', bbox=None,
                              bbox_crs=None, geom_wkt=geom, geom_crs=geom_crs,
                              datetime_=None, properties=[],
                              sortby=[],
                              select_properties=[],
                              skip_geometry=False,
                              q=None, language='en', filterq=None)

            elif c_type == "coverage":
                # Query using the provider logic
                query_args = {
                    'geom': geom,
                    'geom_crs': geom_crs
                }
                res = p.query(**query_args)

            else:
                res = None
                pass # Skip, unsupported

            # Store the result in the response content
            content[c] = res

            # Save to a JSON file
            files.append(ExtractProcessor._save_file(c + ".json", res))

        # Destination zip file path and name
        dest_zip = './Extraction.zip'

        # Save all files to a zip file
        zip_file = ExtractProcessor._zip_file(files, dest_zip)

        # Put the zip file in S3
        ExtractProcessor._connect_s3_send_file(self.processor_def['s3_iam_role'], self.processor_def['s3_bucket_name'], zip_file)

        mimetype = 'application/json'

        return mimetype, content


    @staticmethod
    def _save_file(file_name: str, content: dict):
        with open(f'./{file_name}', 'w') as f:
            json.dump(content, f)
        return file_name


    @staticmethod
    def _zip_file(file_names: list, zip_file_name: str):
        with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in file_names:
                zipf.write(f'./{f}')
            zipf.close()
            return zipf


    @staticmethod
    def _connect_s3_send_file(iam_role: str, bucket_name: str, file_path: str):
        try:
            # Create an STS client object that represents a live connection to the
            # STS service
            sts_client = boto3.client('sts')

            # Call the assume_role method of the STSConnection object and pass the role
            # ARN and a role session name.
            assumed_role_object = sts_client.assume_role(
                RoleArn=iam_role,
                RoleSessionName="AssumeRoleECS"
            )

            # From the response that contains the assumed role, get the temporary
            # credentials that can be used to make subsequent API calls
            credentials = assumed_role_object['Credentials']

            # Use the temporary credentials that AssumeRole returns to make a
            # connection to Amazon S3
            s3_resource = boto3.resource(
                's3',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token= credentials['SessionToken']
            )

            # Send the file to the bucket
            s3_resource.Bucket(bucket_name).upload_file('./Extraction.zip', os.path.basename('Extraction.zip'))

        except botocore.exceptions.ClientError as e:
            print("ERROR UPLOADING FILE TO S3")
            print(e)
            raise


    @staticmethod
    def _get_collection_type_from_providers(providers: list):
        # For each provider
        for p in providers:
            if p['type'] == "feature":
                return "feature"
            elif p['type'] == "coverage":
                return "coverage"
        return None


    def __repr__(self):
        return f'<ExtractProcessor> {self.name}'
