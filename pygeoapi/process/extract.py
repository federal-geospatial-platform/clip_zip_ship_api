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

import os, logging, json, zipfile, boto3, botocore, requests, psycopg2, uuid
from psycopg2 import sql
from xml.etree import cElementTree as ET

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from pygeoapi.util import (get_provider_by_type, to_json)
from pygeoapi.plugin import load_plugin


LOGGER = logging.getLogger(__name__)


# Configurations - placed here inside the plugin
EXTRACT_FOLDER = "extractions"
TABLE_NAME = "czs_collection"
FIELD_COLLECTION_NAME = "collection_name"
FIELD_METADATA_XML = "metadata_cat_xml"


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

            # Save to a JSON file and keep track
            files.append(ExtractProcessor._save_file_json(c + ".json", res))

            # Fetch the metadata xml for the collection
            meta = self.query_collection(self.processor_def['settings']['database'], c)

            # If found
            if meta and meta['metadata_cat_xml']:
                # Save the metadata for the collection and keep track
                files.append(ExtractProcessor._save_file_xml(c + ".xml", meta['metadata_cat_xml']))

        # Destination zip file path and name
        dest_zip = f'./{uuid.uuid4()}.zip'

        # Save all files to a zip file
        zip_file = ExtractProcessor._zip_file(files, dest_zip)

        # Put the zip file in S3
        #ExtractProcessor._connect_s3_send_file(self.processor_def['settings']['s3_iam_role'], self.processor_def['settings']['s3_bucket_name'], zip_file)

        mimetype = 'application/json'

        return mimetype, content


    def query_collection(self, database_info: dict, collection: str):
        """
        Queries the Collection information based on the given name.

        :returns: The information on a collection.
        """

        # Connect to the database
        with psycopg2.connect(host=database_info['host'], port=database_info['port'], dbname=database_info['dbname'],
                              user=database_info['user'], password=database_info['password'], sslmode="allow") as conn:
            # Open a cursor
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                str_query = """SELECT {field_collection_name},
                                      {field_collection_metadata}::text
                               FROM {table_name} WHERE {field_collection_name}=%s"""

                # Query in the database
                query = sql.SQL(str_query).format(
                    field_collection_name=sql.Identifier(FIELD_COLLECTION_NAME),
                    field_collection_metadata=sql.Identifier(FIELD_METADATA_XML),
                    table_name=sql.Identifier(database_info['dbname'], TABLE_NAME))

                # Execute cursor
                cur.execute(query, (collection,))

                # Fetch
                res = cur.fetchall()
                if len(res) > 0:
                    return res[0]
                return None


    @staticmethod
    def _save_file_json(file_name: str, content: dict):
        if not os.path.exists(f'./{EXTRACT_FOLDER}'):
            os.makedirs(f'./{EXTRACT_FOLDER}')
        with open(f'./{EXTRACT_FOLDER}/{file_name}', 'w', encoding='utf-8') as f:
            json.dump(content, f, indent=4)
        return file_name


    @staticmethod
    def _save_file_xml(file_name: str, content: dict):
        if not os.path.exists(f'./{EXTRACT_FOLDER}'):
            os.makedirs(f'./{EXTRACT_FOLDER}')
        with open(f'./{EXTRACT_FOLDER}/{file_name}', 'w', encoding='utf-8') as f:
            f.write(content)
        return file_name


    @staticmethod
    def _zip_file(file_names: list, zip_file_name: str):
        if not os.path.exists(f'./{EXTRACT_FOLDER}'):
            os.makedirs(f'./{EXTRACT_FOLDER}')
        with zipfile.ZipFile(f"./{EXTRACT_FOLDER}/{zip_file_name}", 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in file_names:
                # Write the file in the zip
                zipf.write(f'./{EXTRACT_FOLDER}/{f}', f'./{f}')
                # Delete the file now that it's in the zip
                os.remove(f'./{EXTRACT_FOLDER}/{f}')
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
