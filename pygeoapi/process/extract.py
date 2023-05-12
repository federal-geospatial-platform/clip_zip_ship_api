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

import os, logging
from psycopg2 import sql
from xml.etree import cElementTree as ET

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from pygeoapi.provider.base import ProviderQueryError, ProviderPreconditionFailed
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
        'en': 'This process takes a list of collections, a geometry wkt and crs as inputs and proceeds to extract the records of all collections.',
        'fr': 'Ce processus prend une liste de collections, une géométrie en format wkt avec un crs et extrait les enregistrements de toutes les collections.',
    },
    'keywords': ['extract'],
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
                "coll_name_1",
                "coll_name_2"
            ],
            "geom": "POLYGON((-72.3061 45.3656, -72.3061 45.9375, -71.7477 45.9375, -71.7477 45.3656, -72.3061 45.3656))",
            "geom_crs": 4326
        }
    }
}


class ExtractProcessor(BaseProcessor):
    """
    Extract Processor used to query multiple collections, of various providers, at the same time.
    In this iteration, only collection types feature and coverage are supported, but the logic could be scaled up.
    """

    def __init__(self, processor_def, process_metadata):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.extract.ExtractProcessor
        """

        # If none set, use default
        if not process_metadata:
            process_metadata = PROCESS_METADATA

        super().__init__(processor_def, process_metadata)

    def get_collection_type(self, coll_name: str):
        # Read the configuration for it
        c_conf = self.processor_def['collections'][coll_name]

        # Get the collection type by its providers
        return ExtractProcessor._get_collection_type_from_providers(c_conf['providers'])

    def get_collection_coverage_mimetype(self, coll_name: str):
        # Read the configuration for it
        c_conf = self.processor_def['collections'][coll_name]

        # Get the collection type by its providers
        return ExtractProcessor._get_collection_mimetype_image_from_providers(c_conf['providers'])

    def execute(self, data):
        """
        Entry point of the execution process.
        """

        # Read the input params
        geom = data['geom']
        geom_crs = data['geom_crs']
        colls = data['collections']

        # Validate execution
        if self.on_query_validate_execution(geom, geom_crs, colls):
            # For each collection to query
            query_res = {}
            for c in colls:
                # Call on query with it which will query the collection based on its provider
                query_res[c] = self.on_query(c, geom, geom_crs)

            # Finalize the results
            self.on_query_finalize(data, query_res)

            # Return result
            return self.on_query_results(query_res)

        else:
            raise ProviderPreconditionFailed()

    def on_query(self, coll_name: str, geom_wkt: str, geom_crs: int):
        """
        Overridable function to query a particular collection given its name.
        One trick here is that the collections in processor_def must be a deepcopy of the
        ressources from the API configuration.
        """

        # Read the configuration for it
        c_conf = self.processor_def['collections'][coll_name]

        # Get the collection type by its providers
        c_type = ExtractProcessor._get_collection_type_from_providers(c_conf['providers'])

        # Get the provider by type
        provider_def = get_provider_by_type(c_conf['providers'], c_type)

        # Load the plugin
        p = load_plugin('provider', provider_def)

        # If the collection has a provider of type feature
        if c_type == "feature":
            # Query using the provider logic and clip = True!
            res = p.query(offset=0, limit=10,
                          resulttype='results', bbox=None,
                          bbox_crs=None, geom_wkt=geom_wkt, geom_crs=geom_crs,
                          datetime_=None, properties=[],
                          sortby=[],
                          select_properties=[],
                          skip_geometry=False,
                          q=None, language='en', filterq=None,
                          clip=True)

        elif c_type == "coverage":
            # Query using the provider logic
            query_args = {
                'geom': geom_wkt,
                'geom_crs': geom_crs,
                'format_': 'native'
            }
            res = p.query(**query_args)

        else:
            res = None
            pass # Skip, unsupported

        # Return the query result
        return res

    def on_query_validate_execution():
        """
        Override this method to perform validations pre-execution
        """
        return True

    def on_query_finalize(self, data: dict, query_res: dict):
        """
        Override this method to do further things with the queried results
        """

        pass

    def on_query_results(self, query_res: dict):
        """
        Override this method to return something else than the default json of the results
        """

        # Return the query results
        return 'application/json', query_res

    @staticmethod
    def _get_collection_type_from_providers(providers: list):
        # For each provider
        for p in providers:
            if p['type'] == "feature":
                return "feature"
            elif p['type'] == "coverage":
                return "coverage"
        return None

    @staticmethod
    def _get_collection_mimetype_image_from_providers(providers: list):
        # For each provider
        for p in providers:
            if p['type'] == "coverage":
                if 'format' in p and 'mimetype' in p['format']:
                    return p['format']['mimetype']
        return None

    def __repr__(self):
        return f'<ExtractProcessor> {self.name}'
