import sys, os, logging, json, requests
from dateutil.parser import parse
from urllib.parse import urlparse
from http import HTTPStatus
from typing import Any, Tuple, Union
from configparser import ConfigParser
import json, ast, yaml, psycopg2
import psycopg2.extras
from psycopg2 import sql
from pygeoapi.linked_data import jsonldify
from pygeoapi.api import API, APIRequest, pre_process
from pygeoapi.util import (to_json, yaml_load)
from pygeoapi import api_collections
from pygeoapi import api_aws
from copy import deepcopy




class API_CZS(API):
    """
    This class inherits and overrides some specific functions from
     the core PyGeoAPI-API object to add NRCan-specificfunctionalities.
    """

    def __init__(self, config):
        """
        Initialize object
        """
        super().__init__(config)


    def on_load_resources(self, resources):
        """
        Performs our own dynamic load of the available resources from the Clip Zip Ship database.

        :param resources: the resources as PyGeoAPI would typically work with

        :returns: the resources that PyGeoAPI should work with
        """

        # Reads the AWS Secrets manager to retrieve sensitive parameters
        aws_secrets = api_aws.get_secret("ca-central-1", "secretsmanager", "/stage/cdtk_api_pygeoapi")
        self.config["settings"]["database"] = aws_secrets["database"]
        self.config["settings"]["email"] = aws_secrets["email"]
        self.config["settings"]["s3"] = aws_secrets["s3"]

        # Open the connection
        with open_conn(self.config["settings"]["database"]) as conn:
            # Flush the resources with a new dictionary
            the_resources = {}

            # Fetch the collections from the database
            data = api_collections.fetch_collections(conn)

            for d in data:
                providerDict = None

                # If not already loaded
                if not d["collection_name"] in the_resources:
                    thisTemplate = api_collections.tableTemplateDict[d['provider_type']]

                    # Depending on the feature
                    if d['provider_type'] == "feature":
                        providerDict = api_collections.load_template_postgres(thisTemplate, d)

                    elif d['provider_type'] == "coverage":
                        providerDict = api_collections.load_template_rasterio(thisTemplate, d)

                    # If loading anything
                    if providerDict:
                        the_resources[d["collection_name"]] = deepcopy(providerDict)

                else:
                    print("Collection already loaded: " + d["collection_name"])
                    #pass # Already loaded this resource key

        # Add our custom process dynamically
        the_resources['extract'] = {
            'type': 'process',
            'processor': {
                'name': 'pygeoapi.process.extract_nrcan.ExtractNRCanProcessor',
                'collections': deepcopy(the_resources),
                'settings': deepcopy(self.config["settings"])
            }
        }

        # Return the resources
        return the_resources


    def on_description_filter_spatially(self, collections, geom_wkt, geom_crs):
        """
        Performs our own spatial filter to filter the collections based on a geometry wkt.

        :param collections: the collections as PyGeoAPI would typically respond with
        :param geom_wkt: the geometry wkt on which to filter the collections
        :param geom_crs: The geometry crs for the related geometry wkt

        :returns: the spatially filtered collections (if actually filtering)
        """

        # print("on_description_filter_spatially: filtering?=" + ('true' if geom_wkt else 'false'))
        print("Count before: " + str(len(collections)))

        # If filtering spatially using a wkt
        if geom_wkt:
            # Filtering
            # Open the connection
            with open_conn(self.config["settings"]["database"]) as conn:
                # Query the collections intersecting with the bbox
                collections_in_bbox = api_collections.query_collections(conn, geom_wkt, geom_crs, 4617)
                collection_names_in_bbox = [o["collection_name"] for o in collections_in_bbox]

                # Filter to only keep the collections intersecting the bbox
                collections_filtered = {}
                for k, v in collections.items():
                    if k in collection_names_in_bbox:
                        collections_filtered[k] = v
                collections = collections_filtered

        # Return the filtered collections list
        print("Count after: " + str(len(collections)))
        return collections


    def on_build_collection_finalize(self, locale, collection_data_type, input_coll, active_coll):
        """
        Performs some additional processing to group the collections by parents and themes.

        :param locale: The language
        :param collection_data_type: The collection type
        :param input_coll: The input collection group
        :param active_coll: The current collection being finalized
        """

        # print("on_build_collection_finalize : "  + collection_data_type + " : " + active_coll['title'])

        # Add the theme information to the output
        if 'theme' in input_coll:
            if isinstance(input_coll['theme'], dict):
                # Depending on the language
                #active_coll['theme_id'] = input_coll['theme']['id']
                active_coll['theme'] = input_coll['theme']['en']
                if str(locale) == 'fr_CA':
                    active_coll['theme'] = input_coll['theme']['fr']

            else:
                active_coll['theme'] = input_coll['theme']

        if 'parent' in input_coll:
            if isinstance(input_coll['parent'], dict):
                # Depending on the language
                #active_coll['parent_id'] = input_coll['parent']['id']
                active_coll['parent'] = input_coll['parent']['en']
                if str(locale) == 'fr_CA':
                    active_coll['parent'] = input_coll['parent']['fr']

            else:
                active_coll['parent'] = input_coll['parent']

        if 'project' in input_coll:
            active_coll['project'] = input_coll['project']
        if 'short_name' in input_coll:
            active_coll['short_name'] = input_coll['short_name']
        if 'org_schema' in input_coll:
            active_coll['org_schema'] = input_coll['org_schema']


    @pre_process
    @jsonldify
    def reload_resources(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:
        """
        Reloads the resources.

        :param request: A request object
        :returns: tuple of headers, status code, content
        """

        headers = request.get_response_headers()

        # Reinitialize the configuration
        self.load_resources()

        return headers, HTTPStatus.OK, to_json({"reloaded": True}, self.pretty_print)


    @pre_process
    @jsonldify
    def reload_stac(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:
        """
        Reloads the rasterio information in the database.

        :param request: A request object
        :returns: tuple of headers, status code, content
        """

        headers = request.get_response_headers()

        uuids = ['f129611d-7ca1-418b-8390-ebac5adf958e',
                 'f498bb69-3982-4b62-94db-4c0e0065bc17',
                 '03ccfb5c-a06e-43e3-80fd-09d4f8f69703',
                 '230f1f6d-353e-4d02-800b-368f4c48dc86',
                 '37745ea7-d0cf-4ef6-b6b8-1cb3a7fce0b8',
                 '0fe65119-e96e-4a57-8bfe-9d9245fba06b',
                 'd8627209-bda2-436f-b22b-0eb19fdc6660',
                 '7f245e4d-76c2-4caa-951a-45d1d2051333',
                 '62de5952-a5eb-4859-b086-22a8ba8024b8',
                 '768570f8-5761-498a-bd6a-315eb6cc023d',
                 'b352a71a-011e-4a8e-b97c-77eae5ed3226',
                 '4e8e3c6a-c961-4def-bdc7-f24823462818',
                 '93d94cac-05d2-4ea0-82e1-3ff8500ebf93']
        #uuids = ['f129611d-7ca1-418b-8390-ebac5adf958e']
        #uuids = ['0fe65119-e96e-4a57-8bfe-9d9245fba06b']

        # Open connection
        with open_conn(self.config["settings"]["database"]) as conn:
            # Delete all coverage/rasterio information
            api_stac_cogs.flush_rasterio(conn)

            # Add the uuids
            api_stac_cogs.add_rasterio(conn,
                self.config["settings"]["catalogue_url"],
                self.config["settings"]["datacube_url"],
                self.config["settings"]["metadata_url"],
                self.config["settings"]["projection_default"],
                uuids)

            # Done
            conn.commit()

        return headers, HTTPStatus.OK, to_json({"reloaded": True}, self.pretty_print)


def open_conn(database):
    """
    Connects to the Clip Zip Ship database which holds the collections informations.

    :returns: A :class:`~psycopg2` connection
    """

    # Connects and returns the connection
    # print('Connecting to the PostgreSQL database...')
    return psycopg2.connect(user=database["user"],
                            password=database["password"],
                            host=database["host"],
                            port=database["port"],
                            database=database["dbname"])



