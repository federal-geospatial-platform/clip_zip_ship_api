import os  # noqa: E401
from http import HTTPStatus
from typing import Any, Tuple, Union
import psycopg2
import psycopg2.extras
from pygeoapi.linked_data import jsonldify
from pygeoapi.api import API, APIRequest, pre_process
from pygeoapi.util import to_json
from pygeoapi import api_collections
from pygeoapi import api_aws
from copy import deepcopy
from datetime import datetime, timezone


class API_CZS(API):
    """
    This class inherits and overrides some specific functions from
     the core PyGeoAPI-API object to add NRCan-specificfunctionalities.
    """

    def __init__(self, config, openapi):
        """
        Initialize object
        """
        self.secret_aws_keys = {}
        super().__init__(config, openapi)

    def on_load_resources(self, resources):
        """
        Performs our own dynamic load of the available resources from the
        Clip Zip Ship database.

        :param resources: the resources as PyGeoAPI would typically work with

        :returns: the resources that PyGeoAPI should work with
        """

        # Precond for the GitHub automated testing running vanilla
        if "settings" not in self.config:
            return super().on_load_resources(resources)

        # Reads the AWS Secrets manager to retrieve sensitive parameters
        aws_secrets = api_aws.get_secret("ca-central-1", "secretsmanager",
                                         self.config["settings"]["secret_aws_key"])  # noqa
        self.config["settings"]["database"] = aws_secrets["database"]
        self.config["settings"]["email"] = aws_secrets["email"]
        self.config["settings"]["s3"] = aws_secrets["s3"]

        # Set the manager config on-the-fly as well
        self.config["server"]["manager"] = {}
        self.config["server"]["manager"]["name"] = "PostgresDB"
        self.config["server"]["manager"]["max_concurrent"] = 0
        self.config["server"]["manager"]["max_queue"] = 0
        self.config["server"]["manager"]["result_in_db"] = True
        self.config["server"]["manager"]["internal_error_in_db"] = True
        self.config["server"]["manager"]["connection"] = {}
        self.config["server"]["manager"]["connection"]["host"] = aws_secrets["database"]["host"]  # noqa
        self.config["server"]["manager"]["connection"]["port"] = aws_secrets["database"]["port"]  # noqa
        self.config["server"]["manager"]["connection"]["dbname"] = aws_secrets["database"]["dbname"]  # noqa
        self.config["server"]["manager"]["connection"]["user"] = aws_secrets["database"]["user"]  # noqa
        self.config["server"]["manager"]["connection"]["password"] = aws_secrets["database"]["password"]  # noqa

        # Open the connection
        with open_conn(self.config["settings"]["database"]) as conn:
            # Flush the resources with a new dictionary
            the_resources = {}

            # Fetch the collections from the database
            data = api_collections.fetch_collections(conn)

            # For each collection found in the database
            for d in data:
                providerDict = None

                # If not already loaded
                if not d["collection_name"] in the_resources:
                    # Get the template
                    thisTemplate = api_collections.tableTemplateDict[d['provider_type']]  # noqa

                    # Depending on the feature
                    if d['provider_type'] == "feature":
                        # Check if we've fetched this secret aws key before
                        sec_key = d['data_secret_aws_key']
                        if sec_key not in self.secret_aws_keys:
                            # Fetch the secret value
                            sec_val = api_aws.get_secret("ca-central-1", "secretsmanager", sec_key)  # noqa

                            # Store it
                            self.secret_aws_keys[sec_key] = {
                                'data_host': sec_val['host'],
                                'data_port': sec_val['port'],
                                'data_dbname': sec_val['dbname'],
                                'data_user': sec_val['user'],
                                'data_password': sec_val['password'],
                            }

                        # Combine the data
                        d2 = {**d, **self.secret_aws_keys[sec_key]}

                        # Load template for Postgres
                        providerDict = api_collections.load_template_postgres(thisTemplate, d2)  # noqa

                    elif d['provider_type'] == "coverage":
                        # Load template for Coverage
                        providerDict = api_collections.load_template_rasterio(thisTemplate, d)  # noqa

                    # If loading anything
                    if providerDict:
                        the_resources[d["collection_name"]] = deepcopy(providerDict)  # noqa

                else:
                    print("Collection already loaded: " + d["collection_name"])
                    # pass # Already loaded this resource key

        # Add our custom process dynamically
        the_resources['extract'] = {
            'type': 'process',
            'processor': {
                'name': 'pygeoapi.process.extract_nrcan.ExtractNRCanProcessor',
                'server': self.config["server"],
                'settings': deepcopy(self.config["settings"]),
                'collections': deepcopy(the_resources)
            }
        }

        # Return the resources
        return the_resources

    def on_load_resources_check(self, last_loaded_resources):
        """
        Overrides the check if the resources should be reloaded for the
        currently running instance of pygeoapi.
        This is useful as pygeoapi can be distributed (load balanced) on
        multiple instances with their own allocated memory for their resources.

        :param last_loaded_resources: the UTC date of the last time the
        resources were loaded on this particular pygeoapi instance.

        :returns: True if the resources should be reloaded (no need to reload
        the resources manually here, the API mother class will do it)
        """

        # Precond for the GitHub automated testing running vanilla
        if "settings" not in self.config:
            return super().on_load_resources_check(last_loaded_resources)

        # Open the connection
        with open_conn(self.config["settings"]["database"]) as conn:
            # Check if the date of last load is prior
            # to the date in the database
            date_loaded = api_collections.get_flag_reload_resources(conn)

            # If the date last loaded on the pygeoapi instance is prior
            # to date in the database
            print(f'{os.getpid()} - Date last load - {last_loaded_resources}')

            if last_loaded_resources < date_loaded:
                print(f'{os.getpid()} - Must reload collections')
                # Must reload
                return True

        # No need to reload
        print(f'{os.getpid()} - No need to reload collections')
        return False

    def on_description_filter_spatially(self, collections, geom_wkt, geom_crs):
        """
        Performs our own spatial filter to filter the collections based on a
        geometry wkt.

        :param collections: the collections as PyGeoAPI would typically
        respond with
        :param geom_wkt: the geometry wkt on which to filter the collections
        :param geom_crs: The geometry crs for the related geometry wkt

        :returns: the spatially filtered collections (if actually filtering)
        """

        # print("on_description_filter_spatially: filtering?=" + ('true' if geom_wkt else 'false'))  # noqa
        # print("Count before: " + str(len(collections)))

        # Precond for the GitHub automated testing running vanilla
        if "settings" not in self.config:
            return super().on_description_filter_spatially(collections, geom_wkt, geom_crs)  # noqa

        # If filtering spatially using a wkt
        if geom_wkt:
            # Filtering
            # Open the connection
            with open_conn(self.config["settings"]["database"]) as conn:
                # Query the collections intersecting with the bbox
                collections_in_bbox = api_collections.query_collections(conn, geom_wkt, geom_crs, 4617)  # noqa
                collection_names_in_bbox = [o["collection_name"] for o in collections_in_bbox]  # noqa

                # Filter to only keep the collections intersecting the bbox
                collections_filtered = {}
                for k, v in collections.items():
                    if k in collection_names_in_bbox:
                        collections_filtered[k] = v
                collections = collections_filtered

        # Return the filtered collections list
        # print("Count after: " + str(len(collections)))
        return collections

    def on_build_collection_finalize(self, locale, collections,
                                     collection_data_type, input_coll,
                                     active_coll):
        """
        Performs some additional processing to group the collections by parents
        and themes.

        :param locale: The language
        :param collection_data_type: The collection type
        :param input_coll: The input collection group
        :param active_coll: The current collection being finalized
        """

        # print("on_build_collection_finalize : "  + collection_data_type + " : " + active_coll['title'])  # noqa

        # Precond for the GitHub automated testing running vanilla
        if "settings" not in self.config:
            return super().on_build_collection_finalize(locale, collections,
                                                        collection_data_type,
                                                        input_coll,
                                                        active_coll)

        # Add the theme information to the output
        if 'theme' in input_coll:
            if isinstance(input_coll['theme'], dict):
                # Depending on the language
                # active_coll['theme_id'] = input_coll['theme']['id']
                active_coll['theme'] = input_coll['theme']['en']
                if str(locale) == 'fr_CA':
                    active_coll['theme'] = input_coll['theme']['fr']

            else:
                active_coll['theme'] = input_coll['theme']

        if 'parent' in input_coll:
            if isinstance(input_coll['parent'], dict):
                # Depending on the language
                # active_coll['parent_id'] = input_coll['parent']['id']
                active_coll['parent'] = input_coll['parent']['en']
                if str(locale) == 'fr_CA':
                    active_coll['parent'] = input_coll['parent']['fr']

            else:
                active_coll['parent'] = input_coll['parent']

        if 'parent_title' in input_coll:
            if isinstance(input_coll['parent_title'], dict):
                # Depending on the language
                # active_coll['parent_id'] = input_coll['parent']['id']
                active_coll['parent_title'] = input_coll['parent_title']['en']
                if str(locale) == 'fr_CA':
                    active_coll['parent_title'] = input_coll['parent_title']['fr']  # noqa

            else:
                active_coll['parent_title'] = input_coll['parent_title']

        if 'short_name' in input_coll:
            active_coll['short_name'] = input_coll['short_name']

        if 'org_schema' in input_coll:
            active_coll['org_schema'] = input_coll['org_schema']

        # If specific collection requested, add the wkt
        if len(collections.items()) == 1 and active_coll['id']:
            # Open the connection
            with open_conn(self.config["settings"]["database"]) as conn:
                # Query wkt for the collection
                active_coll['wkt'] = api_collections.get_wkt_collection(conn, active_coll['id'])  # noqa

        if 'providers' in input_coll and 'crs' in input_coll["providers"][0]:
            active_coll['crs'] = input_coll["providers"][0]["crs"]

        if 'providers' in input_coll and 'max_extract_area' in input_coll["providers"][0]:  # noqa
            active_coll['max_extract_area'] = input_coll["providers"][0]['max_extract_area']  # noqa

    @pre_process
    @jsonldify
    def reload_resources(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:  # noqa
        """
        Reloads the resources.

        :param request: A request object
        :returns: tuple of headers, status code, content
        """

        headers = request.get_response_headers()

        # Open the connection
        with open_conn(self.config["settings"]["database"]) as conn:
            # Indicate that the resources should be reloaded by flagging it in
            # the database.
            # That way, any/all pygeoapi spawn instances which use the
            # collections in their memory will reload
            # prior to responding on their next call
            the_date = datetime.now(timezone.utc)
            api_collections.update_flag_reload_resources(conn, the_date)
            conn.commit()

        return headers, HTTPStatus.OK, to_json({"reloaded": True, "date": the_date}, self.pretty_print)  # noqa


def open_conn(database):
    """
    Connects to the Clip Zip Ship database which holds the collections
    informations.

    :returns: A :class:`~psycopg2` connection
    """

    # Connects and returns the connection
    # print('Connecting to the PostgreSQL database...')
    return psycopg2.connect(user=database["user"],
                            password=database["password"],
                            host=database["host"],
                            port=database["port"],
                            database=database["dbname"])
