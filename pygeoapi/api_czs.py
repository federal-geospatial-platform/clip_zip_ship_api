'''
    summary
    -------

    The above code is looping through the tables in the database and filling the yaml file with the data from the tables.

    the command can be issued as below:

    Examples
    --------

    case external: C:\Python38\python.exe C:/python38Projects/createYMLfromPostgresql.py postgreSQL_database_external.ini

    case internal: C:\Python38\python.exe C:/python38Projects/createYMLfromPostgresql.py postgreSQL_database.ini

    The configuration file: py postgreSQL_database.ini or py postgreSQL_database_external.ini must be exist before
    launching the command.

    Examples
    --------

        [postgresql]

        host=core-4b2e37e.nrn-awscloud.internal

        database=ddr_qgis

        port=7082

        user=postgres

        password=****

        [yamlfile]

        yamlfile=pygeoapi_config.yml

    if the yml file does not exist, then the header is used to create a new yml file. If the yml exists,
    then the data provider section is updated according the information in postgresql tables.
'''

import sys, os, logging, json, requests
from dateutil.parser import parse
from urllib.parse import urlparse
from typing import Any, Tuple, Union
from configparser import ConfigParser
import json, ast, yaml, psycopg2
import psycopg2.extras
from psycopg2 import sql
from pygeoapi.linked_data import jsonldify
from pygeoapi.api import API, APIRequest, pre_process
from pygeoapi.util import (to_json, yaml_load)
from pygeoapi.geonetwork import GeoNetworkReader
from copy import deepcopy


header=\
'''
    server:
        bind:
            host: 0.0.0.0
            port: 5000
        url: http://localhost:5000
        mimetype: application/json; charset=UTF-8
        encoding: utf-8
        gzip: false
        language: en-US
        cors: true
        pretty_print: true
        limit: 10
        map:
            url: https://tile.openstreetmap.org/{z}/{x}/{y}.png
            attribution: '<a href="https://wikimediafoundation.org/wiki/Maps_Terms_of_Use">Wikimedia maps</a> | Map data &copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap contributors</a>'
        ogc_schemas_location: http://schemas.opengis.net
    logging:
        level: ERROR
    metadata:
        identification:
            title: pygeoapi Demo instance - running latest GitHub version
            description: pygeoapi provides an API to geospatial data
            keywords:
            - geospatial
            - data
            - api
            keywords_type: theme
            terms_of_service: https://creativecommons.org/licenses/by/4.0/
            url: https://github.com/geopython/pygeoapi
        license:
            name: CC-BY 4.0 license
            url: https://creativecommons.org/licenses/by/4.0/
        provider:
            name: pygeoapi Development Team
            url: https://pygeoapi.io
        contact:
            name: Kralidis, Tom
            position: Lead Dev
            address: Mailing Address
            city: City
            stateorprovince: Administrative Area
            postalcode: Zip or Postal Code
            country: Canada
            phone: +xx-xxx-xxx-xxxx
            fax: +xx-xxx-xxx-xxxx
            email: you@example.org
            url: Contact URL
            hours: Hours of Service
            instructions: During hours of service.  Off on weekends.
            role: pointOfContact
'''

# This is a data structure template for a Postgres provider.
postgres_template=\
'''
    type: null
    title:
        en: null
        fr: null
    parent:
        en: null
        fr: null
    theme:
        en: null
        fr: null
    description:
        en: null
        fr: null
    keywords:
        en: null
        fr: null
    links:
    -   type: null
        rel: null
        title: null
        href: null
        hreflang: null
    extents:
        spatial:
            bbox: null
            crs: null
        temporal:
            begin: null
            end: null
    providers:
    -   type: null
        name: null
        data:
            host: null
            port: null
            dbname: null
            user: null
            password: null
            search_path: null
        id_field: null
        table: null
        crs: null
'''


# This is the data structure template for a WFS provider.
wfs_template=\
'''
    type: null
    title:
        en: null
        fr: null
    parent:
        en: null
        fr: null
    theme:
        en: null
        fr: null
    description:
        en: null
        fr: null
    keywords:
        en: null
        fr: null
    links:
    -   type: null
        rel: null
        title: null
        href: null
        hreflang: null
    extents:
        spatial:
            bbox: null
            crs: null
        temporal:
            begin: null
            end: null
    providers:
    -   type: null
        name: null
        data:
            source_type: null
            source: ''
            source_srs: null
            target_srs: null
            source_capabilities: null
            source_options: null
            gdal_ogr_options: null
        id_field: null
        layer: null
'''



# This is the data structure template for the rasterio provider.
rasterio_template =\
'''
    type: null
    title:
        en: null
        fr: null
    parent:
        en: null
        fr: null
    theme:
        en: null
        fr: null
    description:
        en: null
        fr: null
    keywords:
        en: null
        fr: null
    extents:
        spatial:
            bbox: null
            crs: null
        temporal:
            begin: null
            end: null
    links:
    -   type: null
        rel: null
        title: null
        href: null
        hreflang: null
    providers:
    -   type: null
        name: null
        data: null
        format:
            name:
            mimetype:
        crs: null
'''


# This is a dictionary that is used to select different data structure with different table.
tableTemplateDict = {
        "feature": postgres_template,
        #"pygeoapi_wfs_collection_info": wfs_template,
        "coverage": rasterio_template
    }


LOGGER = logging.getLogger(__name__)


def fillCommon(itemvalue, template, data):
    '''This function fills the common information for a collection (whether Feature or Coverage types).

    Parameters
    ----------
    itemvalue
        the item being filled
    template
        the template file
    dataRecords
        a list of tuples containing the data to be imported.

    Returns
    -------
        A dictionary of dictionaries. The key is the collection name of the record in the database. The value is a dictionary of the record.

    '''

    itemvalue["type"] = data["collection_type"]
    itemvalue["title"]["en"] = data["collection_title_en"]
    itemvalue["title"]["fr"] = data["collection_title_fr"]
    itemvalue["parent"]["en"] = data["collection_parent_en"]
    itemvalue["parent"]["fr"] = data["collection_parent_fr"]
    itemvalue["theme"]["en"] = data["collection_theme_en"]
    itemvalue["theme"]["fr"] = data["collection_theme_fr"]
    itemvalue["description"]["en"] = data["collection_description_en"]
    itemvalue["description"]["fr"] = data["collection_description_fr"]
    itemvalue["keywords"]["en"] = data["collection_keywords_en"]
    itemvalue["keywords"]["fr"] = data["collection_keywords_fr"]
    itemvalue["links"][0]["type"] = data["links_type"]
    itemvalue["links"][0]["rel"] = data["links_rel"]
    itemvalue["links"][0]["title"] = data["links_title"]
    itemvalue["links"][0]["href"] = data["links_href"]
    itemvalue["links"][0]["hreflang"] = data["links_hreflang"]
    itemvalue["extents"]["spatial"]["bbox"] = data["extents_spatial_bbox"]
    itemvalue["extents"]["spatial"]["crs"] = data["extents_spatial_crs"]
    itemvalue["providers"][0]["type"] = data["provider_type"]
    itemvalue["providers"][0]["name"] = data["provider_name"]
    itemvalue["providers"][0]["crs"] = data["collection_crs"]
    

def fillPostgresDict(template, data):
    '''This function takes a template and a list of records and returns a dictionary of the template filled with the records

    Parameters
    ----------
    template
    	the template file
    dataRecords
    	a list of tuples containing the data to be imported.

    Returns
    -------
    	A dictionary of dictionaries. The key is the collection name of the record in the database. The value is a dictionary of the record.

    '''
    
    itemvalue = yaml.load(template, Loader=yaml.FullLoader)
    fillCommon(itemvalue, template, data)
    itemvalue["providers"][0]["data"]["host"] = data["data_host"]
    itemvalue["providers"][0]["data"]["port"] = data["data_port"]
    itemvalue["providers"][0]["data"]["dbname"] = data["data_dbname"]
    itemvalue["providers"][0]["data"]["user"] = data["data_user"]
    itemvalue["providers"][0]["data"]["password"] = data["data_password"]
    itemvalue["providers"][0]["data"]["search_path"] = data["data_search_path"]
    itemvalue["providers"][0]["id_field"] = data["data_id_field"]
    itemvalue["providers"][0]["table"] = data["data_table"]    
    return itemvalue


def fillWFSDict(template, data):
    '''The function takes a template and a list of records and returns a dictionary of records

    Parameters
    ----------
    template
    	the template file
    dataRecords
    	a list of tuples containing the data records

    Returns
    -------
    	A dictionary of dictionaries. The key is the collection name of the record in the database. The value is a dictionary of the record.

    '''
    itemvalue = yaml.load(template, Loader=yaml.FullLoader)
    fillCommon(itemvalue, template, data)
    return itemvalue


def fillRasterioDict(template, data):
    '''The function takes a template and a list of records and returns a dictionary of records

    Parameters
    ----------
    template
    	the template file
    dataRecords
    	a list of tuples containing the data records

    Returns
    -------
    	A dictionary of dictionaries. The key is the collection name of the record in the database. The value is a dictionary of the record.

    '''
    itemvalue = yaml.load(template, Loader=yaml.FullLoader)
    fillCommon(itemvalue, template, data)
    itemvalue["extents"]["temporal"]["begin"] = data["extents_temporal_begin"]
    itemvalue["extents"]["temporal"]["end"] = data["extents_temporal_end"]
    itemvalue["providers"][0]["data"] = data["data"]
    
    #if data[16] != None:
        #itemvalue["providers"][0]["options"] = json.loads(data[16].replace("'",'"'))
    if data["format_name"] != None:
        itemvalue["providers"][0]["format"]["name"] = data["format_name"]
        itemvalue["providers"][0]["format"]["mimetype"] = data["format_mimetype"]
    return itemvalue


def config(filename='postgreSQL_database.ini', section='postgresql'):
    '''Reads the configuration file and returns a dictionary of the parameters

    Parameters
    ----------
    filename, optional
    	The name of the configuration file.
    section, optional
    	The section name in the .ini file.

    Returns
    -------
    	A dictionary of the parameters in the config file.

    '''
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def open_conn(database):
    """
    Connects to the database.

    :returns: A :class:`~psycopg2` connection
    """

    # Connects and returns the connection
    #print('Connecting to the PostgreSQL database...')
    return psycopg2.connect(user=database["user"],
                            password=database["password"],
                            host=database["host"],
                            port=database["port"],
                            database=database["dbname"])


def fetch_collections(conn):
    '''
    Fetches the collection information from the table. When no record, then return [].
    Parameters
    ----------
    configFile
    	the name of the configuration file.

    Returns
    -------
    	A list of tuples.
    '''

    # Create a cursor
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # SQL query
    sql_query = "SELECT * FROM {table_coll} ORDER BY {order_field}"

    # Execute a statement
    cur.execute(sql.SQL(sql_query).format(table_coll=sql.Identifier("czs", "v_czs_collections"),
                                          order_field=sql.Identifier("collection_uuid")))

    # Fetch and return
    return cur.fetchall()


def flush_rasterio(conn):
    # Open a cursor
    with conn.cursor() as cur:
        str_query = "DELETE FROM {table} WHERE provider_type = %s AND provider_name = %s"

        # Query in the database
        query = sql.SQL(str_query).format(
            table=sql.Identifier("czs", "czs_collection"))

        # Execute cursor
        cur.execute(query, ('coverage', 'rasterio'))


def read_node(node):
    # Make sure we read the "value" (not a dictionary due to namespaces being read at the node level)
    value = None
    if isinstance(node, dict):
        if "#text" in node:
            value = value["#text"]

        elif "@codeListValue" in node:
            value = value["@codeListValue"]
    return value


def get_catalogue_cogs(uuids, catalogue_url: str):

    catalogue_results = {}

    # For each catalogue GUID to load
    for uuid in uuids:
        # Connect to GeoNetwork to get the XML
        response = requests.get(catalogue_url.format(metadata_uuid=uuid))

        # Create class
        geonetwork = GeoNetworkReader(response.text)

        # For each cog urls found
        for cog_info in geonetwork.get_cogs():
            catalogue_results[cog_info["url_path"]] = {
                "metadata_uuid": uuid,
                "theme_en": geonetwork.topic(),
                "theme_fr": geonetwork.topic(),
                "title_en": cog_info["name"]["en"],
                "title_fr": cog_info["name"]["fr"],
                "keywords_en": geonetwork.keywords_full()["en"],
                "keywords_fr": geonetwork.keywords_full()["fr"],
                "description_en": geonetwork.title_full()["en"],
                "description_fr": geonetwork.title_full()["fr"]
            }

    return catalogue_results


def get_stac_cogs(datacube_url: str):

    stac_results = {}

    # Reinitialize the STAC
    req = requests.get(datacube_url + '/collections')

    if req.status_code == 200:
        data1 = json.loads(req.text)
        
        # For each collection
        for col in data1['collections']:
            # Get the items
            req2 = requests.get(datacube_url + '/collections/' + col['id'] + "/items")

            if req2.status_code == 200:
                data2 = json.loads(req2.text)

                # For each feature
                for feat in data2["features"]:
                    
                    # For each asset
                    for ass in feat["assets"]:
                        asset = feat["assets"][ass]

                        # If an image
                        if 'href' in asset and asset['href'].endswith('.tif'):
                            # Read the url path
                            path = urlparse(asset['href']).path

                            # Fill the results
                            stac_results[path] = {
                                "collection" : col,
                                "feature": feat,
                                "asset": asset
                            }

    return stac_results


def load_rasterio(conn, metadata_uid, coll_type, coll_name, coll_theme_en, coll_theme_fr,
                  coll_title_en, coll_title_fr, coll_desc_en, coll_desc_fr, coll_keywords_en, coll_keywords_fr,
                  coll_crs, provider_type, provider_name, provider_data, provider_format, provider_mimetype,
                  extents_spatial_bbox, extents_spatial_crs, extents_temporal_begin, extents_temporal_end,
                  links_type, links_rel, links_title, links_href, links_hreflang,
                  geom):
    
    # Redirect to the stored procedure in the database
    return None


def query_collections(database, polygon_wkt: str, projection_id: int, projection_id_data: int):
    # Open the connection
    with open_conn(database) as conn:
        # Create a cursor
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query filter
        spatial_filter = ("ST_Intersects(GEOM, ST_Transform(ST_GeomFromText('{polygon_wkt}', {projection_id})," + str(projection_id_data) + "))").format(polygon_wkt=polygon_wkt, projection_id=projection_id)

        # SQL query
        sql_query = "SELECT * FROM {table} WHERE {spatial_filter} ORDER BY {field_coll_name}"

        # Execute a statement
        cur.execute(sql.SQL(sql_query).format(table=sql.Identifier("czs", "czs_collection"),
                                              spatial_filter=sql.SQL(spatial_filter),
                                              field_coll_name=sql.Identifier("collection_name")))

        # Fetch and return
        return cur.fetchall()


class API_CZS(API):

    def __init__(self, config):
        """
        Initialize object
        """
        super().__init__(config)

    def on_load_resources(self):

        # HACK: ALEX: New function to load the self.config object from the database

        # Open the connection
        with open_conn(self.config["settings"]["database"]) as conn:
            # Flush the resources with a new dictionary
            self.config['resources'] = {}

            # Fetch the collections from the database
            data = fetch_collections(conn)

            for d in data:
                providerDict = None
                
                # If not already loaded
                if not d["collection_name"] in self.config['resources']:
                    thisTemplate = tableTemplateDict[d['provider_type']]
                
                    # Depending on the feature
                    if d['provider_type'] == "feature":
                        providerDict = fillPostgresDict(thisTemplate, d)
                    
                    elif d['provider_type'] == "coverage":
                        providerDict = fillRasterioDict(thisTemplate, d)
                    
                    # If loading anything
                    if providerDict:
                        self.config['resources'][d["collection_name"]] = deepcopy(providerDict)

                else:
                    print("Already loaded: " + d["collection_name"])
                    #print(d)
                    pass # Already loaded this resource key

        ############################################
        # Config path
        config_path = os.environ.get('PYGEOAPI_CONFIG')

        # Stringify
        ymalStringData = yaml.dump(self.config, indent=4, default_flow_style=False, sort_keys=False)

        # Write to file
        #with open(config_path, 'w') as outfile:
        #    outfile.write(ymalStringData)
        ############################################

        ############################################
        ### Read the config as generated
        ###config = None
        ###with open(config_path, encoding='utf8') as ff:
        ###    config = yaml_load(ff)
        ############################################

        return self.config
        
    def on_filter_spatially(self, collections, geom_wkt, geom_crs):
        if geom_wkt:
            # Query the collections intersecting with the bbox
            collections_in_bbox = query_collections(self.config["settings"]["database"], geom_wkt, geom_crs, 4617)
            collection_names_in_bbox = [o["collection_name"] for o in collections_in_bbox]

            # Filter to only keep the collections intersecting the bbox
            collections_filtered = {}
            for k, v in collections.items():
                if k in collection_names_in_bbox:
                    collections_filtered[k] = v
            collections = collections_filtered

        # Return the filtered collections list
        return collections

    def on_build_collection_finalize(self, locale, collection_data_type, input_coll, active_coll):

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


        #if len(input_coll["providers"]) > 0 and 'crs' in input_coll["providers"][0]:
        #    active_coll['crs'] = input_coll["providers"][0]["crs"]

        # Add the providers information to the output, but remove the data information for the feature collections
        #if 'providers' in input_coll:
        #    active_coll['providers'] = input_coll['providers']
        #    for p in active_coll['providers']:
        #        if 'type' in p and p['type'] == 'feature' and 'data' in p:
                    #del p['data']

    @pre_process
    @jsonldify
    def reload_stac(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:

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
            flush_rasterio(conn)

            # Add the uuids
            self.add_rasterio(conn, uuids)

            # Done
            conn.commit()

        return headers, 200, to_json({"reloaded": True}, self.pretty_print)        


    def add_rasterio(self, conn, uuids):
        # Get catalogue cogs to load
        catalogue_cogs = get_catalogue_cogs(uuids, self.config["settings"]["catalogue_url"].rstrip('/'))

        # Get stac cogs
        stac_cogs = get_stac_cogs(self.config["settings"]["datacube_url"].rstrip('/'))

        # For each catalogue cogs, check if exist in stac cogs
        for cat_url in catalogue_cogs:
            if cat_url in stac_cogs:
                #print("FOUND!")
                
                col = stac_cogs[cat_url]["collection"]
                feat = stac_cogs[cat_url]["feature"]
                asset = stac_cogs[cat_url]["asset"]
                
                guid = catalogue_cogs[cat_url]["metadata_uuid"]
                
                title_en = col['id']
                if "title_en" in catalogue_cogs[cat_url]:
                    title_en = catalogue_cogs[cat_url]["title_en"]

                title_fr = col['id']
                if "title_fr" in catalogue_cogs[cat_url]:
                    title_fr = catalogue_cogs[cat_url]["title_fr"]

                theme_en = catalogue_cogs[cat_url]["theme_en"]
                theme_fr = catalogue_cogs[cat_url]["theme_fr"]

                desc_en = col['id']
                if "description_en" in catalogue_cogs[cat_url]:
                    desc_en = catalogue_cogs[cat_url]["description_en"]
                
                desc_fr = col['id']
                if "description_fr" in catalogue_cogs[cat_url]:
                    desc_fr = catalogue_cogs[cat_url]["description_fr"]

                keywords_en = catalogue_cogs[cat_url]["keywords_en"]
                keywords_fr = catalogue_cogs[cat_url]["keywords_fr"]
                
                coll_name = feat['id']
                bbox = feat['bbox']

                projection = self.config["settings"]["projection_default"]
                if 'proj:epsg' in feat['properties']:
                    projection = feat['properties']['proj:epsg']

                mimetype = asset['type']
                if ";" in mimetype:
                    mimetype = mimetype.split(";")[0].strip()

                date = None
                if 'properties' in feat and 'datetime' in feat['properties']:
                    date = parse(feat['properties']['datetime'])

                load_rasterio(conn,
                              guid, 'collection', coll_name, theme_en, theme_fr,
                              title_en, title_fr, desc_en, desc_fr, keywords_en, keywords_fr,
                              projection,
                              'coverage', 'rasterio', 
                              asset['href'],
                              'GTiff',
                              mimetype,
                              bbox,
                              'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
                              date, date,
                              'text/html', 'canonical', 'information',
                              self.config["settings"]["metadata_url"].rstrip('/') + '/' + guid,
                              'en-CA',
                              "ST_Multi(ST_AsText(ST_MakeEnvelope(" + ','.join(str(v) for v in bbox) + "," + str(projection) + ")))")


# To run in command line
#if __name__ == '__main__':
    # TODO: Use configuration file
    #generate_config({})
    
