import sys, os, json
import json, ast, yaml, psycopg2
import psycopg2.extras
from psycopg2 import sql


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
    project: null
    short_name: null
    org_schema: null
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
    project: null
    short_name: null
    org_schema: null
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
    wkt: null
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



def load_template_common(itemvalue, template, data):
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
    itemvalue["short_name"] = data["collection_short_name"]
    itemvalue["org_schema"] = data["collection_org_schema"]
    itemvalue["wkt"] = data["wkt"]
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
    itemvalue["providers"][0]["crs"] = [data["collection_crs"]]


def load_template_postgres(template, data):
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
    load_template_common(itemvalue, template, data)
    itemvalue["providers"][0]["data"]["host"] = data["data_host"]
    itemvalue["providers"][0]["data"]["port"] = data["data_port"]
    itemvalue["providers"][0]["data"]["dbname"] = data["data_dbname"]
    itemvalue["providers"][0]["data"]["user"] = data["data_user"]
    itemvalue["providers"][0]["data"]["password"] = data["data_password"]
    itemvalue["providers"][0]["data"]["search_path"] = data["data_search_path"]
    itemvalue["providers"][0]["id_field"] = data["data_id_field"]
    itemvalue["providers"][0]["table"] = data["data_table"]
    return itemvalue


def load_template_rasterio(template, data):
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
    load_template_common(itemvalue, template, data)
    itemvalue["extents"]["temporal"]["begin"] = data["extents_temporal_begin"]
    itemvalue["extents"]["temporal"]["end"] = data["extents_temporal_end"]
    itemvalue["providers"][0]["data"] = data["data"]

    #if data[16] != None:
        #itemvalue["providers"][0]["options"] = json.loads(data[16].replace("'",'"'))
    if data["format_name"] != None:
        itemvalue["providers"][0]["format"]["name"] = data["format_name"]
        itemvalue["providers"][0]["format"]["mimetype"] = data["format_mimetype"]
    return itemvalue


def query_collections(conn, polygon_wkt: str, projection_id: int, projection_id_data: int):
    # Create a cursor
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Query filter
    spatial_filter = f"ST_Intersects(GEOM, ST_Transform(ST_MakeValid(ST_GeomFromText('{polygon_wkt}', {projection_id}))," + str(projection_id_data) + "))"

    # SQL query
    sql_query = "SELECT * FROM {table} WHERE {spatial_filter} ORDER BY {field_coll_name}"

    # Execute a statement
    cur.execute(sql.SQL(sql_query).format(table=sql.Identifier(conn.info.dbname, "czs_collection"),
                                          spatial_filter=sql.SQL(spatial_filter),
                                          field_coll_name=sql.Identifier("collection_name")))

    # Fetch and return
    return cur.fetchall()


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
    sql_query = "SELECT *, ST_AsText(geom) as wkt FROM {table_coll} ORDER BY {order_field}"

    # Execute a statement
    cur.execute(sql.SQL(sql_query).format(table_coll=sql.Identifier(conn.info.dbname, "v_czs_collections"),
                                          order_field=sql.Identifier("collection_uuid")))

    # Fetch and return
    return cur.fetchall()


