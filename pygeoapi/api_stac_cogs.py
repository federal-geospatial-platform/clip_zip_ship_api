import sys, os, json, requests, psycopg2
from dateutil.parser import parse
from urllib.parse import urlparse
import psycopg2.extras
from psycopg2 import sql
from pygeoapi.geonetwork import GeoNetworkReader


def flush_rasterio(conn):
    # Open a cursor
    with conn.cursor() as cur:
        str_query = "DELETE FROM {table} WHERE provider_type = %s AND provider_name = %s"

        # Query in the database
        query = sql.SQL(str_query).format(
            table=sql.Identifier(conn.info.dbname, "czs_collection"))

        # Execute cursor
        cur.execute(query, ('coverage', 'rasterio'))


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


def add_rasterio(conn, catalogue_url, datacube_url, metadata_url, projection_default, uuids):
        # Get catalogue cogs to load
        catalogue_cogs = get_catalogue_cogs(uuids, catalogue_url.rstrip('/'))

        # Get stac cogs
        stac_cogs = get_stac_cogs(datacube_url.rstrip('/'))

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

                projection = projection_default
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
                              metadata_url.rstrip('/') + '/' + guid,
                              'en-CA',
                              "ST_Multi(ST_AsText(ST_MakeEnvelope(" + ','.join(str(v) for v in bbox) + "," + str(projection) + ")))")



