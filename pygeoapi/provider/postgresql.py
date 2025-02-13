# =================================================================
#
# Authors: Jorge Samuel Mendes de Jesus <jorge.dejesus@protonmail.com>
#          Tom Kralidis <tomkralidis@gmail.com>
#          Mary Bucknell <mbucknell@usgs.gov>
#          John A Stevenson <jostev@bgs.ac.uk>
#          Colin Blackburn <colb@bgs.ac.uk>
#          Francesco Bartoli <xbartolone@gmail.com>
#
# Copyright (c) 2018 Jorge Samuel Mendes de Jesus
# Copyright (c) 2023 Tom Kralidis
# Copyright (c) 2022 John A Stevenson and Colin Blackburn
# Copyright (c) 2023 Francesco Bartoli
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

# Testing local docker:
# docker run --name "postgis" \
# -v postgres_data:/var/lib/postgresql -p 5432:5432 \
# -e ALLOW_IP_RANGE=0.0.0.0/0 \
# -e POSTGRES_USER=postgres \
# -e POSTGRES_PASS=postgres \
# -e POSTGRES_DBNAME=test \
# -d -t kartoza/postgis

# Import dump:
# gunzip < tests/data/hotosm_bdi_waterways.sql.gz |
#  psql -U postgres -h 127.0.0.1 -p 5432 test

import logging

from copy import deepcopy
from geoalchemy2 import Geometry  # noqa - this isn't used explicitly but is needed to process Geometry columns
from geoalchemy2.functions import ST_MakeEnvelope, ST_Transform, Find_SRID, \
     ST_PolygonFromText, ST_Intersection, ST_MakeValid
from geoalchemy2.shape import to_shape
from pygeofilter.backends.sqlalchemy.evaluate import to_filter
import pyproj
import shapely
from sqlalchemy import create_engine, MetaData, PrimaryKeyConstraint, asc, desc
from sqlalchemy.engine import URL
from sqlalchemy.exc import InvalidRequestError, OperationalError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, load_only
from sqlalchemy.sql.expression import and_

from pygeoapi.provider.base import BaseProvider, \
    ProviderConnectionError, ProviderQueryError, ProviderItemNotFoundError
from pygeoapi.util import get_transform_from_crs, get_area_from_wkt_in_km2

_ENGINE_STORE = {}
_TABLE_MODEL_STORE = {}
LOGGER = logging.getLogger(__name__)


class PostgreSQLProvider(BaseProvider):
    """Generic provider for Postgresql based on psycopg2
    using sync approach and server side
    cursor (using support class DatabaseCursor)
    """

    def __init__(self, provider_def):
        """
        PostgreSQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor

        :returns: pygeoapi.provider.base.PostgreSQLProvider
        """
        LOGGER.debug('Initialising PostgreSQL provider.')
        super().__init__(provider_def)

        self.table = provider_def['table']
        self.id_field = provider_def['id_field']
        self.geom = provider_def.get('geom_field', 'geom')
        self.max_extract_area_km_2 = provider_def['max_extract_area'] if 'max_extract_area' in provider_def else 1000  # noqa

        LOGGER.debug(f'Name: {self.name}')
        LOGGER.debug(f'Table: {self.table}')
        LOGGER.debug(f'ID field: {self.id_field}')
        LOGGER.debug(f'Geometry field: {self.geom}')

        # Read table information from database
        options = None
        if provider_def.get('options'):
            options = provider_def['options']
        self._store_db_parameters(provider_def['data'], options)
        self._engine, self.table_model = self._get_engine_and_table_model()
        LOGGER.debug(f'DB connection: {repr(self._engine.url)}')

        # Read the table fields
        self.fields = self.get_fields()
        LOGGER.debug('Fields: {}'.format(self.fields))

        # Read the table SRID
        if self._is_table_spatial():
            self.srid = self.get_srid()
        else:
            self.srid = None
        LOGGER.debug('SRID: {}'.format(self.srid))

    def query(self, offset=0, limit=10, resulttype='results',
              bbox=None, bbox_crs=None, geom_wkt=None,
              geom_crs=None, data_crs=None,
              datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None,
              filterq=None, crs_transform_spec=None, clip=0, **kwargs):
        """
        Query Postgis for all the content.
        e,g: http://localhost:5000/collections/hotosm_bdi_waterways/items?
        limit=1&resulttype=results

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy] to query on
        (when specified)
        :param bbox_crs: the spatial projection of the bounding box
        (when specified)
        :param geom_wkt: the geom wkt to query on
        (when specified)
        :param geom_crs: the spatial projection of the geom wkt
        (when specified)
        :param data_crs: the spatial projection of the data being queried, as
        read from the provider configuration
        (when specified).
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)
        :param filterq: CQL query as text string
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """

        LOGGER.debug('Preparing filters')
        property_filters = self._get_property_filters(properties)
        cql_filters = self._get_cql_filters(filterq)
        spat_filter = self._get_spatial_filter(bbox, bbox_crs, geom_wkt, geom_crs)  # noqa
        order_by_clauses = self._get_order_by_clauses(sortby, self.table_model)
        selected_properties = self._select_properties_clause(select_properties,
                                                             skip_geometry)

        LOGGER.debug('Querying PostGIS')

        # Execute query within self-closing database Session context
        with Session(self._engine) as session:
            # #### NRCAN SPECIFIC START
            # If there's a geometry for the request
            if geom_wkt:
                # If the area is valid
                if get_area_from_wkt_in_km2(geom_wkt, geom_crs) <= self.max_extract_area_km_2:  # noqa
                    # Limit can be infinite
                    print("Override the limit!")
                    limit = 1000000000
            # #### NRCAN SPECIFIC END

            out_crs = self.srid
            crs_transform_out = self._get_crs_transform(crs_transform_spec)
            if crs_transform_out:
                out_crs = pyproj.CRS.from_wkt(crs_transform_spec.target_crs_wkt).to_epsg()  # noqa

            if clip > 0 and geom_wkt:
                results = (
                    session.query(self.table_model, ST_Transform(ST_Intersection(getattr(self.table_model, self.geom),  # noqa
                                                                                 ST_Transform(ST_MakeValid(  # noqa
                                                                                     ST_PolygonFromText(geom_wkt,  # noqa
                                                                                                        geom_crs)),  # noqa
                                                                                              self.srid)  # noqa
                                                                                 ),  # noqa
                                                                 out_crs).label('inters')  # noqa
                                  )
                    .filter(property_filters)
                    .filter(cql_filters)
                    .filter(spat_filter)
                    .order_by(*order_by_clauses)
                    .options(selected_properties)
                    .offset(offset))
            else:
                results = (session.query(self.table_model)
                           .filter(property_filters)
                           .filter(cql_filters)
                           .filter(spat_filter)
                           .order_by(*order_by_clauses)
                           .options(selected_properties)
                           .offset(offset))

            matched = results.count()
            if limit < matched:
                returned = limit
            else:
                returned = matched

            LOGGER.debug(f'Found {matched} result(s)')

            LOGGER.debug('Preparing response')
            response = {
                'type': 'FeatureCollection',
                'features': [],
                'numberMatched': matched,
                'numberReturned': returned,
                'crs': {
                    'type': 'name',
                    'properties': {
                        'name': f'urn:ogc:def:crs:EPSG::{out_crs}'
                    }
                }
            }

            if resulttype == "hits" or not results:
                response['numberReturned'] = 0
                return response
            for item in results.limit(limit):
                if clip > 0 and geom_wkt:
                    # Default to feature, with item[0]
                    obj = self._sqlalchemy_to_feature(item[0], crs_transform_out)  # noqa

                    # Do more with say item[1] (clipped geometry already in
                    # correct reference system)
                    shapely_geom = to_shape(item[1])
                    geojson_geom = shapely.geometry.mapping(shapely_geom)

                    if clip == 2:
                        # Store as enhanced attribute in the geojson
                        obj['geometry_clipped'] = geojson_geom

                    else:
                        # Override
                        obj['geometry'] = geojson_geom
                    response['features'].append(obj)

                else:
                    # Default
                    response['features'].append(
                        self._sqlalchemy_to_feature(item, crs_transform_out)
                    )

        return response

    def get_fields(self):
        """
        Return fields (columns) from PostgreSQL table

        :returns: dict of fields
        """
        LOGGER.debug('Get available fields/properties')

        # sql-schema only allows these types, so we need to map from sqlalchemy
        # string, number, integer, object, array, boolean, null,
        # https://json-schema.org/understanding-json-schema/reference/type.html
        column_type_map = {
            str: 'string',
            float: 'number',
            int: 'integer',
            bool: 'boolean',
        }
        default_value = 'string'

        def _column_type_to_json_schema_type(column_type):
            try:
                python_type = column_type.python_type
            except NotImplementedError:
                LOGGER.warning(f'Unsupported column type {column_type}')
                return default_value
            else:
                try:
                    return column_type_map[python_type]
                except KeyError:
                    LOGGER.warning(f'Unsupported column type {column_type}')
                    return default_value

        return {
            str(column.name): {
                'type': _column_type_to_json_schema_type(column.type)
            }
            for column in self.table_model.__table__.columns
            if column.name != self.geom  # Exclude geometry column
        }

    def get_srid(self):
        """
        Return the srid of the underlying table

        :returns: integer of the SRID
        """
        LOGGER.debug('Get SRID')

        # Execute query within self-closing database Session context
        srid = None
        with Session(self._engine) as session:
            srid = session.scalar(
                Find_SRID(self.schema, self.table, self.geom))
        return srid

    def get(self, identifier, crs_transform_spec=None, **kwargs):
        """
        Query the provider for a specific
        feature id e.g: /collections/hotosm_bdi_waterways/items/13990765

        :param identifier: feature id
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """
        LOGGER.debug(f'Get item by ID: {identifier}')

        # Execute query within self-closing database Session context
        with Session(self._engine) as session:
            # Retrieve data from database as feature
            query = session.query(self.table_model)
            item = query.get(identifier)
            if item is None:
                msg = f"No such item: {self.id_field}={identifier}."
                raise ProviderItemNotFoundError(msg)
            crs_transform_out = self._get_crs_transform(crs_transform_spec)
            feature = self._sqlalchemy_to_feature(item, crs_transform_out)

            # Drop non-defined properties
            if self.properties:
                props = feature['properties']
                dropping_keys = deepcopy(props).keys()
                for item in dropping_keys:
                    if item not in self.properties:
                        props.pop(item)

            # Add fields for previous and next items
            id_field = getattr(self.table_model, self.id_field)
            prev_item = (session.query(self.table_model)
                         .order_by(id_field.desc())
                         .filter(id_field < identifier)
                         .first())
            next_item = (session.query(self.table_model)
                         .order_by(id_field.asc())
                         .filter(id_field > identifier)
                         .first())
            feature['prev'] = (getattr(prev_item, self.id_field)
                               if prev_item is not None else identifier)
            feature['next'] = (getattr(next_item, self.id_field)
                               if next_item is not None else identifier)

        return feature

    def _store_db_parameters(self, parameters, options):
        self.db_user = parameters.get('user')
        self.db_host = parameters.get('host')
        self.db_port = parameters.get('port', 5432)
        self.db_name = parameters.get('dbname')
        self.db_search_path = parameters.get('search_path', ['public'])
        self.schema = self.db_search_path[0]
        self._db_password = parameters.get('password')
        self.db_options = options

    def _is_table_spatial(self):
        """
        Test if a table is spatial by checking if he table name is present
        in the geometry_column table
        """

        with Session(self._engine) as session:
            sql = f"SELECT count(*) FROM geometry_columns " \
                  f"                WHERE f_table_schema = '{self.schema}'" \
                  f"                      AND f_table_name = '{self.table}';"
            result = session.execute(sql)
            nb_rows = result.scalar()
            if nb_rows != 0:
                is_spatial = True  # Cursor is empty (the table is not spatial)
            else:
                is_spatial = False

        return is_spatial

    def _get_engine_and_table_model(self):
        """
        Create a SQL Alchemy engine for the database and reflect the table
        model.  Use existing versions from stores if available to allow reuse
        of Engine connection pool and save expensive table reflection.
        """
        # One long-lived engine is used per database URL:
        # https://docs.sqlalchemy.org/en/14/core/connections.html#basic-usage
        engine_store_key = (self.db_user, self.db_host, self.db_port,
                            self.db_name)
        try:
            engine = _ENGINE_STORE[engine_store_key]
        except KeyError:
            conn_str = URL.create(
                'postgresql+psycopg2',
                username=self.db_user,
                password=self._db_password,
                host=self.db_host,
                port=self.db_port,
                database=self.db_name
            )
            conn_args = {
                'client_encoding': 'utf8',
                'application_name': 'pygeoapi'
            }
            if self.db_options:
                conn_args.update(self.db_options)
            engine = create_engine(
                conn_str,
                connect_args=conn_args,
                pool_pre_ping=True)
            _ENGINE_STORE[engine_store_key] = engine

        # Reuse table model if one exists
        table_model_store_key = (self.db_host, self.db_port, self.db_name,
                                 self.table)
        try:
            table_model = _TABLE_MODEL_STORE[table_model_store_key]
        except KeyError:
            table_model = self._reflect_table_model(engine)
            _TABLE_MODEL_STORE[table_model_store_key] = table_model

        return engine, table_model

    def _reflect_table_model(self, engine):
        """
        Reflect database metadata to create a SQL Alchemy model corresponding
        to target table.  This requires a database query and is expensive to
        perform.
        """
        metadata = MetaData(engine)

        # Look for table in the first schema in the search path
        try:
            metadata.reflect(schema=self.schema, only=[self.table], views=True)
        except OperationalError:
            msg = (f"Could not connect to {repr(engine.url)} "
                   "(password hidden).")
            raise ProviderConnectionError(msg)
        except InvalidRequestError:
            msg = (f"Table '{self.table}' not found in schema '{self.schema}' "
                   f"on {repr(engine.url)}.")
            raise ProviderQueryError(msg)

        # Create SQLAlchemy model from reflected table
        # It is necessary to add the primary key constraint because SQLAlchemy
        # requires it to reflect the table, but a view in a PostgreSQL database
        # does not have a primary key defined.
        sqlalchemy_table_def = metadata.tables[f'{self.schema}.{self.table}']

        try:
            sqlalchemy_table_def.append_constraint(
                PrimaryKeyConstraint(self.id_field)
            )
        except KeyError:
            msg = (f"No such id_field column ({self.id_field}) on "
                   f"{self.schema}.{self.table}.")
            raise ProviderQueryError(msg)

        Base = automap_base(metadata=metadata)
        Base.prepare(
            name_for_scalar_relationship=self._name_for_scalar_relationship,
        )
        TableModel = getattr(Base.classes, self.table)

        return TableModel

    @staticmethod
    def _name_for_scalar_relationship(
            base, local_cls, referred_cls, constraint,
    ):
        """Function used when automapping classes and relationships from
        database schema and fixes potential naming conflicts.
        """
        name = referred_cls.__name__.lower()
        local_table = local_cls.__table__
        if name in local_table.columns:
            newname = name + '_'
            LOGGER.debug(
                f'Already detected column name {name!r} in table '
                f'{local_table!r}. Using {newname!r} for relationship name.'
            )
            return newname
        return name

    def _sqlalchemy_to_feature(self, item, crs_transform_out=None):
        feature = {
            'type': 'Feature'
        }

        # Add properties from item
        item_dict = item.__dict__
        item_dict.pop('_sa_instance_state')  # Internal SQLAlchemy metadata
        feature['properties'] = item_dict
        feature['id'] = item_dict.pop(self.id_field)

        # Convert geometry to GeoJSON style
        if feature['properties'].get(self.geom):
            wkb_geom = feature['properties'].pop(self.geom)
            shapely_geom = to_shape(wkb_geom)
            if not shapely_geom.is_empty:
                if crs_transform_out is not None:
                    shapely_geom = crs_transform_out(shapely_geom)
                geojson_geom = shapely.geometry.mapping(shapely_geom)
                feature['geometry'] = geojson_geom
            else:
                feature['geometry'] = None
        else:
            feature['geometry'] = None

        return feature

    def _get_order_by_clauses(self, sort_by, table_model):
        # Build sort_by clauses if provided
        clauses = []
        for sort_by_dict in sort_by:
            model_column = getattr(table_model, sort_by_dict['property'])
            order_function = asc if sort_by_dict['order'] == '+' else desc
            clauses.append(order_function(model_column))

        # Otherwise sort by primary key (to ensure reproducible output)
        if not clauses:
            clauses.append(asc(getattr(table_model, self.id_field)))

        return clauses

    def _get_cql_filters(self, filterq):
        if not filterq:
            return True  # Let everything through

        # Convert filterq into SQL Alchemy filters
        field_mapping = {
            column_name: getattr(self.table_model, column_name)
            for column_name in self.table_model.__table__.columns.keys()}
        cql_filters = to_filter(filterq, field_mapping)

        return cql_filters

    def _get_property_filters(self, properties):
        if not properties:
            return True  # Let everything through

        # Convert property filters into SQL Alchemy filters
        # Based on https://stackoverflow.com/a/14887813/3508733
        filter_group = []
        for column_name, value in properties:
            column = getattr(self.table_model, column_name)
            filter_group.append(column == value)
        property_filters = and_(*filter_group)

        return property_filters

    def _get_spatial_filter(self, bbox, bbox_crs, geom_wkt, geom_crs):

        if self.srid:
            # If a geom is specified
            query_shape = None
            if geom_wkt:
                # If a geom_crs is specified
                if geom_crs:
                    # Make the polygon from wkt
                    query_shape = ST_MakeValid(ST_PolygonFromText(geom_wkt, geom_crs))  # noqa

                    # Project the geometry to the SRID of the table
                    query_shape = ST_Transform(query_shape, self.srid)

                else:
                    # Make the polygon from wkt
                    query_shape = ST_PolygonFromText(geom_wkt)

            elif bbox:
                # If a bbox_crs is specified
                if bbox_crs:
                    # Append the srid to the bbox coordinates
                    bbox.append(int(bbox_crs))

                    # Make the bbox envelope with the provided crs
                    query_shape = ST_MakeEnvelope(*bbox)

                    # Project the bbox's to the SRID of the table
                    query_shape = ST_Transform(query_shape, self.srid)

                else:
                    # Make the bbox envelope assuming the same crs as the data
                    query_shape = ST_MakeEnvelope(*bbox)

            else:
                return True  # Let everything through

        else:
            return True  # No SRID ==> No geometry ==> No spatial filtering

        geom_column = getattr(self.table_model, self.geom)
        return geom_column.ST_Intersects(query_shape)

    def _select_properties_clause(self, select_properties, skip_geometry):
        # List the column names that we want
        if select_properties:
            column_names = set(select_properties)
        else:
            # get_fields() doesn't include geometry column
            column_names = set(self.fields.keys())

        if self.properties:  # optional subset of properties defined in config
            properties_from_config = set(self.properties)
            column_names = column_names.intersection(properties_from_config)

        if not skip_geometry:
            column_names.add(self.geom)

        # Convert names to SQL Alchemy clause
        selected_columns = []
        for column_name in column_names:
            try:
                column = getattr(self.table_model, column_name)
                selected_columns.append(column)
            except AttributeError:
                pass  # Ignore non-existent columns
        selected_properties_clause = load_only(*selected_columns)

        return selected_properties_clause

    def _get_crs_transform(self, crs_transform_spec=None):
        if crs_transform_spec is not None:
            crs_transform = get_transform_from_crs(
                pyproj.CRS.from_wkt(crs_transform_spec.source_crs_wkt),
                pyproj.CRS.from_wkt(crs_transform_spec.target_crs_wkt),
            )
        else:
            crs_transform = None
        return crs_transform
