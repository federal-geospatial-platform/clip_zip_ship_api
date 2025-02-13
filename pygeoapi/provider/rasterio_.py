# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2022 Tom Kralidis
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

import logging

from pyproj import CRS, Transformer
import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import (reproject, calculate_default_transform)
from rasterio.enums import Resampling
import rasterio.mask

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderQueryError)
from pygeoapi.util import read_data
import shapely

LOGGER = logging.getLogger(__name__)


class RasterioProvider(BaseProvider):
    """Rasterio Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.rasterio_.RasterioProvider
        """

        super().__init__(provider_def)

        try:
            self._data = rasterio.open(self.data)
            self._coverage_properties = self._get_coverage_properties()
            self.axes = self._coverage_properties['axes']
            self.crs = self._coverage_properties['bbox_crs']
            self.num_bands = self._coverage_properties['num_bands']
            self.fields = [str(num) for num in range(1, self.num_bands+1)]
            self.native_format = provider_def['format']['name']
        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def get_coverage_domainset(self, *args, **kwargs):
        """
        Provide coverage domainset
        :returns: CIS JSON object of domainset metadata
        """

        domainset = {
            'type': 'DomainSet',
            'generalGrid': {
                'type': 'GeneralGridCoverage',
                'srsName': self._coverage_properties['bbox_crs'],
                'axisLabels': [
                    self._coverage_properties['x_axis_label'],
                    self._coverage_properties['y_axis_label']
                ],
                'axis': [{
                    'type': 'RegularAxis',
                    'axisLabel': self._coverage_properties['x_axis_label'],
                    'lowerBound': self._coverage_properties['bbox'][0],
                    'upperBound': self._coverage_properties['bbox'][2],
                    'uomLabel': self._coverage_properties['bbox_units'],
                    'resolution': self._coverage_properties['resx']
                }, {
                    'type': 'RegularAxis',
                    'axisLabel': self._coverage_properties['y_axis_label'],
                    'lowerBound': self._coverage_properties['bbox'][1],
                    'upperBound': self._coverage_properties['bbox'][3],
                    'uomLabel': self._coverage_properties['bbox_units'],
                    'resolution': self._coverage_properties['resy']
                }],
                'gridLimits': {
                    'type': 'GridLimits',
                    'srsName': 'http://www.opengis.net/def/crs/OGC/0/Index2D',
                    'axisLabels': ['i', 'j'],
                    'axis': [{
                        'type': 'IndexAxis',
                        'axisLabel': 'i',
                        'lowerBound': 0,
                        'upperBound': self._coverage_properties['width']
                    }, {
                        'type': 'IndexAxis',
                        'axisLabel': 'j',
                        'lowerBound': 0,
                        'upperBound': self._coverage_properties['height']
                    }]
                }
            },
            '_meta': {
                'tags': self._coverage_properties['tags']
            }
        }

        return domainset

    def get_coverage_rangetype(self, *args, **kwargs):
        """
        Provide coverage rangetype
        :returns: CIS JSON object of rangetype metadata
        """

        rangetype = {
            'type': 'DataRecord',
            'field': []
        }

        for i, dtype, nodataval in zip(self._data.indexes, self._data.dtypes,
                                       self._data.nodatavals):
            LOGGER.debug(f'Determing rangetype for band {i}')

            name, units = None, None
            if self._data.units[i-1] is None:
                parameter = _get_parameter_metadata(
                    self._data.profile['driver'], self._data.tags(i))
                name = parameter['description']
                units = parameter['unit_label']

            rangetype['field'].append({
                'id': i,
                'type': 'Quantity',
                'name': name,
                'encodingInfo': {
                    'dataType': f'http://www.opengis.net/def/dataType/OGC/0/{dtype}'  # noqa
                },
                'nodata': nodataval,
                'uom': {
                    'id': f'http://www.opengis.net/def/uom/UCUM/{units}',
                    'type': 'UnitReference',
                    'code': units
                },
                '_meta': {
                    'tags': self._data.tags(i)
                }
            })

        return rangetype

    def query(self, properties=[], subsets={}, bbox=None, bbox_crs=4326,
              geom=None, geom_crs: int = 4326, out_crs: int = None,
              datetime_=None, format_='json', **kwargs):
        """
        Extract data from collection
        :param properties: list of bands
        :param subsets: dict of subset names with lists of ranges
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param bbox_crs: bounding box crs
        :param geom: geometry as wkt
        :param geom_crs: geometry crs
        :param out_crs: output crs when expecting a reprojection
        :param datetime_: temporal (datestamp or extent)
        :param format_: data format of output

        :returns: coverage data as dict of CoverageJSON or native format
        """

        bands = properties
        LOGGER.debug(f'Bands: {bands}, subsets: {subsets}')

        args = {
            'indexes': None
        }
        shapes = []

        if not bbox:
            bbox = []

        if all([not bands, not subsets, not bbox, not geom, format_ != 'json']):  # noqa
            LOGGER.debug('No parameters specified, returning native data')
            return read_data(self.data)

        if all([self._coverage_properties['x_axis_label'] in subsets,
                self._coverage_properties['y_axis_label'] in subsets,
                len(bbox) > 0]):
            msg = 'bbox and subsetting by coordinates are exclusive'
            LOGGER.warning(msg)
            raise ProviderQueryError(msg)

        if geom:
            # Load the wkt as a shapes (GeoJSON)
            shapes = shapely.wkt.loads(geom)

            crs_src = CRS.from_epsg(geom_crs)

            if self.options and 'crs' in self.options:
                crs_dest = CRS.from_string(self.options['crs'])
            else:
                crs_dest = self._data.crs

            if crs_src == crs_dest:
                LOGGER.debug('source geom CRS and data CRS are the same')

                # Make it as GeoJSON
                shapes = shapely.geometry.mapping(shapes)

            else:
                LOGGER.debug('source geom CRS and data CRS are different')
                LOGGER.debug('reprojecting geom into native coordinates')

                # shapely<2.0, sample code not working in shapely<2.0 :(
                # Transform
                # import pyproj
                # from functools import partial # (import this to try it..)
                # project = partial(pyproj.transform, crs_src.to_string(), crs_dest.to_string())  # noqa
                # shapes = shapely.ops.transform(project, shapes)

                # shapely>2.0
                # Transform
                project = Transformer.from_crs(crs_src, crs_dest, always_xy=True)  # noqa
                shapes = shapely.ops.transform(project.transform, shapes)

                # Store the bbox representation for rasterio's ouput
                bbox = shapes.bounds

                # Make it as GeoJSON
                shapes = shapely.geometry.mapping(shapes)

            # Make it an array
            shapes = [shapes]

        elif len(bbox) > 0:
            minx, miny, maxx, maxy = bbox

            crs_src = CRS.from_epsg(bbox_crs)

            if self.options and 'crs' in self.options:
                crs_dest = CRS.from_string(self.options['crs'])
            else:
                crs_dest = self._data.crs

            if crs_src == crs_dest:
                LOGGER.debug('source bbox CRS and data CRS are the same')
                shapes = [{
                   'type': 'Polygon',
                   'coordinates': [[
                       [minx, miny],
                       [minx, maxy],
                       [maxx, maxy],
                       [maxx, miny],
                       [minx, miny],
                   ]]
                }]
            else:
                LOGGER.debug('source bbox CRS and data CRS are different')
                LOGGER.debug('reprojecting bbox into native coordinates')

                t = Transformer.from_crs(crs_src, crs_dest, always_xy=True)
                minx2, miny2 = t.transform(minx, miny)
                maxx2, maxy2 = t.transform(maxx, maxy)

                LOGGER.debug(f'Source coordinates: {minx}, {miny}, {maxx}, {maxy}')  # noqa
                LOGGER.debug(f'Destination: {minx2}, {miny2}, {maxx2}, {maxy2}')  # noqa

                shapes = [{
                   'type': 'Polygon',
                   'coordinates': [[
                       [minx2, miny2],
                       [minx2, maxy2],
                       [maxx2, maxy2],
                       [maxx2, miny2],
                       [minx2, miny2],
                   ]]
                }]

        elif (self._coverage_properties['x_axis_label'] in subsets and
                self._coverage_properties['y_axis_label'] in subsets):
            LOGGER.debug('Creating spatial subset')

            x = self._coverage_properties['x_axis_label']
            y = self._coverage_properties['y_axis_label']

            shapes = [{
               'type': 'Polygon',
               'coordinates': [[
                   [subsets[x][0], subsets[y][0]],
                   [subsets[x][0], subsets[y][1]],
                   [subsets[x][1], subsets[y][1]],
                   [subsets[x][1], subsets[y][0]],
                   [subsets[x][0], subsets[y][0]]
               ]]
            }]

        if bands:
            LOGGER.debug('Selecting bands')
            args['indexes'] = list(map(int, bands))

        with rasterio.open(self.data) as _data:
            LOGGER.debug('Creating output coverage metadata')
            out_meta = _data.meta

            if self.options is not None:
                LOGGER.debug('Adding dataset options')
                for key, value in self.options.items():
                    out_meta[key] = value

            if shapes:  # spatial subset
                try:
                    LOGGER.debug('Clipping data spatially')

                    # Query
                    out_image, out_transform = rasterio.mask.mask(
                        _data,
                        filled=False,
                        shapes=shapes,
                        crop=True,
                        indexes=args['indexes'])

                except ValueError as err:
                    LOGGER.error(err)
                    raise ProviderQueryError(err)

                update_params = {'driver': self.native_format,
                                 'height': out_image.shape[1],
                                 'width': out_image.shape[2],
                                 'transform': out_transform}
                if 'compression' in kwargs:
                    update_params['compress'] = kwargs['compression']
                out_meta.update(update_params)
            else:  # no spatial subset
                LOGGER.debug('Creating data in memory with band selection')
                out_image = _data.read(indexes=args['indexes'])

            if bbox:
                out_meta['bbox'] = [bbox[0], bbox[1], bbox[2], bbox[3]]
            elif shapes:
                out_meta['bbox'] = [
                    subsets[x][0], subsets[y][0],
                    subsets[x][1], subsets[y][1]
                ]
            else:
                out_meta['bbox'] = [
                    _data.bounds.left,
                    _data.bounds.bottom,
                    _data.bounds.right,
                    _data.bounds.top
                ]

            out_meta['units'] = _data.units

            # If returning json
            if format_ == 'json':
                LOGGER.debug('Creating output in CoverageJSON')
                out_meta['bands'] = args['indexes']
                return self.gen_covjson(out_meta, out_image)

            # Serialize in memory to return data in native format
            LOGGER.debug('Serializing data in memory')

            # If we have to reproject the image to another destination CRS
            if out_crs:
                # Use another MemoryFile to do a reprojection
                LOGGER.debug('Returning data in native format and reprojected')
                with MemoryFile() as memfile:
                    with memfile.open(**out_meta) as dest:
                        # Write the result
                        dest.write(out_image)

                        # Create destination memory file
                        with MemoryFile() as memfile_proj:
                            # Reproject
                            self.reproject_data_to_memory_file(dest, memfile_proj, out_crs, kwargs['compression'])  # noqa

                            # Return the reprojected image
                            return memfile_proj.read()

            else:
                # Use a single memory file and return as is
                LOGGER.debug('Returning data in native format and native projection')  # noqa
                with MemoryFile() as memfile:
                    with memfile.open(**out_meta) as dest:
                        dest.write(out_image)
                    return memfile.read()

    def reproject_data_to_memory_file(self, dataset_src, memoryfile_dest: MemoryFile, out_crs: int, compression: str):  # noqa
        # Create the CRS
        crs = CRS.from_epsg(out_crs)
        transform, width, height = calculate_default_transform(dataset_src.crs, crs, dataset_src.width, dataset_src.height, *dataset_src.bounds)  # noqa
        out_meta = dataset_src.meta.copy()

        update_params = {
            'crs': crs,
            'transform': transform,
            'width': width,
            'height': height}
        if compression:
            update_params['compress'] = compression
        out_meta.update(update_params)

        with memoryfile_dest.open(**out_meta) as dest_proj:
            for i in range(1, dataset_src.count + 1):
                reproject(
                    source=rasterio.band(dataset_src, i),
                    destination=rasterio.band(dest_proj, i),
                    src_transform=dataset_src.transform,
                    src_crs=dataset_src.crs,
                    dst_transform=transform,
                    dst_crs=crs,
                    resampling=Resampling.nearest)

    def gen_covjson(self, metadata, data):
        """
        Generate coverage as CoverageJSON representation
        :param metadata: coverage metadata
        :param data: rasterio DatasetReader object
        :returns: dict of CoverageJSON representation
        """

        LOGGER.debug('Creating CoverageJSON domain')
        minx, miny, maxx, maxy = metadata['bbox']

        cj = {
            'type': 'Coverage',
            'domain': {
                'type': 'Domain',
                'domainType': 'Grid',
                'axes': {
                    'x': {
                        'start': minx,
                        'stop': maxx,
                        'num': metadata['width']
                    },
                    'y': {
                        'start': maxy,
                        'stop': miny,
                        'num': metadata['height']
                    }
                },
                'referencing': [{
                    'coordinates': ['x', 'y'],
                    'system': {
                        'type': self._coverage_properties['crs_type'],
                        'id': self._coverage_properties['bbox_crs']
                    }
                }]
            },
            'parameters': {},
            'ranges': {}
        }

        if metadata['bands'] is None:  # all bands
            bands_select = range(1, len(self._data.dtypes) + 1)
        else:
            bands_select = metadata['bands']

        LOGGER.debug(f'bands selected: {bands_select}')
        for bs in bands_select:
            pm = _get_parameter_metadata(
                self._data.profile['driver'], self._data.tags(bs))

            parameter = {
                'type': 'Parameter',
                'description': pm['description'],
                'unit': {
                    'symbol': pm['unit_label']
                },
                'observedProperty': {
                    'id': pm['observed_property_id'],
                    'label': {
                        'en': pm['observed_property_name']
                    }
                }
            }

            cj['parameters'][pm['id']] = parameter

        try:
            for key in cj['parameters'].keys():
                cj['ranges'][key] = {
                    'type': 'NdArray',
                    # 'dataType': metadata.dtypes[0],
                    'dataType': 'float',
                    'axisNames': ['y', 'x'],
                    'shape': [metadata['height'], metadata['width']],
                }
                # TODO: deal with multi-band value output
                cj['ranges'][key]['values'] = data.flatten().tolist()
        except IndexError as err:
            LOGGER.warning(err)
            raise ProviderQueryError('Invalid query parameter')

        return cj

    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties
        :returns: `dict` of coverage properties
        """

        properties = {
            'bbox': [
                self._data.bounds.left,
                self._data.bounds.bottom,
                self._data.bounds.right,
                self._data.bounds.top
            ],
            'bbox_crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'crs_type': 'GeographicCRS',
            'bbox_units': 'deg',
            'x_axis_label': 'Long',
            'y_axis_label': 'Lat',
            'width': self._data.width,
            'height': self._data.height,
            'resx': self._data.res[0],
            'resy': self._data.res[1],
            'num_bands': self._data.count,
            'tags': self._data.tags()
        }

        if self._data.crs is not None:
            if self._data.crs.is_projected:
                properties['bbox_crs'] = f'http://www.opengis.net/def/crs/OGC/1.3/{self._data.crs.to_epsg()}'  # noqa
                properties['x_axis_label'] = 'x'
                properties['y_axis_label'] = 'y'
                properties['bbox_units'] = self._data.crs.linear_units
                properties['crs_type'] = 'ProjectedCRS'

        properties['axes'] = [
            properties['x_axis_label'], properties['y_axis_label']
        ]

        return properties


def _get_parameter_metadata(driver, band):
    """
    Helper function to derive parameter name and units
    :param driver: rasterio/GDAL driver name
    :param band: int of band number
    :returns: dict of parameter metadata
    """

    parameter = {
        'id': None,
        'description': None,
        'unit_label': None,
        'unit_symbol': None,
        'observed_property_id': None,
        'observed_property_name': None
    }

    if driver == 'GRIB':
        parameter['id'] = band['GRIB_ELEMENT']
        parameter['description'] = band['GRIB_COMMENT']
        parameter['unit_label'] = band['GRIB_UNIT']
        parameter['unit_symbol'] = band['GRIB_UNIT']
        parameter['observed_property_id'] = band['GRIB_SHORT_NAME']
        parameter['observed_property_name'] = band['GRIB_COMMENT']

    return parameter
