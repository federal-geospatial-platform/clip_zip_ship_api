# pygeoapi

[![DOI](https://zenodo.org/badge/121585259.svg)](https://zenodo.org/badge/latestdoi/121585259)
[![Build](https://github.com/geopython/pygeoapi/actions/workflows/main.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/main.yml)
[![Docker](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml)

[pygeoapi](https://pygeoapi.io) is a Python server implementation of the [OGC API](https://ogcapi.ogc.org) suite of standards. The project emerged as part of the next generation OGC API efforts in 2018 and provides the capability for organizations to deploy a RESTful OGC API endpoint using OpenAPI, GeoJSON, and HTML. pygeoapi is [open source](https://opensource.org/) and released under an [MIT license](https://github.com/geopython/pygeoapi/blob/master/LICENSE.md).

Please read the docs at [https://docs.pygeoapi.io](https://docs.pygeoapi.io) for more information.

# This Fork
This particular fork aims to implement specific NRCan-related business requirements as part of the pygeoapi community efforts. As those NRCan-features are implemented, a great effort is made to try to separate what is NRCan-related-business and what can be pushed back in the pygeoapi core. To achieve this, class inheritance is key.
For example:
 - In `pygeoapi/pygeoapi/` there are 2 API classes: (1) `api.py` and (2) `api_czs.py`. The former has limited modifications to make sure it could eventually be pushed back for everyone using pygeoapi to benefit. The later inherits from the former and adds NRCan-related business logic. 
 - In `pygeoapi/pygeoapi/process/` there are 2 new process classes: (1) `extract.py` and (2) `extract_nrcan.py`. The former adds a process that could eventually be pushed back for everyone using pygeoapi to benefit. The later inherits from the former and adds NRCan-related business logic.

This fork also implements custom features for:
- Querying enhancements when using the rasterio provider:
  - Such as sending a geometry wkt and geometry crs
  
- Querying enhancements when using the postgresql provider:
  - Such as sending a geometry wkt and geometry crs (at the time of this writing, the cql filter isn't working to query using other crs than 4326)
  - Returning not only the features, but also the clipped features when wanted
