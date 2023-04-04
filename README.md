# pygeoapi

[![Build](https://github.com/geopython/pygeoapi/actions/workflows/main.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/main.yml)
[![Docker](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml)

[pygeoapi](https://pygeoapi.io) is a Python server implementation of the [OGC API](https://ogcapi.ogc.org) suite of standards. The project emerged as part of the next generation OGC API efforts in 2018 and provides the capability for organizations to deploy a RESTful OGC API endpoint using OpenAPI, GeoJSON, and HTML. pygeoapi is [open source](https://opensource.org/) and released under an [MIT license](https://github.com/geopython/pygeoapi/blob/master/LICENSE.md).

Please read the docs at [https://docs.pygeoapi.io](https://docs.pygeoapi.io) for more information.

# This Fork
This particular fork aims to implement specific NRCan-related business requirements as part of the pygeoapi community efforts. As those NRCan-features are implemented, a great effort is made to try to separate what is NRCan-related-business and what can be pushed back in the pygeoapi core. To achieve this, class inheritance is key.
For example:
 - In `pygeoapi/pygeoapi/` there are 2 API classes: (1) `api.py` and (2) `api_czs.py`. The former has limited modifications to make sure it could eventually be pushed back for everyone using pygeoapi to benefit. The later inherits from the former and adds NRCan-related business logic. 
 - In `pygeoapi/pygeoapi/process/` there are 2 new process classes: (1) `extract.py` and (2) `extract_nrcan.py`. The former adds a process that could eventually be pushed back for everyone using pygeoapi to benefit. The later inherits from the former and adds NRCan-related business logic.

This fork also:
- Implemented custom features for querying using the rasterio and postgresql providers such as sending a custom crs, sending a geometry wkt (instead of a bbox), returning not only the features, but the clipped features when specified (for the postgresql provider), and more..
