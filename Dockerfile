# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#          Just van den Broecke <justb4@gmail.com>
#          Francesco Bartoli <xbartolone@gmail.com>
#          Angelos Tzotsos <gcpp.kalxas@gmail.com>
#
# Copyright (c) 2020 Tom Kralidis
# Copyright (c) 2019 Just van den Broecke
# Copyright (c) 2020 Francesco Bartoli
# Copyright (c) 2021 Angelos Tzotsos
# Copyright (c) 2023 Bernhard Mallinger
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

FROM ubuntu:jammy

LABEL maintainer="Just van den Broecke <justb4@gmail.com>"

# Docker file for full geoapi server with libs/packages for all providers.
# Server runs with gunicorn. You can override ENV settings.
# Defaults:
# SCRIPT_NAME=/
# CONTAINER_NAME=pygeoapi
# CONTAINER_HOST=0.0.0.0
# CONTAINER_PORT=80
# WSGI_WORKERS=4
# WSGI_WORKER_TIMEOUT=6000
# WSGI_WORKER_CLASS=gevent

# Calls entrypoint.sh to run. Inspect it for options.
# Contains some test data. Also allows you to verify by running all unit tests.
# Simply run: docker run -it geopython/pygeoapi test
# Override the default config file /pygeoapi/local.config.yml
# via Docker Volume mapping or within a docker-compose.yml file. See example at
# https://github.com/geopython/demo.pygeoapi.io/tree/master/services/pygeoapi

# Build arguments
# add "--build-arg BUILD_DEV_IMAGE=true" to Docker build command when building with test/doc tools

# ARGS
ARG TZ="Etc/UTC"
ARG LANG="en_US.UTF-8"
ARG ADD_DEB_PACKAGES="\
    libsqlite3-mod-spatialite \
    python3-dask \
    python3-elasticsearch \
    python3-fiona \
    python3-gdal \
    python3-jsonpatch \
    python3-netcdf4 \
    python3-pandas \
    python3-psycopg2 \
    python3-pymongo \
    python3-pyproj \
    python3-rasterio \
    python3-scipy \
    python3-shapely \
    python3-tinydb \
    python3-xarray \
    python3-zarr \
    python3-mapscript \
    python3-pytest \
    python3-pyld"

# ENV settings
ENV TZ=${TZ} \
    LANG=${LANG} \
    DEBIAN_FRONTEND="noninteractive" \
    DEB_BUILD_DEPS="\
    curl \
    unzip" \
    DEB_PACKAGES="\
    locales \
    tzdata \
    gunicorn \
    python3-dateutil \
    python3-gevent \
    python3-greenlet \
    python3-pip \
    python3-tz \
    python3-unicodecsv \
    python3-yaml \
    ${ADD_DEB_PACKAGES}"

WORKDIR /pygeoapi

# Set configuration for environment
ARG AWS_ENV=dev

# Install operating system dependencies
RUN \
    apt-get update -y \
    && apt-get --no-install-recommends install -y ${DEB_PACKAGES} ${DEB_BUILD_DEPS}  \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
    && echo "For ${TZ} date=$(date)" && echo "Locale=$(locale)"  \

    # temporary remove
    # && add-apt-repository ppa:ubuntugis/ubuntugis-unstable \

    # OGC schemas local setup
    && mkdir /schemas.opengis.net \
    && curl -O http://schemas.opengis.net/SCHEMAS_OPENGIS_NET.zip \
    && unzip ./SCHEMAS_OPENGIS_NET.zip "ogcapi/*" -d /schemas.opengis.net \
    && rm -f ./SCHEMAS_OPENGIS_NET.zip \

    # Install remaining pygeoapi deps
    && pip3 install -r requirements.txt \
    && pip3 install -r requirements-docker.txt \

    # Install pygeoapi
    && pip3 install --no-cache-dir -e . \

    # Cleanup TODO: remove unused Locales and TZs
    # NOTE: this tries to remove gcc, but the actual package gcc-11 can't be
    #       removed because python3-scipy depends on python3-pythran which
    #       depends on g++
    && apt-get remove --purge -y gcc ${DEB_BUILD_DEPS} \
    && apt-get clean \
    && apt autoremove -y  \
    && rm -rf /var/lib/apt/lists/*

# Install SSL and NGINX Configuration dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    curl \
    awscli \
    nginx-core && \
    rm -rf /var/lib/apt/lists/*

# Configure SSL
RUN mkdir /gccp
COPY ssl/get-ssl-cert.sh /gccp/get-ssl-cert.sh
COPY ssl/run-get-ssl-cert.sh /gccp/run-get-ssl-cert.sh
RUN chmod +x /gccp/run-get-ssl-cert.sh /gccp/get-ssl-cert.sh
RUN /gccp/run-get-ssl-cert.sh
RUN rm -rf /gccp

# Configure NGINX
RUN unlink /etc/nginx/sites-enabled/default
COPY nginx/nginx.conf /etc/nginx/nginx.conf
COPY nginx/server-api /etc/nginx/sites-available/server-api
RUN ln -s /etc/nginx/sites-available/server-api /etc/nginx/sites-enabled/server-api

# Set default config and entrypoint for Docker Image
COPY docker/entrypoint.sh /entrypoint.sh
COPY local.config.${AWS_ENV}.yml /pygeoapi/local.config.yml
COPY local.openapi.${AWS_ENV}.yml /pygeoapi/local.openapi.yml

# Set permissions and expose ports
RUN chmod -R 1777 /pygeoapi && \
    chown -R www-data:www-data /pygeoapi
EXPOSE 443/tcp

RUN chmod +x /entrypoint.sh

CMD service nginx start && /entrypoint.sh
