'''
    Summary
    -------

    This class facilitates reading of a geonetwork result metadata information.
'''

import sys, os, logging, json, requests, xmltodict
from urllib.parse import urlparse


#URL_PREFIX = "https://gcgeo.gc.ca/geonetwork/metadata/eng/"
URL_GEO_NETWORK = {
    #"URL": "https://maps.canada.ca/geonetwork/srv/eng/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&outputSchema=http://www.isotc211.org/2005/gmd&typeNames=gmd:MD_Metadata&constraintLanguage=FILTER&id={metadata_uuid}",
    "ROOT": "csw:GetRecordByIdResponse",
    "METADATA": "gmd:MD_Metadata",
    "FILE_IDENTIFIER": "gmd:fileIdentifier",
    "LANGUAGE": "gmd:language",
    "REFERENCE_SYSTEM_INFO": "gmd:referenceSystemInfo",
    "REFERENCE_SYSTEM_IDENTIF": "gmd:MD_ReferenceSystem",
    "RS_IDENT": "gmd:referenceSystemIdentifier",
    "RS_IDENTIF": "gmd:RS_Identifier",
    "RS_IDENTIF_CODE": "gmd:code",
    "IDENTIFY_INFO": "gmd:identificationInfo",
    "DISTRIBUTION_INFO": "gmd:distributionInfo",
    "DATA_IDENTIFY": "gmd:MD_DataIdentification",
    "EXTENT": "gmd:extent",
    "EX_EXTENT": "gmd:EX_Extent",
    "GEOGRAPHIC_ELEMENT": "gmd:geographicElement",
    "GEOGRAPHIC_BOUNDING_BOX": "gmd:EX_GeographicBoundingBox",
    "GEOGRAPHIC_BOUNDING_BOX_WEST": "gmd:westBoundLongitude",
    "GEOGRAPHIC_BOUNDING_BOX_EAST": "gmd:eastBoundLongitude",
    "GEOGRAPHIC_BOUNDING_BOX_SOUTH": "gmd:southBoundLatitude",
    "GEOGRAPHIC_BOUNDING_BOX_NORTH": "gmd:northBoundLatitude",
    "TEMPORAL_ELEMENT": "gmd:temporalElement",
    "EX_TEMPORAL_EXTENT": "gmd:EX_TemporalExtent",
    "TIME_PERIOD": "gml:TimePeriod",
    "BEGIN_POSITION": "gml:beginPosition",
    "END_POSITION": "gml:endPosition",
    "DISTRIBUTION": "gmd:MD_Distribution",
    "TRANSFER_OPT": "gmd:transferOptions",
    "TRANSFER_DIGITAL": "gmd:MD_DigitalTransferOptions",
    "TRANSFER_ONLINE": "gmd:onLine",
    "TRANSFER_ONLINE_RES": "gmd:CI_OnlineResource",
    "TRANSFER_ONLINE_RES_LINK": "gmd:linkage",
    "TRANSFER_ONLINE_RES_LINK_URL": "gmd:URL",
    "TRANSFER_ONLINE_NAME": "gmd:name",
    "TRANSFER_ONLINE_RES_DESC": "gmd:description",
    "TRANSFER_ONLINE_RES_DESC_CHAR": "gco:CharacterString",
    "TOPIC_CATEGORY": "gmd:topicCategory",
    "TOPIC_CATEGORY_CODE": "gmd:MD_TopicCategoryCode",
    "CITATION": "gmd:citation",
    "CI_CITATION": "gmd:CI_Citation",
    "TITLE": "gmd:title",
    "FREE_TEXT": "gmd:PT_FreeText",
    "TEXT_GROUP": "gmd:textGroup",
    "LOCALIZED": "gmd:LocalisedCharacterString",
    "CHAR_STRING": "gco:CharacterString",
    "DATE_STAMP": "gmd:dateStamp",
    "DATE_TIME": "gco:DateTime",
    "DATE": "gco:Date",
    "TEXT": "#text",
    "CODE_LIST_VALUE": "@codeListValue",
    "GRAPHIC_OVERVIEW": "gmd:graphicOverview",
    "BROWSE_GRAPHIC": "gmd:MD_BrowseGraphic",
    "FILE_NAME": "gmd:fileName",
    "DESC_KEYWORDS": "gmd:descriptiveKeywords",
    "KEYWORDS": "gmd:MD_Keywords",
    "KEYWORD": "gmd:keyword",
    "TYPE": "gmd:type",
    "TYPE_CODE": "gmd:MD_KeywordTypeCode",
    "DECIMAL": "gco:Decimal"
}
DETERMINANTS_EN = ["a", "an", "the"]
DETERMINANTS_FR = ["le", "la", "les", "un", "une", "des"]


class GeoNetworkReader(object):
    """
    Class representing a simplified GeoNetwork result of a query to GeoNetwork
    """

    def __init__(self, xml_content: str):
        self._xml_content = xml_content

        # Parse the XML to JSON
        responseJson = xmltodict.parse(xml_content)

        # If found
        if URL_GEO_NETWORK["ROOT"] in responseJson and \
           URL_GEO_NETWORK["METADATA"] in responseJson[URL_GEO_NETWORK["ROOT"]]:
            # Grab root
            self._meta_root = responseJson[URL_GEO_NETWORK["ROOT"]][URL_GEO_NETWORK["METADATA"]]

            # Grab the UUID
            self._uuid = _dig_node_one_value(self._meta_root, [URL_GEO_NETWORK["FILE_IDENTIFIER"],
                                                               URL_GEO_NETWORK["CHAR_STRING"]])

            # Grab the spatial reference system
            self._srid = _dig_node_one_value(self._meta_root, [URL_GEO_NETWORK["REFERENCE_SYSTEM_INFO"],
                                                               URL_GEO_NETWORK["REFERENCE_SYSTEM_IDENTIF"],
                                                               URL_GEO_NETWORK["RS_IDENT"],
                                                               URL_GEO_NETWORK["RS_IDENTIF"],
                                                               URL_GEO_NETWORK["RS_IDENTIF_CODE"],
                                                               URL_GEO_NETWORK["CHAR_STRING"]]);

            # Grab the information
            self._language = _dig_node_one_value(self._meta_root, [URL_GEO_NETWORK["LANGUAGE"],
                                                                   URL_GEO_NETWORK["CHAR_STRING"]])
            # Grab data identification root
            self._data_identif_root = _dig_node_one(self._meta_root, [URL_GEO_NETWORK["IDENTIFY_INFO"],
                                                                      URL_GEO_NETWORK["DATA_IDENTIFY"]])

            # Grab the extent
            self._extent = {}
            self._extent["west"] = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["EXTENT"],
                                                                                 URL_GEO_NETWORK["EX_EXTENT"],
                                                                                 URL_GEO_NETWORK["GEOGRAPHIC_ELEMENT"],
                                                                                 URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX"],
                                                                                 URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX_WEST"],
                                                                                 URL_GEO_NETWORK["DECIMAL"]])
            self._extent["east"] = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["EXTENT"],
                                                                                 URL_GEO_NETWORK["EX_EXTENT"],
                                                                                 URL_GEO_NETWORK["GEOGRAPHIC_ELEMENT"],
                                                                                 URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX"],
                                                                                 URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX_EAST"],
                                                                                 URL_GEO_NETWORK["DECIMAL"]])
            self._extent["south"] = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["EXTENT"],
                                                                                  URL_GEO_NETWORK["EX_EXTENT"],
                                                                                  URL_GEO_NETWORK["GEOGRAPHIC_ELEMENT"],
                                                                                  URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX"],
                                                                                  URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX_SOUTH"],
                                                                                  URL_GEO_NETWORK["DECIMAL"]])
            self._extent["north"] = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["EXTENT"],
                                                                                  URL_GEO_NETWORK["EX_EXTENT"],
                                                                                  URL_GEO_NETWORK["GEOGRAPHIC_ELEMENT"],
                                                                                  URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX"],
                                                                                  URL_GEO_NETWORK["GEOGRAPHIC_BOUNDING_BOX_NORTH"],
                                                                                  URL_GEO_NETWORK["DECIMAL"]])

            # Grab the time extent
            self._temporal_extent = {}
            self._temporal_extent["begin"] = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["EXTENT"],
                                                                                           URL_GEO_NETWORK["EX_EXTENT"],
                                                                                           URL_GEO_NETWORK["TEMPORAL_ELEMENT"],
                                                                                           URL_GEO_NETWORK["EX_TEMPORAL_EXTENT"],
                                                                                           URL_GEO_NETWORK["EXTENT"],
                                                                                           URL_GEO_NETWORK["TIME_PERIOD"],
                                                                                           URL_GEO_NETWORK["BEGIN_POSITION"]])
            self._temporal_extent["end"] = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["EXTENT"],
                                                                               URL_GEO_NETWORK["EX_EXTENT"],
                                                                               URL_GEO_NETWORK["TEMPORAL_ELEMENT"],
                                                                               URL_GEO_NETWORK["EX_TEMPORAL_EXTENT"],
                                                                               URL_GEO_NETWORK["EXTENT"],
                                                                               URL_GEO_NETWORK["TIME_PERIOD"],
                                                                               URL_GEO_NETWORK["END_POSITION"]])

            # Grab distribution root
            self._distribution_info_root = _dig_node_one(self._meta_root, [URL_GEO_NETWORK["DISTRIBUTION_INFO"],
                                                                           URL_GEO_NETWORK["DISTRIBUTION"]])

            # Grab transfer options
            self._transfer_options = _dig_node_one(self._distribution_info_root, [URL_GEO_NETWORK["TRANSFER_OPT"]])

            # If a dictionary, move to a list
            if isinstance(self._transfer_options, dict):
                self._transfer_options = [self._transfer_options]

            self._title_og = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["CITATION"],
                                                                           URL_GEO_NETWORK["CI_CITATION"],
                                                                           URL_GEO_NETWORK["TITLE"],
                                                                           URL_GEO_NETWORK["CHAR_STRING"]])

            self._title_alt = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["CITATION"],
                                                                            URL_GEO_NETWORK["CI_CITATION"],
                                                                            URL_GEO_NETWORK["TITLE"],
                                                                            URL_GEO_NETWORK["FREE_TEXT"],
                                                                            URL_GEO_NETWORK["TEXT_GROUP"],
                                                                            URL_GEO_NETWORK["LOCALIZED"]])

            # Grab the topic
            self._topic = "topic"
            if URL_GEO_NETWORK["TOPIC_CATEGORY"] in self._data_identif_root:
                self._topic = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["TOPIC_CATEGORY"],
                                                                            URL_GEO_NETWORK["TOPIC_CATEGORY_CODE"]])

            # Grab the date
            self._date = ""
            if URL_GEO_NETWORK["DATE_STAMP"] in self._meta_root and \
               URL_GEO_NETWORK["DATE_TIME"] in self._meta_root[URL_GEO_NETWORK["DATE_STAMP"]]:
                self._date = _dig_node_one_value(self._meta_root, [URL_GEO_NETWORK["DATE_STAMP"], URL_GEO_NETWORK["DATE_TIME"]])

            elif URL_GEO_NETWORK["DATE_STAMP"] in self._meta_root and \
                 URL_GEO_NETWORK["DATE"] in self._meta_root[URL_GEO_NETWORK["DATE_STAMP"]]:
                self._date = _dig_node_one_value(self._meta_root, [URL_GEO_NETWORK["DATE_STAMP"], URL_GEO_NETWORK["DATE"]])

            # Grab the thumbnail url
            self._thumbnail_url = _dig_node_one_value(self._data_identif_root, [URL_GEO_NETWORK["GRAPHIC_OVERVIEW"],
                                                                                URL_GEO_NETWORK["BROWSE_GRAPHIC"],
                                                                                URL_GEO_NETWORK["FILE_NAME"],
                                                                                URL_GEO_NETWORK["CHAR_STRING"]])

            # Check keywords
            keywords_group = _dig_node_all(self._data_identif_root, [URL_GEO_NETWORK["DESC_KEYWORDS"],
                                                                     URL_GEO_NETWORK["KEYWORDS"]])

            self._keywords_nice_group = {}
            key_group = _dig_node_one_value(keywords_group, [URL_GEO_NETWORK["TYPE"],
                                                             URL_GEO_NETWORK["TYPE_CODE"]])

            if key_group not in self._keywords_nice_group:
                self._keywords_nice_group[key_group] = {
                    "og": [],
                    "alt": []
                }

            self._keywords_nice_group[key_group]["og"].extend(_dig_node_all_values(keywords_group, [URL_GEO_NETWORK["KEYWORD"],
                                                                                                    URL_GEO_NETWORK["CHAR_STRING"]]))

            self._keywords_nice_group[key_group]["alt"].extend(_dig_node_all_values(keywords_group, [URL_GEO_NETWORK["KEYWORD"],
                                                                                                     URL_GEO_NETWORK["FREE_TEXT"],
                                                                                                     URL_GEO_NETWORK["TEXT_GROUP"],
                                                                                                     URL_GEO_NETWORK["LOCALIZED"],
                                                                                                     URL_GEO_NETWORK["TEXT"]]))

            ## Further split each node on the commas and rebuild the lists in the dictionary (in case keywords are split by commas)
            self._keywords_splits = {
                "og": [],
                "alt": []
            }
            for key in self._keywords_nice_group:
                for rec in self._keywords_nice_group[key]["og"]:
                    self._keywords_splits["og"].extend([x.strip() for x in rec.split(',')])
                for rec in self._keywords_nice_group[key]["alt"]:
                    self._keywords_splits["alt"].extend([x.strip() for x in rec.split(',')])


    def is_english(self):
        return "eng" in self._language


    def uuid(self):
        return self._uuid


    def srid(self):
        if ":" in self._srid:
            return self._srid.split(":")[1]
        return self._srid


    def extent(self):
        return self._extent


    def temporal_extent(self):
        return self._temporal_extent


    def title_full(self):
        if self.is_english():
            return {"en": self._title_og, "fr": self._title_alt}
        else:
            return {"en": self._title_alt, "fr": self._title_og}


    def topic(self):
        return self._topic


    def keywords_full(self):
        if self.is_english():
            return {"en": self._keywords_splits["og"], "fr": self._keywords_splits["alt"]}
        else:
            return {"en": self._keywords_splits["alt"], "fr": self._keywords_splits["og"]}


    def to_dict(self):
        return {
            "uuid": self.uuid(),
            "topic": self.topic(),
            "srid": self.srid(),
            "title_en": self.title_full()["en"],
            "title_fr": self.title_full()["fr"],
            "keywords_en": self.keywords_full()["en"],
            "keywords_fr": self.keywords_full()["fr"],
            "extent": self.extent(),
            "temporal_extent": self.temporal_extent(),
            "cogs": self.get_cogs()
        }


    def get_cogs(self):

        cogs_infos = []

        # Loop on the transfer options nodes
        for trans_opt_node in self._transfer_options:
            # Get transfer digital if any
            transf_node = trans_opt_node[URL_GEO_NETWORK["TRANSFER_DIGITAL"]]

            # If existing
            if transf_node:
                # If online tag in it
                if URL_GEO_NETWORK["TRANSFER_ONLINE"] in transf_node:
                    # Get the online nodes
                    online_nodes = transf_node[URL_GEO_NETWORK["TRANSFER_ONLINE"]]

                    # For each online node
                    for online in online_nodes:
                        # Read the url
                        url = _dig_node_one_value(online, [URL_GEO_NETWORK["TRANSFER_ONLINE_RES"],
                                                           URL_GEO_NETWORK["TRANSFER_ONLINE_RES_LINK"],
                                                           URL_GEO_NETWORK["TRANSFER_ONLINE_RES_LINK_URL"]])

                        # If the url points to the datacube cog
                        if url and url.endswith(".tif"):
                            name_og =  _dig_node_one_value(online, [URL_GEO_NETWORK["TRANSFER_ONLINE_RES"],
                                                                    URL_GEO_NETWORK["TRANSFER_ONLINE_NAME"],
                                                                    URL_GEO_NETWORK["CHAR_STRING"]])

                            name_alt = _dig_node_one_value(online, [URL_GEO_NETWORK["TRANSFER_ONLINE_RES"],
                                                                    URL_GEO_NETWORK["TRANSFER_ONLINE_NAME"],
                                                                    URL_GEO_NETWORK["FREE_TEXT"],
                                                                    URL_GEO_NETWORK["TEXT_GROUP"],
                                                                    URL_GEO_NETWORK["LOCALIZED"]])

                            # Read the url path
                            cogs_infos.append({
                                "url": url,
                                "url_path": urlparse(url).path,
                                "name": {
                                    "en": name_og if self.is_english() else name_alt,
                                    "fr": name_alt if self.is_english() else name_og
                                }
                            })

        return cogs_infos


def _dig_node_one(starting_node, list_keys):
    value = None
    founds = _dig_node_all(starting_node, list_keys)
    if len(founds) > 0:
        value = founds.pop(0)
    return value


def _dig_node_one_value(starting_node, list_keys):
    # Redirect
    value = _dig_node_one(starting_node, list_keys)

    # Make sure we read the "value" (not a dictionary due to namespaces being read at the node level)
    if isinstance(value, dict):
        if URL_GEO_NETWORK["TEXT"] in value:
            value = value[URL_GEO_NETWORK["TEXT"]]

        elif URL_GEO_NETWORK["CODE_LIST_VALUE"] in value:
            value = value[URL_GEO_NETWORK["CODE_LIST_VALUE"]]
    return value


def _dig_node_all(starting_node, list_keys):
    founds = []
    _dig_node_REC(starting_node, list_keys, founds)
    return founds


def _dig_node_all_values(starting_node, list_keys):
    # Redirect
    founds = _dig_node_all(starting_node, list_keys)

    # Make sure we read the "value" (not a dictionary due to namespaces being read at the node level)
    found_values = []
    for f in founds:
        value = f
        if isinstance(f, dict) and URL_GEO_NETWORK["TEXT"] in f:
            value = f[URL_GEO_NETWORK["TEXT"]]
        found_values.append(value)
    return found_values


def _dig_node_REC(current_node, list_keys, founds):
    # If done
    if len(list_keys) == 0:
        founds.append(current_node)
        return

    # If the node is a dictionary
    if isinstance(current_node, dict):
        key = list_keys[0]
        list_keys_local = list_keys.copy()
        list_keys_local.pop(0)
        if key in current_node:
            _dig_node_REC(current_node[key], list_keys_local, founds)

    elif isinstance(current_node, list):
        for itm in current_node:
            _dig_node_REC(itm, list_keys, founds)
