# =================================================================
#
# Authors: Alexandre Roy <alexandre.roy@nrcan-rncan.gc.ca>
#
# Copyright (c) 2023 Alexandre Roy
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

import os, logging, json, zipfile, requests, uuid, emails, shutil
import shapely, pyproj
from mimetypes import guess_extension
from xml.etree import cElementTree as ET
from pygeoapi.process.extract import (
    ExtractProcessor,
    CollectionsUndefinedException,
    CollectionsNotFoundException,
    ClippingAreaUndefinedException,
    ClippingAreaCrsUndefinedException,
    OutputCRSNotANumberException
)
from pygeoapi import api_aws
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from pygeoapi.provider.base import (
    ProviderPreconditionFailed,
    ProviderRequestEntityTooLargeError
)
from pygeoapi.util import get_provider_by_type, to_json, get_area_from_wkt_in_km2
from pygeoapi.plugin import load_plugin


LOGGER = logging.getLogger(__name__)


# Configurations - placed here inside the NRCan plugin
EXTRACT_FOLDER = "extractions"
TABLE_NAME = "czs_collection"
FIELD_COLLECTION_NAME = "collection_name"
FIELD_METADATA_XML = "metadata_cat_xml"


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'extract-nrcan',
    'title': {
        'en': 'Extract the data',
        'fr': 'Extrait les données'
    },
    'description': {
        'en': 'This process takes a list of collections and a geometry wkt as input, extracts the records, saves them as geojson in a zip file, stores the zip file to an S3 Bucket, and returns the URL to download the file.',
        'fr': 'Ce processus prend une liste de collections, une géométrie en format wkt, extrait les enregistrements qui intersectent la géométrie, sauvegarde les informations en geojson, sauvegarde le tout dans un zip file dans un Bucket S3, et retourne le chemin URL pour télécharger le fichier.',
    },
    'jobControlOptions': ['async-execute'],
    'keywords': ['extract', 'clip zip ship'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'collections': {
            'title': 'An array of collection names to extract records from',
            'description': 'An array of collection names to extract records from',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 99,
            'metadata': None,  # TODO how to use?
            'keywords': ['collections', 'records']
        },
        'geom': {
            'title': 'The geometry as WKT format',
            'description': 'The geometry as WKT format',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['geometry']
        },
        'geom_crs': {
            'title': 'The crs of the input geometry',
            'description': 'The crs of the input geometry',
            'schema': {
                'type': 'integer'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['crs']
        }
    },
    'outputs': {
        'echo': {
            'title': 'The url to the zip file containing the information',
            'description': 'The url to the zip file containing the information',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            "collections": [
                "cdem_mpi__major_projects_inventory_point",
                "cdem_mpi__major_projects_inventory_line",
                "cdem_mpi__cdem"
            ],
            "geom": "POLYGON((-72.3061 45.3656, -72.3061 45.9375, -71.7477 45.9375, -71.7477 45.3656, -72.3061 45.3656))",
            "geom_crs": 4326
        }
    }
}


class ExtractNRCanProcessor(ExtractProcessor):
    """Extract Processor"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.extract.ExtractNRCanProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.extract_url = ""  # Will store the extract url result
        self.email = None  # The email for the process
        self.errors = []  # The errors during the process


    def on_query_validate_inputs(self, data: dict):
        """
        Override this method to perform validations of inputs
        """

        if "email" in data and data['email']:
            # Store the email
            self.email = data['email']

        else:
            # Error
            err = EmailUndefinedException()
            self.errors.append(err)
            LOGGER.warning(err)

        if "collections" in data and data['collections']:
            # Store the collections
            self.colls = data['collections']

            # Check if each collection exists
            for c in self.colls:
                if not c in self.processor_def['collections']:
                    # Error
                    err = CollectionsNotFoundException(c)
                    self.errors.append(err)
                    LOGGER.warning(err)

        else:
            # Error
            err = CollectionsUndefinedException()
            self.errors.append(err)
            LOGGER.warning(err)

        if "geom" in data and data['geom']:
            # Store the input geometry
            self.geom_wkt = data['geom']

        else:
            # Error
            err = ClippingAreaUndefinedException()
            self.errors.append(err)
            LOGGER.warning(err)

        if "geom_crs" in data and data["geom_crs"]:
            # Store the crs
            self.geom_crs = data["geom_crs"]

        else:
            # Error
            err = ClippingAreaCrsUndefinedException()
            self.errors.append(err)
            LOGGER.warning(err)

        if "out_crs" in data and data["out_crs"]:
            # If a number
            if data["out_crs"].isdigit():
                # Store the crs
                self.out_crs = int(data["out_crs"])

            else:
                err = OutputCRSNotANumberException()
                self.errors.append(err)
                LOGGER.warning(err)

        else:
            # Optional parameter, all good
            pass

        # Update the job with the received parameters
        # Obfuscate the email
        email = self.email
        if email and '@' in email:
            email = '@' + email.split('@')[1]
        self.process_manager.update_job(self.job_id, {'collections': self.colls, 'email': email, 'geom': self.geom_wkt, 'geom_crs': self.geom_crs})

        # If no errors
        if not self.errors:
            # All good
            return True
        return False


    def on_query_validate_execution(self, data: dict):
        """
        Override this method to perform validations pre-execution
        """
        # Get the area of the input geometry
        area = get_area_from_wkt_in_km2(self.geom_wkt, self.geom_crs)

        # For each collection to extract
        collection_over_limit = False
        for coll_name in self.colls:
            # Read collection config
            c = self.processor_def['collections'][coll_name]

            # Read the max area
            max_extract_area = c['providers'][0]['max_extract_area']

            # If the area is over the maximum for the collection
            if area > max_extract_area:
                err = ClippingAreaTooLargeException(coll_name, max_extract_area, area)
                self.errors.append(err)
                LOGGER.warning(err)
                collection_over_limit = True

        # If no errors
        if not self.errors:
            # All good
            return True
        return False


    def on_query_finalize(self, data: dict, query_res: dict):
        """
        Overrides the finalization process.
        Now that the query on each collection has happened and we have the results, we create
        a zip file and place the zip in a S3 Bucket.
        """

        # The files that we want to zip
        files = []

        # Unique key for drive location
        unique_key = uuid.uuid4()

        # For each collection result
        for c in query_res:
            # Depending on the type
            if self.get_collection_type(c) == "coverage":
                # Save coverage image and keep track
                files.append(self._save_file_image(unique_key, c, self.get_collection_coverage_mimetype(c), query_res[c]))

            else:
                # Save to a JSON file and keep track
                files.append(self._save_file_geojson(unique_key, c, query_res[c]))

            # Get the metadata xml for the collection
            metadata_xml = self.get_metadata_xml_from_coll_conf(self.processor_def['settings']['catalogue_url'],
                                                                self.processor_def['collections'][c])

            # If found
            if metadata_xml:
                files.append(self._save_file_xml(unique_key, c, metadata_xml))

        # Destination zip file path and name
        dest_zip = f'{unique_key}.zip'

        # Save all files to a zip file
        zip_file = self._zip_file(unique_key, files, dest_zip)

        # Put the zip file in S3
        api_aws.connect_s3_send_file(f"./{EXTRACT_FOLDER}/{unique_key}/{dest_zip}",
                                     self.processor_def['settings']['s3']['iam_role'],
                                     self.processor_def['settings']['s3']['bucket_name'],
                                     self.processor_def['settings']['s3']['bucket_prefix'],
                                     os.path.basename(dest_zip))

        # Store the extract url
        self.extract_url = f"{self.processor_def['settings']['extract_url']}{os.path.basename(dest_zip)}"

        # Send email
        self.send_emails(self.processor_def['settings']['email'], self.job_id, self.email, self.colls, self.geom_wkt, self.geom_crs, self.extract_url, [], self.errors, None)

        # Now that it's copied on S3, delete local
        shutil.rmtree(f"./{EXTRACT_FOLDER}/{unique_key}", ignore_errors=True)


    def on_exception(self, exception: Exception):
        """
        Overrides the behavior when an exception happened in the process
        """

        # Try sending an email
        try:
            self.send_emails(self.processor_def['settings']['email'], self.job_id, self.email, self.colls, self.geom_wkt, self.geom_crs, self.extract_url, [], self.errors, exception)

        except:
            # Continue
            raise
            #pass


    def on_query_results(self, query_res: dict):
        """
        Overrides the results to return a json of the extract url instead of the actual data from the extraction.
        """

        return 'application/json', {'extract_url': self.extract_url}


    @staticmethod
    def get_metadata_xml_from_coll_conf(catalog_url: str, coll_conf: dict):
        """
        Fetches the metadata from the GeoNetwork catalogue
        """

        try:
            # Find the metadata uuid from the link
            metadata_uuid = ExtractNRCanProcessor.get_metadata_from_links(coll_conf['links'])

            # If read from config
            if metadata_uuid:
                # Query the catalog using the metadata uuid
                response = requests.get(catalog_url.format(metadata_uuid=metadata_uuid))
                metadata = response.text.strip()

                # If no metadata actually found
                if "gmd:MD_Metadata" in metadata:
                    # Save the metadata for the collection and keep track
                    return metadata

        except Exception as err:
            print("Couldn't read metadata from catalog: " + str(err))


    @staticmethod
    def get_metadata_from_links(links: list):
        """
        Queries the Collection metadata information based on the given name.

        :returns: The information on a collection.
        """

        for l in links:
            if l['type'] == "text/html" and l['rel'] == "canonical":
                return os.path.basename(l['href'])
        return None


    @staticmethod
    def _save_file_geojson(extract_key: str, coll_name: str, query_res: dict):
        """
        Saves the given query_res in a geojson file in the EXTRACT_FOLDER
        """

        file_name = f"{coll_name}.geojson"
        if not os.path.exists(f'./{EXTRACT_FOLDER}/{extract_key}'):
            os.umask(0)
            os.makedirs(f'./{EXTRACT_FOLDER}/{extract_key}', mode=0o777)
        with open(f'./{EXTRACT_FOLDER}/{extract_key}/{file_name}', 'w', encoding='utf-8') as f:
            json.dump(query_res, f, indent=4)
        return file_name


    @staticmethod
    def _save_file_image(extract_key: str, coll_name: str, mimetype: str, query_res):
        """
        Saves the given query_res in a geojson file in the EXTRACT_FOLDER
        """
        file_name = f"{coll_name}{guess_extension(mimetype)}"
        if not os.path.exists(f'./{EXTRACT_FOLDER}/{extract_key}'):
            os.umask(0)
            os.makedirs(f'./{EXTRACT_FOLDER}/{extract_key}', mode=0o777)
        with open(f'./{EXTRACT_FOLDER}/{extract_key}/{file_name}', 'wb') as f:
            f.write(query_res)
        return file_name


    @staticmethod
    def _save_file_xml(extract_key: str, coll_name: str, query_res: dict):
        """
        Saves the given xml query_res in a xml file in the EXTRACT_FOLDER
        """

        file_name = f"{coll_name}.xml"
        if not os.path.exists(f'./{EXTRACT_FOLDER}/{extract_key}'):
            os.umask(0)
            os.makedirs(f'./{EXTRACT_FOLDER}/{extract_key}', mode=0o777)
        with open(f'./{EXTRACT_FOLDER}/{extract_key}/{file_name}', 'w', encoding='utf-8') as f:
            f.write(query_res)
        return file_name


    @staticmethod
    def _zip_file(extract_key: str, file_names: list, zip_file_name: str):
        """
        Saves the given file names in a zip file in the EXTRACT_FOLDER and
        deletes the original files in the process.
        """

        if not os.path.exists(f'./{EXTRACT_FOLDER}/{extract_key}'):
            os.umask(0)
            os.makedirs(f'./{EXTRACT_FOLDER}/{extract_key}', mode=0o777)
        with zipfile.ZipFile(f"./{EXTRACT_FOLDER}/{extract_key}/{zip_file_name}", 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in file_names:
                # Write the file in the zip
                zipf.write(f'./{EXTRACT_FOLDER}/{extract_key}/{f}', f'./{f}')
                # Delete the file now that it's in the zip
                os.remove(f'./{EXTRACT_FOLDER}/{extract_key}/{f}')
            zipf.close()
            return zipf


    @staticmethod
    def send_emails(email_config: dict, job_id: str, email: str, colls: list, geom_wkt: str, geom_crs: str, download_link: str, warnings: list, errors: list, big_error: Exception):
        """
        Sends an email
        """

        # If there's an email
        if email:
            # Prepare the email
            message = emails.html(
                html=ExtractNRCanProcessor._send_emails_body_user(job_id, email, colls, geom_wkt, geom_crs, download_link, email_config['from'], warnings, errors, big_error),
                subject="Résultat de votre requête d'extraction / Result of your extraction request",
                mail_from=email_config['from']
            )

            # If there was no error
            if not errors and not big_error:
                # Add admin in CC
                message.cc = email_config['admin_user_cc']

            # Send the email
            r = message.send(
                to=email,
                smtp={
                    "host": email_config['host'],
                    "port": email_config['port'],
                    "timeout": email_config['timeout'],
                    "user": email_config['username'],
                    "password": email_config['password'],
                    "tls": True
                }
            )

        # If there was some error
        if errors or big_error:
            # Prepare the email
            message = emails.html(
                html=ExtractNRCanProcessor._send_emails_body_admin(job_id, email, colls, geom_wkt, geom_crs, [], warnings, errors, big_error),
                subject="Résultat d'une requête d'extraction / Result of an extraction request",
                mail_from=email_config['from']
            )

            # Send the email
            r = message.send(
                to=email_config['admin_main'],
                smtp={
                    "host": email_config['host'],
                    "port": email_config['port'],
                    "timeout": email_config['timeout'],
                    "user": email_config['username'],
                    "password": email_config['password'],
                    "tls": True
                }
            )


    @staticmethod
    def _send_emails_body_user(job_id: str, email: str, colls: list, geom_wkt: str, geom_crs: str, download_link: str, email_from: str, warnings: list, errors: list, big_error: Exception):

        # Read the warnings
        [english_warnings, french_warnings] = [None, None]

        # If the process was a success
        html_title_color = "#1877f2"
        html_title = "Succès de l'opération d'extraction / Success of the extraction process"

        html_content = f"<i>(English message follows)</i><br/>"

        #
        # French version
        #
        html_content = html_content + f"<br/>Bonjour,<br/><br/>"
        if not errors and not big_error:
            html_content = html_content + f"Votre requête d'extraction a été exécutée avec succès.<br/>Voici le lien de téléchargement: <a href=\"{download_link}\">{download_link}</a><br/><br/>"

        # If there's been a major error the user shouldn't necessary know the details
        user_msg = None
        if errors:
            html_title_color = "#e80000"
            html_title = "Échec de l'opération d'extraction / Failure of the extraction process"
            user_msg = ExtractNRCanProcessor._combine_exceptions_for_response(errors, prefix="<li>", suffix="</li>")
            html_content = html_content + f"L'opération a échouée pour la (les) raison(s) suivantes:<ul>{user_msg.message_fr}</ul><br/>"

        # If there's been an issue the user should be informed
        elif big_error:
            html_title_color = "#e80000"
            html_title = "Échec de l'opération d'extraction / Failure of the extraction process"
            html_content = html_content + f"Un incident majeur est survenu. Un administrateur a été immédiatement informé. <a href=\"mailto:{email_from}\">SVP contactez nous</a>.<br/><br/>"

        # If there was any warning
        if warnings:
            html_content = html_content + f"Les avertissements suivants sont survenus:<ul>{french_warnings}</ul><br/>"

        # Information on the job
        colls_string = None
        if colls:
            for c in colls:
                if not colls_string:
                    colls_string = ""
                colls_string = colls_string + f"<li>{c}</li>"
        parameters = f"<li>JobID: {job_id}</li>"
        #parameters = parameters + f"<li>Email: {email}</li>"
        parameters = parameters + f"<li>Collections:<ul>{colls_string}</ul></li>"
        parameters = parameters + f"<li>GeomWKT: {geom_wkt}</li>"
        parameters = parameters + f"<li>GeomCRS: {geom_crs}</li>"
        html_content = html_content + f"Information sur le traitement:<ul>{parameters}</ul><br/>"

        # French closing
        html_content = html_content + f"<div>Merci,<br/>L'équipe d'extraction Clip Zip Ship<br/><i>Avez-vous besoin d'aide pour votre service cartographique? <a href=\"mailto:{email_from}\">Contactez notre équipe</a>.</i></div>"
        html_content = html_content + "<br/><br/>-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------<br/><br/>"

        #
        # English version
        #
        html_content = html_content + f"Hi,<br/><br/>"
        if not errors and not big_error:
            html_content = html_content + f"Your extraction request proceeded successfully.<br/>Here's the download link: <a href=\"{download_link}\">{download_link}</a><br/><br/>"

        if errors:
            html_content = html_content + f"The operation failed due to the following reason(s):<ul>{user_msg.message}</ul><br/>"

        elif big_error:
            html_content = html_content + f"A major error happened. An admin has been immediately notified. <a href=\"mailto:{email_from}\">Please contact us</a>.<br/><br/>"

        # If there was any warning
        if warnings:
            html_content = html_content + f"Les avertissements suivants sont survenus:<ul>{english_warnings}</ul><br/>"

        # Information on the job
        colls_string = None
        if colls:
            for c in colls:
                if not colls_string:
                    colls_string = ""
                colls_string = colls_string + f"<li>{c}</li>"
        parameters = f"<li>JobID: {job_id}</li>"
        #parameters = parameters + f"<li>Email: {email}</li>"
        parameters = parameters + f"<li>Collections:<ul>{colls_string}</ul></li>"
        parameters = parameters + f"<li>GeomWKT: {geom_wkt}</li>"
        parameters = parameters + f"<li>GeomCRS: {geom_crs}</li>"
        html_content = html_content + f"Information on the extraction:<ul>{parameters}</ul><br/>"

        # English closing
        html_content = html_content + f"<div>Thanks,<br/>Clip Zip Ship Extractor Team<br/><i>Need help with your extraction? <a href=\"mailto:{email_from}\">Contact our team</a>.</i></div>"

        # Global Footer
        html_footer_sent_to = f"<br/><br/>This message was automatically sent to <a href='mailto:{email}' style='color:#1b74e4;text-decoration:none' target='_blank'>{email}</a>"

        # Redirect
        return ExtractNRCanProcessor._send_emails_body_build(html_title, html_title_color, html_content, html_footer_sent_to)


    @staticmethod
    def _send_emails_body_admin(job_id: str, email: str, colls: list, geom_wkt: str, geom_crs: str, progress_marks: list, warnings: list, errors: list, big_error: Exception):

        # Read the progress marks
        #[english_log, french_log] = ExtractNRCanProcessor._combine_progress_marks_for_response(progress_marks, prefix="<li>", suffix="</li>")
        [english_log, french_log] = [None, None]

        # Read the warnings
        #[english_warnings, french_warnings] = ExtractNRCanProcessor._combine_warnings_for_response(warnings, prefix="<li>", suffix="</li>")
        [english_warnings, french_warnings] = [None, None]

        # Titles
        html_title_color = "#e80000"
        html_title = "Échec de l'opération d'extraction / Failure of the extraction process"
        html_content = f"<i>(English message follows)</i><br/>"

        #
        # French version
        #
        html_content = html_content + f"<br/>Bonjour à l'administrateur,<br/><br/>"

        # If there's been a major error the user shouldn't necessary know the details
        user_msg = None
        if big_error:
            html_content = html_content + f"Une erreur majeure est survenue. Voici le message seulement visible par un administrateur:<ul><li>{str(big_error)}</li></ul><br/>"

        # If there's been an issue the user should be informed
        if errors:
            user_msg = ExtractNRCanProcessor._combine_exceptions_for_response(errors, admin=True, prefix="<li>", suffix="</li>")
            html_content = html_content + f"Les erreurs suivantes sont survenues:<ul>{user_msg.message_fr}</ul><br/>"

        # If there was any warning
        if warnings:
            html_content = html_content + f"Les avertissements suivants sont survenus:<ul>{french_warnings}</ul><br/>"

        # Information on the job
        colls_string = None
        if colls:
            for c in colls:
                if not colls_string:
                    colls_string = ""
                colls_string = colls_string + f"<li>{c}</li>"
        parameters = f"<li>JobID: {job_id}</li>"
        #parameters = parameters + f"<li>Email: {email}</li>"
        parameters = parameters + f"<li>Collections:<ul>{colls_string}</ul></li>"
        parameters = parameters + f"<li>GeomWKT: {geom_wkt}</li>"
        parameters = parameters + f"<li>GeomCRS: {geom_crs}</li>"
        html_content = html_content + f"Information sur le traitement:<ul>{parameters}</ul><br/>"

        # If log
        if french_log:
            html_content = html_content + f"<br/>Voici le log:<ul>{french_log}</ul><br/>"
        html_content = html_content + "<br/>-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------<br/><br/>"


        #
        # English version
        #
        html_content = html_content + f"Hi administrator,<br/><br/>"

        # If there's been a major error
        if big_error:
            html_content = html_content + f"A major error happened. Here's the message only visible to an admin:<ul><li>{str(big_error)}</li></ul><br/>"

        # If there was any errors
        if errors:
            html_content = html_content + f"The following errors happened:<ul>{user_msg.message}</ul><br/>"

        # If there was any warning
        if warnings:
            html_content = html_content + f"The following warnings happened:<ul>{english_warnings}</ul><br/>"

        # Information on the job
        colls_string = None
        if colls:
            for c in colls:
                if not colls_string:
                    colls_string = ""
                colls_string = colls_string + f"<li>{c}</li>"
        parameters = f"<li>JobID: {job_id}</li>"
        #parameters = parameters + f"<li>Email: {email}</li>"
        parameters = parameters + f"<li>Collections:<ul>{colls_string}</ul></li>"
        parameters = parameters + f"<li>GeomWKT: {geom_wkt}</li>"
        parameters = parameters + f"<li>GeomCRS: {geom_crs}</li>"
        html_content = html_content + f"Information on the extraction:<ul>{parameters}</ul><br/>"

        # If log
        if english_log:
            html_content = html_content + f"<br/>Here's the log:<ul>{english_log}</ul><br/>"

        # Redirect
        return ExtractNRCanProcessor._send_emails_body_build(html_title, html_title_color, html_content, "")


    @staticmethod
    def _send_emails_body_build(html_title: str, title_color: str, html_body_content: str, html_footer_sent_to: str):

        return f"""
                <div style='margin:0;padding:0' dir='ltr' bgcolor='#ffffff'>
                    <table border='0' cellspacing='0' cellpadding='0' align='center' style='border-collapse:collapse'>
                        <tbody>
                            <tr>
                                <td style='font-family:Helvetica Neue,Helvetica,Lucida Grande,tahoma,verdana,arial,sans-serif;background:#ffffff'>
                                    <table border='0' width='100%' cellspacing='0' cellpadding='0' style='border-collapse:collapse'>
                                        <tbody>
                                            <tr>
                                                <td height='20' style='line-height:20px' colspan='3'></td>
                                            </tr>
                                            <tr>
                                                <td height='1' colspan='3' style='line-height:1px'></td>
                                            </tr>
                                            <tr>
                                                <td width='15' style='display:block;width:15px'></td>
                                                <td>
                                                    <table border='0' width='100%' cellspacing='0' cellpadding='0' style='border-collapse:collapse'>
                                                        <tbody>
                                                            <tr>
                                                                <td height='15' style='line-height:15px' colspan='3'></td>
                                                            </tr>
                                                            <tr>
                                                                <td width='32' align='left' valign='middle' style='height:32;line-height:0px'>
                                                                    <div><img src='data:image/png;base64,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' style='max-width: 120px;'></div>
                                                                </td>
                                                                <td width='15' style='display:block;width:15px'></td>
                                                                <td width='100%'><span style='font-family:Helvetica Neue,Helvetica,Lucida Grande,tahoma,verdana,arial,sans-serif;font-size:19px;line-height:32px;color:{title_color}'>{html_title}</span></td>
                                                            </tr>
                                                            <tr style='border-bottom:solid 1px #e5e5e5'>
                                                                <td height='15' style='line-height:15px' colspan='3'></td>
                                                            </tr>
                                                        </tbody>
                                                    </table>
                                                </td>
                                                <td width='15' style='display:block;width:15px'></td>
                                            </tr>
                                            <tr>
                                                <td width='15' style='display:block;width:15px'></td>
                                                <td>
                                                    <table border='0' width='100%' cellspacing='0' cellpadding='0' style='border-collapse:collapse'>
                                                        <tbody>
                                                            <tr>
                                                                <td>
                                                                    <span style='font-family:Helvetica Neue,Helvetica,Lucida Grande,tahoma,verdana,arial,sans-serif;font-size:16px;line-height:21px;color:#141823'>
                                                                        <div>{html_body_content}</div>
                                                                    </span>
                                                                </td>
                                                            </tr>
                                                        </tbody>
                                                    </table>
                                                </td><td width='15' style='display:block;width:15px'></td>
                                            </tr>
                                            <tr>
                                                <td width='15' style='display:block;width:15px'></td>
                                                <td>
                                                    <table border='0' width='100%' cellspacing='0' cellpadding='0' align='left' style='border-collapse:collapse'>
                                                        <tbody>
                                                            <tr>
                                                                <td style='font-family:Helvetica Neue,Helvetica,Lucida Grande,tahoma,verdana,arial,sans-serif;font-size:11px;color:#aaaaaa;line-height:16px'>{html_footer_sent_to}.<br>DDR Extraction - National Resources Canada/Ressources Naturelles Canada</td>
                                                            </tr>
                                                        </tbody>
                                                    </table>
                                                </td>
                                                <td width='15' style='display:block;width:15px'></td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                    <br/><br/>
                </div>
            """


    @staticmethod
    def _combine_exceptions_for_response(exceptions: list, *, admin: bool = False, prefix: str = "", suffix: str = os.linesep):
        """
        Loops on the exceptions and create a single UserMessageException which encompases all messages.
        """

        # For each exception
        msg_english_total = ""
        msg_french_total = ""
        for e in exceptions:
            [msg_english, msg_french] = ExtractNRCanProcessor._combine_exceptions_for_response_read_exception(e, admin)
            msg_english_total = msg_english_total + prefix + msg_english + suffix
            msg_french_total = msg_french_total + prefix + msg_french + suffix
        msg_english_total = msg_english_total.strip(os.linesep)
        msg_french_total = msg_french_total.strip(os.linesep)

        return UserMessageException(500, msg_english_total, msg_french_total)


    @staticmethod
    def _combine_exceptions_for_response_read_exception(e, admin: bool = False):
        """
        """

        if isinstance(e, EmailUndefinedException):
            return [str(e),  # English message works fine
                    f"Le paramètre d'entré 'email' n'est pas défini"]

        elif isinstance(e, CollectionsUndefinedException):
            return [str(e),  # English message works fine
                    f"Le paramètre d'entré 'collections' n'est pas défini"]

        elif isinstance(e, CollectionsNotFoundException):
            return [str(e),  # English message works fine
                    f"La collection {e.coll_name} n'existe pas"]

        elif isinstance(e, ClippingAreaUndefinedException):
            return [str(e),  # English message works fine
                    f"Le paramètre d'entré 'geom' n'est pas défini"]

        elif isinstance(e, ClippingAreaCrsUndefinedException):
            return [str(e),  # English message works fine
                    f"Le paramètre d'entré 'geom_crs' n'est pas défini"]

        elif isinstance(e, ClippingAreaTooLargeException):
            return [str(e),  # English message works fine
                    f"L'aire d'extraction était {e.extract_area} km2 qui est plus grande que le maximum de {e.max_area} km2 pour {e.collection}"]

        elif isinstance(e, OutputCRSNotANumberException):
            return [str(e),  # English message works fine
                    f"Le paramètre d'entré 'out_crs' n'est pas un nombre"]

        # If admin
        if admin:
            return [str(e), str(e)]

        # Default
        return ["A fatal error happened", "Une erreur fatale est survenue"]


    def __repr__(self):
        return f'<ExtractNRCanProcessor> {self.name}'


class EmailUndefinedException(ProviderPreconditionFailed):
    """Exception raised when no email is defined"""
    def __init__(self):
        super().__init__("Input parameter 'email' is undefined")


class ClippingAreaTooLargeException(ProviderRequestEntityTooLargeError):
    """Exception raised when no clipping area is too large"""
    def __init__(self, collection: str, max_area: float, extract_area: float):
        super().__init__(f"Clipping area was {extract_area} km2 which is greater than the maximum area of {max_area} km2 for {collection}")
        self.collection = collection
        self.max_area = max_area
        self.extract_area = extract_area


class UserMessageException(Exception):
    """Exception raised when a message (likely an error message) needs to be sent to the User."""
    def __init__(self, code, message, message_fr):
        super(UserMessageException, self)
        self.code = code
        self.title = "Error"
        if code == 500:
            self.title = "Internal Server Error"
        elif code == 400:
            self.title = "Bad Request"
        elif code == 401:
            self.title = "Unauthorized"
        elif code == 403:
            self.title = "Forbidden"
        elif code == 404:
            self.title = "Not Found"
        elif code == 405:
            self.title = "Method Not Allowed"
        elif code == 429:
            self.title = "Too Many Requests"
        self.message = message
        self.message_fr = message_fr

    def __str__(self):
        return f"ENG: {self.message} | FR: {self.message_fr}"
