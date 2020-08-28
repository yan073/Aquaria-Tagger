from datetime import datetime
import ftplib
from gzip import open as gopen
import logging
import MySQLdb
import os
import settings
import time
import urllib.request as urllib_request
import xml.etree.ElementTree as ET


class PubChemScraper:
    """
    Scrape PubChem website for compound names and synonyms, populating the chem_record
    and chem_name tables.
    """

    # UniChem source ID (primary key in chemscraper.chem_source)
    SOURCE_ID = 22


    def scrape(self):
        """
        Scrape the PubChem site for compound names and synonyms. Files downloaded will be around
        1GB in size so are deleted as processed. This currently creates ~100GB of data in the
        chemscraper DB.
        """
        # Get list of zipped files at FTP source
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        logging.info('Scraping PubChem started at %s' % dt_string)

        conn = None
        cursor = None
        try:
            conn = MySQLdb.connect(host=settings.db_host,
                                   db=settings.db_name,
                                   user=settings.db_user,
                                   passwd=settings.db_password,
                                   charset="utf8")
            cursor = conn.cursor()
            query = "SELECT COUNT(*) FROM chem_source WHERE id = %s"
            cursor = conn.cursor()
            cursor.execute(query, (self.SOURCE_ID,))
            source_exists = cursor.fetchone()
            if not source_exists or source_exists[0] != 1:
                logging.error('The source could not be located in the DB. Have you run "scrape_source_names"?')
                return

            current_record_pk_id = 0
            query = "SELECT MAX(id) FROM chem_record"
            cursor.execute(query)
            result = cursor.fetchone()
            if result[0]:
                current_record_pk_id = result[0]

            logging.info('Querying FTP file list')
            file_list = self.get_list_of_ftp_compound_files(settings.pubchem_ftp_host,
                                                            settings.pubchem_ftp_compound_directory,
                                                            'gz')
            if file_list and len(file_list) > 0:
                for file_name in file_list:
                    ftp_file_path = 'ftp://%s/pubchem/Compound/CURRENT-Full/XML/%s' % (settings.pubchem_ftp_host, file_name)
                    local_file_path = settings.temporary_file_location + file_name
                    logging.info('Downloading %s' % ftp_file_path)
                    attempt_count = 0
                    file_loaded = False
                    while not file_loaded:
                        try:
                            urllib_request.urlretrieve(ftp_file_path, local_file_path)
                            file_loaded = True
                        except Exception as file_e:
                            logging.error('Problem retrieving file: ', file_e)
                            logging.error('Waiting 60 seconds and trying again')
                            attempt_count = attempt_count + 1
                            if attempt_count == 10:
                                break
                            else:
                                time.sleep(60)
                                continue

                    if file_loaded:
                        with gopen(local_file_path) as xml_file:
                            now = datetime.now()
                            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                            logging.info("Starting XML parsing: %s" % dt_string)

                            context = ET.iterparse(xml_file)
                            query_record_data = []
                            query_name_data = []

                            current_record_name_list = []

                            for event, elem in context:
                                if elem.tag.endswith('PC-CompoundType_id_cid'):

                                    if len(current_record_name_list) > 0:
                                        for name in current_record_name_list:
                                            query_name_data.append((name, current_record_pk_id))
                                        current_record_name_list = []

                                    current_record_pk_id = current_record_pk_id + 1
                                    query_record_data.append((current_record_pk_id, self.SOURCE_ID, elem.text))
                                elif elem.tag.endswith('PC-Urn_label'):
                                    current_label = elem.text

                                elif elem.tag.endswith('PC-Urn_name'):
                                    current_name = elem.text

                                elif elem.tag.endswith('PC-InfoData_value_sval'):
                                    # Not harvesting InChI or SMILES values
                                    if current_name != 'CAS-like Style' and current_name != 'Markup' and (current_label == 'IUPAC Name' or current_label == 'InChIKey'):
                                        if elem.text not in current_record_name_list:
                                            current_record_name_list.append(elem.text)

                                elem.clear()

                            # Add last record and names that will be skipped due to iteration
                            if len(current_record_name_list) > 0:
                                for name in current_record_name_list:
                                    query_name_data.append((name, current_record_pk_id))

                        xml_file.close()

                        now = datetime.now()
                        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                        logging.info("Parsing finished, writing results to DB at %s" % dt_string)

                        """
                        logging.info('records')
                        logging.info(str(query_record_data))
                        logging.info('names')
                        logging.info(str(query_name_data))
                        """

                        conn = MySQLdb.connect(host=settings.db_host,
                                               db=settings.db_name,
                                               user=settings.db_user,
                                               passwd=settings.db_password,
                                               charset="utf8")
                        cursor = conn.cursor()
                        query = 'INSERT INTO chem_record(id, source_key, source_id) VALUES(%s, %s, %s)'
                        cursor.executemany(query, query_record_data)
                        conn.commit()
                        query = 'INSERT INTO chem_name(compound_name, record_key) VALUES(%s, %s)'
                        cursor.executemany(query, query_name_data)
                        conn.commit()
                    # Files are around 1GB, delete after parsing
                    os.remove(local_file_path)
                    now = datetime.now()
                    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                    logging.info("Results written %s" % dt_string)
        except Exception:
            logging.error('Error scraping file')
            logging.exception('')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    def get_list_of_ftp_compound_files(self, host, path, extension):
        """
        Get a list of files at an FTP site that have a specified extension.
        :param host: FTP host
        :param path: FTP path at host
        :param extension: results will be filtered by this extension
        :return: a list of files at the FTP site and path with the specified extension
        """
        file_list = []
        try:
            ftp = ftplib.FTP(host, 'anonymous', '')
            ftp.encoding = "utf-8"
            ftp.cwd(path)
            for file_name in ftp.nlst():
                if file_name.lower().endswith(extension.lower()):
                    file_list.append(file_name)
        except Exception:
            logging.error('Error retrieving list of compound files')
            logging.exception('')
        finally:
            return file_list
