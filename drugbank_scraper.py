import csv
from datetime import datetime
import zipfile
import logging
import MySQLdb
import settings
import urllib.request as urllib_request


class DrugBankScraper:
    """
    Scrape the DrugBank site for compound names and synonyms.
    """

    # UniChem source ID (primary key in chemscraper.chem_source)
    SOURCE_ID = 2

    def scrape(self):
        """
        Scrape the DrugBank website for compound names and synonyms, populating the chem_record
        and chem_name tables.
        """
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

            logging.info('Downloading CSV file')
            local_csv_file = self.download_drugbank_synonym_file()
            if local_csv_file:
                query_record_data = []
                query_name_data = []
                with open(local_csv_file, encoding='utf-8') as csv_file:
                    csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',',
                        quoting=csv.QUOTE_ALL, skipinitialspace=True)
                    line_count = 0
                    for row in csv_reader:
                        if line_count == 0:
                            line_count += 1
                        else:
                            current_record_pk_id += 1
                            db_id = row[0]
                            query_record_data.append((current_record_pk_id, self.SOURCE_ID, db_id))
                            # Main name
                            query_name_data.append((row[2], current_record_pk_id))
                            # Synonyms
                            synonyms = row[5].split('|')
                            for s in synonyms:
                                if s != row[2]:
                                    query_name_data.append((s.strip(), current_record_pk_id))
                            line_count += 1
                cursor = conn.cursor()
                query = 'INSERT INTO chem_record(id, source_key, source_id) VALUES(%s, %s, %s)'
                cursor.executemany(query, query_record_data)
                conn.commit()

                query = 'INSERT INTO chem_name(compound_name, record_key) VALUES(%s, %s)'
                cursor.executemany(query, query_name_data)
                conn.commit()
            else:
                logging.error('Unable to download and unzip CSV file')
        except Exception:
            logging.error('Error scraping file')
            if line_count:
                logging.error('CSV file line: %s' % line_count)
            logging.exception('')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    def download_drugbank_synonym_file(self):
        """
        Download the DrugBank synonym file (location defined in settings.py).
        """
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        logging.info('Downloading synonym file at %s' % dt_string)
        try:
            download_location = settings.drugbank_synonym_file_url
            local_zip_file = settings.temporary_file_location + "drugbank_synonyms.zip"
            urllib_request.urlretrieve(download_location, local_zip_file)
            logging.info('File downloaded, unzipping')
            with zipfile.ZipFile(local_zip_file, 'r') as zip_ref:
                zip_ref.extractall(settings.temporary_file_location)
            return settings.temporary_file_location + 'drugbank_vocabulary.csv'
        except Exception:
            logging.exception('')
            return None
