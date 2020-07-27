from datetime import datetime
import logging
import MySQLdb
import MySQLdb.cursors
import settings


class ChEMBLScraper:
    """
    Scrape the ChEMBL database for compound names, , populating the chem_record and
    chem_name tables.
    Requires downloading and installing the ChEMBL database from:
        https://chembl.gitbook.io/chembl-interface-documentation/downloads
    Then set the chembl_db_* settings in settings.py.
    """

    # UniChem source ID (primary key in chemscraper.chem_source)
    SOURCE_ID = 1

    def scrape(self):
        """
        Query the ChEMBL database for compound names.
        """
        logging.info('Scraping ChEMBL')
        logging.info('Please ensure the ChEMBL data dump has been downloaded and imported into the database')
        logging.info('with the table name defined in settings.py')
        conn_chemscraper = None
        conn_chembl = None
        cursor_chemscraper = None
        cursor_chembl = None
        try:
            conn_chemscraper = MySQLdb.connect(host=settings.db_host,
                                               db=settings.db_name,
                                               user=settings.db_user,
                                               passwd=settings.db_password,
                                               charset="utf8")
            cursor_chemscraper = conn_chemscraper.cursor()
            current_record_pk_id = 0
            query = "SELECT MAX(id) FROM chem_record"
            cursor_chemscraper.execute(query)
            result = cursor_chemscraper.fetchone()
            if result[0]:
                current_record_pk_id = result[0]

            conn_chembl = MySQLdb.connect(host=settings.chembl_db_host,
                                          db=settings.chembl_db_name,
                                          user=settings.chembl_db_user,
                                          passwd=settings.chembl_db_password,
                                          charset="utf8",
                                          cursorclass=MySQLdb.cursors.SSCursor)
            cursor_chembl = conn_chembl.cursor()

            query = """
                SELECT DISTINCT chem_mol.chembl_id, chem_rec.compound_name
                FROM molecule_dictionary AS chem_mol JOIN compound_records as chem_rec
                ON chem_rec.molregno = chem_mol.molregno
            """
            cursor_chembl.execute(query)
            rows = cursor_chembl.fetchmany(size=10000)
            while rows:
                query_record_data = []
                query_name_data = []
                for r in rows:
                    current_record_pk_id = current_record_pk_id + 1
                    query_record_data.append((current_record_pk_id, self.SOURCE_ID, r[0]))
                    query_name_data.append((r[1], current_record_pk_id))
                query = 'INSERT INTO chem_record(id, source_key, source_id) VALUES(%s, %s, %s)'
                cursor_chemscraper.executemany(query, query_record_data)
                conn_chemscraper.commit()
                query = 'INSERT INTO chem_name(compound_name, record_key) VALUES(%s, %s)'
                cursor_chemscraper.executemany(query, query_name_data)
                conn_chemscraper.commit()

                rows = cursor_chembl.fetchmany(size=10000)

        except Exception:
            logging.error('Error scraping ChEMBL')
            logging.exception('')
        finally:
            if cursor_chemscraper:
                cursor_chemscraper.close()
            if cursor_chembl:
                cursor_chembl.close()
            if conn_chemscraper:
                conn_chemscraper.close()
            if conn_chembl:
                conn_chembl.close()


    def create_synonym_list(self):
        """
        Convenience method to create text file directly from ChEMBL DB until chemscraper DB
        can be populated.
        """
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        logging.info('Scraping ChEMBL: %s' % dt_string)
        conn_chembl = None
        cursor_chembl = None
        f = None
        try:
            file_path = settings.temporary_file_location + "source_synonyms_src_1.txt"
            f = open(file_path, "w", encoding="utf-8")

            conn_chembl = MySQLdb.connect(host=settings.chembl_db_host,
                                          db=settings.chembl_db_name,
                                          user=settings.chembl_db_user,
                                          passwd=settings.chembl_db_password,
                                          charset="utf8",
                                          cursorclass=MySQLdb.cursors.SSCursor)
            cursor_chembl = conn_chembl.cursor()
            query = """
                SELECT DISTINCT chem_mol.chembl_id, chem_rec.compound_name
                FROM molecule_dictionary AS chem_mol JOIN compound_records as chem_rec
                ON chem_rec.molregno = chem_mol.molregno
            """
            cursor_chembl.execute(query)
            rows = cursor_chembl.fetchmany(size=10000)
            while rows:
                for r in rows:
                    f.write("%s\t%s\n" % (r[0], r[1]))
                rows = cursor_chembl.fetchmany(size=10000)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info('Scraping complete: %s' % dt_string)
        except Exception:
            logging.error('Error scraping ChEMBL')
            logging.exception('')
        finally:
            if f:
                f.close()
            if cursor_chembl:
                cursor_chembl.close()
            if conn_chembl:
                conn_chembl.close()
