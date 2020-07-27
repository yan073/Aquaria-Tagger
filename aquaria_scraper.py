from datetime import datetime
import json
import logging
import MySQLdb
import settings


class AquariaScraper:
    """
    Parse Aquaria database for small molecule counts.
    Make sure the aquaria_db_* settings have been set in settings.py.
    """

    def count_small_molecules(self):
        """
        Will parse the SMALL_MOLECULES field of the PDB table to count the occurrences
        of small molecules and output the results to a file. File will be a tab delimited
        CSV file, column 0 is the PDB code, column 1 is the count.
        """
        logging.info('Counting Aquaria small molecules')
        conn = None
        cursor = None
        f = None
        try:
            conn = MySQLdb.connect(host=settings.aquaria_db_host,
                                   db=settings.aquaria_db_name,
                                   user=settings.aquaria_db_user,
                                   passwd=settings.aquaria_db_password,
                                   charset="utf8")
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("Querying database: %s" % dt_string)

            # Store counts in a dict
            molecule_count = {}

            query = "SELECT small_molecules FROM PDB"
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            for record in result:
                r = None
                try:
                    # DB column is stored as a JSON string
                    r = json.loads(record[0])
                except Exception as je:
                    logging.error('Problem parsing record: %s' % record)
                if r:
                    for key in r.keys():
                        if key in molecule_count:
                            molecule_count[key] = molecule_count[key] + 1
                        else:
                            molecule_count[key] = 1

            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("Writing counts to file: %s" % dt_string)

            file_path = settings.temporary_file_location + 'molecule_count.txt'
            f = open(file_path, "w")
            for m_key, m_count in molecule_count.items():
                f.write("%s\t%s\n" % (m_key, m_count))

            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("File written: %s" % dt_string)
        except Exception:
            logging.error('Problem creating count')
            logging.exception('')
        finally:
            if f:
                f.close()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        logging.info('Aquaria small molecule count complete')
