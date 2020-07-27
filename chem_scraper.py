from datetime import datetime
import logging
import MySQLdb
import MySQLdb.cursors
import settings
import sys
from pdb_scraper import PDBScraper
from unichem_scraper import UnichemScraper
from aquaria_scraper import AquariaScraper


class ChemScraper:
    """
    Scrape chemical sites for compound names, synonyms and record mappings.
    """
    logging.basicConfig(filename='chem_scraper.log', level=logging.DEBUG)


    def usage(self):
        print('Usage:')
        print('  python3 chem_scraper [option]')
        print('')
        print('Options:')
        print('  -h                                     Show this message')
        print('  scrape_source_names                    Populate sources table from UniChem')
        print('  scrape_all                             Scrape all sources defined in settings.source_list')
        print('  scrape_source <source_id>              Scrape a particular source specified by the UniChem')
        print('                                         source_id')
        print('  scrape_all_mappings                    Scrape record mappings for sources defined in')
        print('                                         settings.source_list')
        print('  scrape_mapping <from_id> <to_id>       Scrape record mappings for source <from_id> to source')
        print('                                         <to_id>')
        print('  scrape_pdb                             Scrape')
        print('  create_pdb_synonym_list <source_id>    Scrape')
        print('  create_source_synonym_list <source_id> Scrape')
        print('  count_small_molecules                  Scrape')


    def scrape_source_names(self):
        """
        Scrape source name information from UniChem and populate chemscraper.chem_source DB table
        with the information.
        """
        logging.info('Scraping source names')
        UnichemScraper().scrape_source_names()
        logging.info('Scraping complete')


    def scrape_source(self, source_id):
        """
        Scrape a specific source for compound names and synonyms and populate the
        chemscraper.chem_record and chem_scraper.chem_name tables.
        :param source_id: the ID of the source to scrape as defined in settings.py (will match
        the UniChem surce ID)
        """
        now = datetime.now()
        try:
            source = settings.source_list[source_id]
            logging.info("Scraping source %s at %s" % (source_id, now.strftime("%Y-%m-%d %H:%M:%S")))
            module = __import__(source['module_name'])
            class_ = getattr(module, source['class_name'])
            scraper = class_()
            scraper.scrape()
            now = datetime.now()
            logging.info("Scraping complete at %s" % now.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            logging.error('Error scraping')
            logging.exception('')


    def scrape_all_sources(self):
        """
        Scrape all sources defined in settings.source_list for compound names and synonyms
        and populate the chemscraper.chem_record and chem_scraper.chem_name tables.
        """
        logging.info('Scraping all sources')
        for source_id in settings.source_list:
            self.scrape_source(source_id)
        logging.info('All scraping complete')


    def scrape_mapping(self, from_id, to_id):
        """
        Populate the chemscraper.chem_mapping table with mappings between the records of 2
        different sources.
        :param from_id: the 'from' source ID
        :param to_id: the 'to' source ID
        """
        logging.info('Scraping mapping from %s to %s' % (from_id, to_id))
        UnichemScraper().scrape_mapping(from_id, to_id)


    def scrape_pdb(self):
        """
        Scrape PDB site for components to populate the chem_scraper.chem_component table.
        """
        PDBScraper().scrape()


    def scrape_all_mappings(self):
        """
        Scrape all record mappings from sources defined in settings.source_list.
        """
        logging.info('Scraping mappings')
        # UniChem only stores mappings from lower id to higher id, so sort list
        sorted_source_list = sorted(settings.source_list)
        for i in range(len(sorted_source_list)):
            if i is not len(sorted_source_list) - 1:
                for j in range(i + 1, len(sorted_source_list)):
                    self.scrape_mapping(sorted_source_list[i], sorted_source_list[j])
        logging.info('Scraping complete')


    def create_pdb_synonym_list(self, source_id = None):
        """
        Create a synonym list for records at a specific source or all sources. Synonyms will map the PDB code
        to the synonyms found in the chemscraper DB at the source (or sources). E.g (names not abbreviated in file):

        6QR    (2S,3R,4R)-2-[(1S,2S)-1-formyl...
        6QR    (2S,3R,4R)-2-[(2S,3S)-3-hydrox...
        6QR    (2<I>S</I>,3<I>R</I>,4<I>R</I>...
        NUB    1,3-dimethyl-5-[1-(tetrahydrop...
        ... etc ...

        :param source_id: the source ID to map to PDB codes. If omitted will map from all sources.
        """
        if source_id == 3:
            logging.error('This function maps TO source 3 (PDB), please choose a different source')
            return
        logging.info('Creating PDB synonym file')
        if source_id:
            logging.info('Source: %s' % source_id)
        conn = None
        cursor = None
        f = None
        try:
            conn = MySQLdb.connect(host=settings.db_host,
                                   db=settings.db_name,
                                   user=settings.db_user,
                                   passwd=settings.db_password,
                                   charset="utf8")
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("Querying database: %s" % dt_string)
            if source_id is None:
                query = """
                    SELECT m.from_id, n.compound_name FROM chem_mapping as m
                    JOIN chem_record as r ON m.to_id = r.id
                    JOIN chem_name as n ON n.record_key = r.id;
                """
                file_name = "pdb_synonyms.txt"
            else:
                query = """
                    SELECT m.from_id, n.compound_name FROM chem_mapping as m
                    JOIN chem_record as r ON m.to_id = r.id
                    JOIN chem_name as n ON n.record_key = r.id
                    WHERE m.to_source_id = {to_source_id};
                """.format(to_source_id=source_id)
                file_name = "pdb_synonyms_src_%s.txt" % source_id
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("Querying complete, writing file: %s" % dt_string)
            file_path = settings.temporary_file_location + file_name
            f = open(file_path, "w")
            for record in results:
                if source_id > 3:
                    from_id = record[0]
                    to_id = record[1]
                else:
                    from_id = record[1]
                    to_id = record[0]
                f.write("%s    %s\n" % (from_id, to_id))
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("File written: %s" % dt_string)
        except Exception:
            logging.error('Problem creating synonym list')
            logging.exception('')
        finally:
            if f:
                f.close()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        logging.info('Synonym file creation complete')


    def create_source_synonym_list(self, source_id = None):
        """
        Create a synonym list for records at a specific source or all sources. Synonyms will map the ID at the source
        to the synonyms found in the chemscraper DB at the same source (or sources).
        E.g (names not abbreviated in file):

        DB00001 Lepirudin recombinant
        DB00002 Cetuximab
        DB00002 Cetuximab
        DB00002 Cétuximab
        DB00002 Cetuximabum
        ... etc ...

        :param source_id: the source ID to map IDs to names. If omitted will map from all sources.
        """
        logging.info('Creating source synonym file')
        if source_id:
            logging.info('Source: %s' % source_id)
        conn = None
        cursor = None
        f = None
        try:
            conn = MySQLdb.connect(host=settings.db_host,
                                   db=settings.db_name,
                                   user=settings.db_user,
                                   passwd=settings.db_password,
                                   charset="utf8",
                                   cursorclass=MySQLdb.cursors.SSCursor)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("Querying database: %s" % dt_string)
            if source_id is None:
                query = """
                    SELECT r.source_id, n.compound_name
                    FROM chem_name as n JOIN chem_record as r
                    ON r.id = n.record_key
                """
                file_name = "source_synonyms.txt"
            else:
                query = """
                    SELECT r.source_id, n.compound_name
                    FROM chem_name as n JOIN chem_record as r
                    ON r.id = n.record_key
                    WHERE r.source_key = {source_key}
                """.format(source_key=source_id)
                file_name = "source_synonyms_src_%s.txt" % source_id

            file_path = settings.temporary_file_location + file_name
            f = open(file_path, "w")

            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchmany(size=1000)
            while rows:
                for line in rows:
                    f.write("%s\t%s\n" % (line[0], line[1]))
                rows = cursor.fetchmany(size=1000)

            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logging.info("File written: %s" % dt_string)
        except Exception:
            logging.info('Problem creating synonym list')
            logging.exception('')
        finally:
            if f:
                f.close()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        logging.info('Synonym file creation complete')


    def count_small_molecules(self):
        """
        Create a small molecule count file. File will be a tab delimited CSV file where column 0
        is the PDB code of the molecule, column 1 is the occurrence count.
        """
        AquariaScraper().count_small_molecules()


    def run(self):
        """
        Parse the arguments and run the requested method
        """
        if (len(sys.argv) < 2 or sys.argv[1].lower() == "-h"):
            self.usage()
        elif sys.argv[1].lower() == 'scrape_source_names' and len(sys.argv) == 2:
            self.scrape_source_names()
        elif sys.argv[1].lower() == 'scrape_all' and len(sys.argv) == 2:
            self.scrape_all_sources()
        elif sys.argv[1].lower() == 'scrape_source' and len(sys.argv) == 3:
            self.scrape_source(int(sys.argv[2]))
        elif sys.argv[1].lower() == 'scrape_all_mappings' and len(sys.argv) == 2:
            self.scrape_all_mappings()
        elif sys.argv[1].lower() == 'scrape_mapping' and len(sys.argv) == 4:
            self.scrape_mapping(int(sys.argv[2]), int(sys.argv[3]))
        elif sys.argv[1].lower() == 'scrape_pdb' and len(sys.argv) == 2:
            self.scrape_pdb()
        elif sys.argv[1].lower() == 'create_pdb_synonym_list':
            if len(sys.argv) == 2:
                self.create_pdb_synonym_list()
            elif len(sys.argv) == 3:
                self.create_pdb_synonym_list(int(sys.argv[2]))
            else:
                self.usage()
        elif sys.argv[1].lower() == 'create_source_synonym_list':
            if len(sys.argv) == 2:
                self.create_source_synonym_list()
            elif len(sys.argv) == 3:
                self.create_source_synonym_list(int(sys.argv[2]))
            else:
                self.usage()
        elif sys.argv[1].lower() == 'count_small_molecules':
            self.count_small_molecules()
        else:
            self.usage()


if __name__ == "__main__":
    ChemScraper().run()
