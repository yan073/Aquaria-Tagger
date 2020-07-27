import gzip
import json
import logging
import MySQLdb
import requests
import settings
import urllib.request as urllib_request


class UnichemScraper:
    """
    Scrape the UniChem site for:
      1. Source names.
      2. Source mappings (linking records between sources).
    """

    def scrape_source_names(self):
        """
        Get source names from UniChem and populate the chemscraper chem_source table.
        """
        source_info = self.get_all_source_information()
        if len(source_info) > 0:
            conn = None
            cursor = None
            try:
                conn = MySQLdb.connect(host=settings.db_host,
                                       db=settings.db_name,
                                       user=settings.db_user,
                                       passwd=settings.db_password,
                                       charset="utf8")
                cursor = conn.cursor()
                query = """
                    INSERT INTO chem_source(id, source_name, name_long, name_label, source_description)
                    VALUES(%s, %s, %s, %s, %s)
                """
                query_data = []
                for source in source_info:
                    query_tuple = (source['src_id'], source['name'], source['name_long'],
                                   source['name_label'], source['description'])
                    query_data.append(query_tuple)
                cursor.executemany(query, query_data)
                conn.commit()
            except Exception:
                logging.exception('')
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        else:
            logging.error('Source information is empty')


    def get_all_source_information(self):
        """
        Query UniChem for source information
        :return: an array of source information
        """
        logging.info("Querying source information")
        src_ids_url = settings.unichem_rest_url + 'src_ids'
        req_src_ids = requests.get(url=src_ids_url)
        src_list = []
        if req_src_ids.status_code == 200:
            source_list = json.loads(req_src_ids.text)
            for source in source_list:
                src_id = source['src_id']
                src_info_url = settings.unichem_rest_url + 'sources/' + str(src_id)
                req_src_info = requests.get(url=src_info_url)
                if req_src_info.status_code == 200:
                    source_info = json.loads(req_src_info.text)
                    src_list.append(source_info[0])
                else:
                    logging.error('Problem retrieving source information from UniChem: %s' % req_src_info.text)
        else:
            logging.error('Problem retrieving source ids from UniChem: %s' % req_src_ids.text)
        logging.info('Finished retrieving source information')
        return src_list


    def download_unichem_source_mapping(self, from_id, to_id):
        """
        Download a source mapping file from UniCHem mapping records from source from_id to source to_id.
        :param from_id: the ID of the 'from' source (chem_source.id, from UniChem)
        :param to_id: the ID of the 'to' source (chem_source.id, from UniChem)
        :return: path to local file if successful, None if not
        """
        logging.info('Downloading source mapping for source %s' % from_id)
        try:
            zip_filename = 'src' + str(from_id) + 'src' + str(to_id) + '.txt.gz'
            txt_filename = zip_filename[0:len(zip_filename)-3]
            download_location = settings.unichem_ftp_url + 'src_id' + str(from_id) + '/' + zip_filename
            local_zip_file = settings.temporary_file_location + zip_filename
            urllib_request.urlretrieve(download_location, local_zip_file)
            logging.info('File downloaded, unzipping')
            f = gzip.open(local_zip_file, 'rb')
            file_content = f.read()
            file_content = file_content.decode('utf-8')
            f.close()
            local_txt_file = settings.temporary_file_location + txt_filename
            unzipped_file = open(local_txt_file, 'w+')
            unzipped_file.write(file_content)
            unzipped_file.close()
            return local_txt_file
        except Exception:
            logging.exception('')
            return None


    def scrape_mapping(self, from_source_id, to_source_id):
        """
        Populate the chem_mapping table linking records from source from_source_id to source to_source_id.
        :param from_source_id: the ID of the 'from' source (chem_source.id, from UniChem)
        :param to_source_id: the ID of the 'to' source (chem_source.id, from UniChem)
        """
        # UniChem mapping files are from lower ID to higher ID.
        if from_source_id >= to_source_id:
            logging.error('from_source_id must be less than to_source_id')
            return
        logging.info('Scraping from source %s to %s' % (from_source_id, to_source_id))
        mapping_file = self.download_unichem_source_mapping(from_source_id, to_source_id)
        if mapping_file:
            conn = None
            cursor = None
            try:
                conn = MySQLdb.connect(host=settings.db_host,
                                       db=settings.db_name,
                                       user=settings.db_user,
                                       passwd=settings.db_password,
                                       charset="utf8")
                cursor = conn.cursor()
                query_data = []
                with open(mapping_file) as sfin:
                    for line in sfin:
                        split_line = line.strip().split()
                        if len(split_line) == 2:
                            # PDB has no records in our DB so must always be the 'FROM' source,
                            # which is a problem if the mapping from_source_id < 3 (PDB)
                            if to_source_id == 3 and from_source_id < to_source_id:
                                to_s_id = from_source_id
                                f_val = split_line[0].strip()
                            else:
                                to_s_id = to_source_id
                                f_val = split_line[1].strip()
                            query = 'SELECT id FROM chem_record WHERE source_id = %s AND source_key = %s'
                            cursor.execute(query, (f_val, to_s_id))
                            result = cursor.fetchone()
                            if result:
                                # PDB has no records, must always be the 'FROM' source
                                if to_source_id == 3 and from_source_id < to_source_id:
                                    f_id = to_source_id
                                    f_val = split_line[1].strip()
                                    t_id = from_source_id
                                else:
                                    f_id = from_source_id
                                    f_val = split_line[0].strip()
                                    t_id = to_source_id
                                query_data.append((f_id, f_val, t_id, result[0]))
                sfin.close()
                query = """
                    INSERT INTO chem_mapping (from_source_id, from_id, to_source_id, to_id)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.executemany(query, query_data)
                conn.commit()

                # Delete file if not needed, I've kept it for testing and validation
                #os.remove(mapping_file)
            except Exception:
                logging.error('error scraping mapping')
                logging.exception('')
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        else:
            logging.error('No mapping file exists, maybe try reversing sources?')
        logging.info('Scraping complete')
