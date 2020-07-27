import logging
import MySQLdb
import settings
import shlex
import urllib.request as urllib_request


class PDBScraper:
    """
    Scrape PDB for components.
    """

    def scrape(self):
        """
        Populate the chemscraper.chem_component table from components downloaded from PDB.
        """
        logging.info('Scraping PDB')
        local_file = self.download_components_file()
        if local_file:
            current_id = None
            current_name = None
            current_formula = None
            current_mon_nstd_parent_comp_id = None
            current_line = 0
            multi_line_name = ''
            reading_multi_line_name = False
            query_data = []
            problem_id_list = []
            fin = None
            conn = None
            cursor = None
            try:
                with open(local_file) as fin:
                    for line in fin:
                        # New chemical, deal with the old one if all data was there
                        if reading_multi_line_name:
                            if line.strip().startswith('_chem_comp.'):
                                current_name = multi_line_name
                                reading_multi_line_name = False
                                multi_line_name = ''
                            else:
                                name_part = line.strip()
                                # Hopefully no names use the ';' character and that character is at the start of a line
                                if name_part.startswith(';'):
                                    name_part = name_part[1:len(name_part) + 1]
                                multi_line_name = multi_line_name + name_part
                        else:
                            split_line = self.c_split(line.strip())
                            if len(split_line) == 1 and split_line[0].startswith('data'):
                                if current_id is not None and current_name is not None and current_formula is not None and current_mon_nstd_parent_comp_id is not None:
                                    query_data.append((current_id, current_name, current_formula, current_mon_nstd_parent_comp_id))
                                elif current_line != 0:
                                    problem_id = 'unknown'
                                    if current_id is not None:
                                        problem_id = current_id
                                    problem_id_list.append(problem_id)
                                current_id = None
                                current_name = None
                                current_formula = None
                                current_mon_nstd_parent_comp_id = None
                            # Single line name, this means names will be on subsequent lines prepended with ';'
                            elif len(split_line) == 1 and split_line[0] == '_chem_comp.name':
                                reading_multi_line_name = True
                            elif len(split_line) == 2:
                                if split_line[0] == '_chem_comp.id':
                                    current_id = split_line[1]
                                elif split_line[0] == '_chem_comp.name':
                                    current_name = split_line[1]
                                elif split_line[0] == '_chem_comp.formula':
                                    current_formula = split_line[1]
                                elif split_line[0] == '_chem_comp.mon_nstd_parent_comp_id':
                                    current_mon_nstd_parent_comp_id = split_line[1]
                        current_line = current_line + 1
                    # Deal with last component
                    if current_id is not None and current_name is not None and current_formula is not None and current_mon_nstd_parent_comp_id is not None:
                        query_data.append((current_id, current_name, current_formula, current_mon_nstd_parent_comp_id))
                    else:
                        problem_id = 'unknown'
                        if current_id is not None:
                            problem_id = current_id
                        problem_id_list.append(problem_id)

                conn = MySQLdb.connect(host=settings.db_host,
                                       db=settings.db_name,
                                       user=settings.db_user,
                                       passwd=settings.db_password)
                cursor = conn.cursor()
                query = """
                    INSERT INTO pdb_components(
                        component_id, component_name, component_formula,
                        component_mon_nstd_parent_comp_id)
                    VALUES(%s, %s, %s, %s)
                """
                cursor.executemany(query, query_data)
                conn.commit()
            except Exception:
                logging.error('Problem reading file, line = %s' % current_line)
                logging.exception('')
                if current_id:
                    logging.error('ID: %s' % current_id)
                if current_name:
                    logging.error('Name: %s' % current_name)
                logging.exception('')
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
                if fin:
                    fin.close()

            # Remove file if not needed, currently kept for validation and testing
            #os.remove(local_file)
        logging.warning('Problem IDs: %s' % problem_id_list)
        logging.info('Scraping complete')


    def c_split(self, value):
        """
        Split lines
        :param value: the line to split
        :return: an array of split values
        """
        lex = shlex.shlex(value)
        lex.quotes = '"'
        lex.whitespace_split = True
        return list(lex)


    def download_components_file(self):
        """
        Download the components file from PDB.
        :return: the local file path if successful, None if not
        """
        logging.info('Downloading components file: %s' % settings.pdb_ftp_components_file)
        local_file = None
        try:
            local_file_path = settings.temporary_file_location + 'components.cif'
            urllib_request.urlretrieve(settings.pdb_ftp_components_file, local_file_path)
            local_file = local_file_path
        except Exception:
            logging.error('Error downloading file')
            logging.exception('')
        finally:
            return local_file
