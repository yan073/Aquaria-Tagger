CREATE DATABASE IF NOT EXISTS chemscraper;

USE chemscraper;

/*
chem_source
The sources of the chemical names, populated via UniChem using the scrape_source_names method.
The only field actually used is id, the others are for information purposes.
Fields:
  id - the UniChem ID of the source (primary key).
  source_name - short version of name suitable for processing.
  name_label - more detailed name of source.
  name_long - a longer version of the name.
  source_description - description of source.
*/
CREATE TABLE IF NOT EXISTS `chem_source` (
  `id` int NOT NULL,
  `source_name` text DEFAULT NULL,
  `name_label` text DEFAULT NULL,
  `name_long` text DEFAULT NULL,
  `source_description` text DEFAULT NULL,
  PRIMARY KEY (`id`)
);

/*
chem_record
Records harvested from the sources. IDs only, names can be be found in chem_name.
Fields:
  id - primary key.
  source_key - the source the record was harvested form, foreign key to chem_source.
  source_id - the ID of the record at the source.
*/
CREATE TABLE IF NOT EXISTS `chem_record` (
  `id` int NOT NULL,
  `source_key` int DEFAULT NULL,
  `source_id` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `chem_source_fkey_idx` (`source_key`),
  CONSTRAINT `chem_source_fkey` FOREIGN KEY (`source_key`) REFERENCES `chem_source` (`id`),
  INDEX(source_key, source_id(12))
);

/*
chem_name
Names and synonyms associated with each chem_record.
Fields:
  id - primary key.
  compound_name - name or synonym associated with the record.
  record_key - the associated record, foreign key to chem_record.
*/
CREATE TABLE IF NOT EXISTS `chem_name` (
  `id` int NOT NULL AUTO_INCREMENT,
  `compound_name` text DEFAULT NULL,
  `record_key` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `chem_record_fkey_idx` (`record_key`),
  CONSTRAINT `chem_record_fkey` FOREIGN KEY (`record_key`) REFERENCES `chem_record` (`id`)
);

/*
chem_mapping
Represents mapping between records of different sources.
Notes:
  * Doesn't have foreign key for from_id as these are mainly PDB records which aren't actually in the DB.
  * from_source_id and to_source_id should be foreign keys into chem_source.
Fields:
  id - primary key.
  from_source_id - the ID of the source for the 'from' records.
  from_id - the ID of the 'from' record at the source. Not a FK into chem_record as mostly contains PDB
            records not in DB.
  to_source_id - the ID of the source for the 'to' records.
  to_id - the ID of the 'to' record at the source, foreign key into chem_record.
*/
CREATE TABLE IF NOT EXISTS `chem_mapping` (
  `id` int NOT NULL AUTO_INCREMENT,
  `from_source_id` int DEFAULT NULL,
  `from_id` text DEFAULT NULL,
  `to_source_id` int DEFAULT NULL,
  `to_id` int DEFAULT NULL,
  KEY `chem_record_to_id_fkey_idx` (`to_id`),
  CONSTRAINT `chem_record_to_id_fkey` FOREIGN KEY (`to_id`) REFERENCES `chem_record` (`id`),
  PRIMARY KEY (`id`)
);

/*
pdb_components
The components harvested from the PDB components.cif file.
Could be merged into chem_record/chem_name if required.
*/
CREATE TABLE IF NOT EXISTS `pdb_components` (
  `id` int NOT NULL AUTO_INCREMENT,
  `component_id` text DEFAULT NULL,
  `component_name` text DEFAULT NULL,
  `component_formula` text DEFAULT NULL,
  `component_mon_nstd_parent_comp_id` text DEFAULT NULL,
  PRIMARY KEY (`id`)
);
