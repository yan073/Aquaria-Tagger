# Aquaria-Tagger
<p>All operations via the chem_scraper.py script.</p>
<p>Usage:</p>
python3 chem_scraper [option]
<p></p>
<p>Options:</p>
<table>
<tr><td>-h</td><td>Show usage</td></tr>
<tr><td>scrape_source_names</td><td>Populate sources table from UniChem</td></tr>
<tr><td>scrape_all</td><td>Scrape all sources defined in settings.source_list</td></tr>
<tr><td>scrape_source &lt;source_id&gt;</td><td>Scrape a particular source specified by the UniChem source_id</td></tr>
<tr><td>scrape_all_mappings</td><td>Scrape record mappings for sources defined in settings.source_list</td></tr>
<tr><td>scrape_mapping &lt;from_id&gt; &lt;to_id&gt;</td><td>Scrape record mappings for source &lt;from_id&gt; to source &lt;to_id&gt;</td></tr>
<tr><td>scrape_pdb</td><td>Populate the chemscraper.chem_component table from PDB</td></tr>
<tr><td>create_pdb_synonym_list &lt;source_id&gt;</td><td>Create a mapping file between PDB and a source</td></tr>
<tr><td>create_source_synonym_list &lt;source_id&gt;</td><td>Create a synonym file for a source</td></tr>
<tr><td>count_small_molecules</td><td>Create a file of smal moecule counts</td></tr>
</table>
