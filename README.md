## Aim and Development

Initially at least, this project has one user journey:

> **As someone** wanting to analyse the text of many documents published via IATI
>
> **I want to** access all live documents, regardless of filetype, with both raw text and metadata in a single place
>
> **So that** I can run arbitrary analysis on the text, and search through the entire corpus.

The logic flow so far is as follows:

1. get_documents(row_count, directory='data/output.json')
    * pulling all documents from SOLR
2. clean_json(input, output)
    * removing the wrapper and extracting just the document rows
3. unpack_xml(xml_string_array)
    * extracting all the document declarations from the XML slugs
4. stitch_document_dicts(dataframe)
    * creating a unified document table
    * **note** this and all previous steps are unique to IATI, and could be abstracted into a document_access_object style module
5. lazy_tika_parse(url)
    * given a dataframe full of URLs, pass them to TIKA and disregard errors

TODO: think about how to store and serve this data - could be a big JSON file, or more likely an SQLite DB, given that we might want to keep the raw documents as well as the text and metadata.

See [high_level.md](planning/high_level.md) for more details.

## Deployment

Currently, this project is designed to run in a docker container within VSCode using default run configurations.

Steps:

* Git Clone
* Open in VSCode (with the `Remote-Containers` extension installed)
* Select 'reopen in container'
* Wait for docker to run (approx 2 minutes)
* Run the python file with the default configuration.