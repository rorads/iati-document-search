# Plan

Get IATI documents, make them easier to use and analyse

## Purpose

This software acts as an gateway to all documents published to via the IATI standard. It presents a single table of document links with content saved in a simplified text format. After the initial parse of the documents, caching requires that only new or altered documents require updating.

Original documents will **not** be stored - only a text representation of each.

## Requirements

1. Index **all** IATI documents in a single flat-file
2. Visualise all alike documents (Annual Reports, Evaluation Reports) in an LDA-style topic space
3. Allow for content search
4. Docker deploy with simple setup

## Quick Plan

- [x] Setup with python3 via docker. will require pandas, requests, lxml
- [x] Pull all documents using [this endpoint.](https://iatidatastore.iatistandard.org/search/activity?q=document_link_url:[*%20TO%20*]&fl=iati_identifier,reporting_org_ref,document_link_xml&wt=json&rows=50) 
  * This will require unpacking string XML from within JSON, but should give all the data we need in one big JSON file
- [x] Reshape data into a dataframe
  - [x] Create additional unique document dataframe with the URL as the id
- [x] Use TIKA to parse text and metadata (requires java in the dockerfile)
- [ ] Use Dask or another parallel processing framework to pull the content, save the text, hash, and discard the document.
  * ~~Either will require pandoc, tika, or some other document handler - using a collection of different libraries in python would be annoying.~~ Using TIKA...
- [ ] Pickle / serialise using JSON / SQLite