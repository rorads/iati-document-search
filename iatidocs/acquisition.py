import requests as rq
import pandas as pd
import json
from lxml import etree
import os
from tika import parser
from tqdm import tqdm
import hashlib

# Get Document Data
def get_documents(row_count, directory='data/output.json'):
    """
    Essentially a wrapper around wget, saving a very large JSON file to a given directory.
    """

    solr_string = ("https://iatidatastore.iatistandard.org/search/activity?"
                        "q=document_link_url:[*%20TO%20*]"
                        "&fl=iati_identifier,reporting_org_ref,document_link_xml"
                        "&wt=json"
                        f"&rows={row_count}")

    # wget the file from SOLR. Full IATI data is about 500MB, taking 5mins 
    # using reasonable internet
    os.system(f'wget "{solr_string}" -O {directory}')

    # print the first 10 lines for diagnostic.
    os.system(f'head -10 {directory}')
    # count the number of iati_identifers in the file
    os.system(f'grep "iati_identifier\\":" {directory} | wc -l')

def clean_json(input, output):
    """
    Given the directory of a raw JSON file from SOLR, cleans the response
    such that only the 'response/docs' elements remain, ready to be 
    used in a DataFrame.

    Returns final directory
    """
    with open(input) as inf:
        response = json.load(inf)
        response = response['response']['docs']
        with open(output, 'w+') as outf:
            json.dump(response, outf)
        outf.close()
    inf.close()

    return output

def unpack_xml(xml_string_array):
    """
    Given a list of xml slugs, returns an serialised dict
    """
    documents = []
    for document_xml in xml_string_array:
        tree = etree.fromstring(document_xml)
        doc_entry = {
            "url": tree.get('url'),
            "title": tree.xpath("title/narrative/text()")[0] if tree.xpath("title/narrative/text()") else None, # get first instance
            "date": tree.xpath("document-date/@iso-date")[0] if tree.xpath("document-date/@iso-date") else None, # get first instance
            "categories": tree.xpath("category/@code")
        }
        documents.append(doc_entry)
    return documents
        
def stitch_document_dicts(dataframe):
    """
    Given a dataframe with a document dict array column, return a new dataframe
    with urls as the index (one row per unique URL, other features should take the first value
    apart from tags which should combine all unique into a set)
    """
    
    document_dict_list = []
    
    [document_dict_list.extend(x) for x in dataframe['document_dicts']]
    
    return_df = pd.DataFrame(document_dict_list).groupby('url').aggregate({
            'date': lambda x: list(x)[0], # get first
            'title': lambda x: list(x)[0], # get first
            'categories': lambda x: set().union(*x) # get full union
            }).reset_index()

    #### Initial logic for caching document content - needs refactor
    # hashes = []
    
    # for url in tqdm(return_df['url']):
    #     try: 
    #         hashes.append(hashlib.md5(rq.get(url).content))
    #     except Exception as e:
    #         hashes.append("error_retreiving_file")

    # return_df['content_hash'] = hashes
    
    return return_df

def lazy_tika_parse(url):
    try:
        parsed_file = parser.from_buffer(rq.get(url).content)
        return parsed_file
    except Exception as e:
        print(e)
        return None

def main():
    """
    Primary logic for wrangling, including some messy scripting
    """
    # get_documents(500000, 'data/output.json') # full call using defaults
    # clean_json('data/output.json', 'data/cleaned_out.json')
    
    # doc_rows = pd.read_json('data/cleaned_out.json')
    # Get df with small sample size for analysis and drafting
    doc_rows = pd.read_json('data/cleaned_out.json').sample(n=1).reset_index()

    doc_rows['document_dicts'] = doc_rows['document_link_xml'].apply(unpack_xml)

    document_dataframe = stitch_document_dicts(doc_rows)

    # TODO: caching document content in initial parse and then using cache to determine which documents to update (?)

    # This approximately 1 minute per 50 documents.
    document_dataframe['tika_object'] = [lazy_tika_parse(x) for x in tqdm(document_dataframe['url'])]

    pass # debug waypoint

if __name__ == '__main__':
    main()