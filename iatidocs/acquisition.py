import requests as rq
import pandas as pd
import json
from lxml import etree
import os
from tika import parser
from tqdm import tqdm
import hashlib
import concurrent.futures
import dask
import dask.dataframe as dd
import dask.bag as bag
from dask.distributed import Client, progress


class IATIAcquisition:

    # Get Document Data
    def get_documents(self, row_count=1000000, directory='data/output.json'):
        """
        Essentially a wrapper around wget, saving a very large JSON file to a given directory.
        
        row_count -- the number of activities

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
    
    def clean_json(self, input='data/output.json', output='data/cleaned_out.json'):
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
                json.dump(response, outf, indent=0)
            outf.close()
        inf.close()

        return output

    @staticmethod
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
            
    def stitch_document_dicts(self, dataframe) -> pd.DataFrame:
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
        
        return return_df

    def lazy_tika_parse(self, url):
        """
        Given a URL, uses TIKA to parse, and returns the full
        parsed_file dict, having inserted a sha224 hash of the
        content as another entry. This allows for a chaching of
        the documents based on content so that only new documents
        need be updated in future.

        returns a '_failure' token as the hash if an invalid 
        URL is given or a non 200 status code is generated.

        """

        try:
            response = rq.get(url)
            if response.status_code == 200:
                content = rq.get(url).content
                parsed_file = parser.from_buffer(content)
                parsed_file['sha224'] = hashlib.sha224(content).hexdigest()
                return parsed_file
            else: 
                return {'_failure': f"failed request, status: {response.status_code}"}
        except Exception as e:
            print(e)
            return {'_failure': f"failed request, exception: {e}"}

    def concurrent_run_tika(self, f, url_list):
        """
        With 8 max workers, this runs at roughly 0.246 seconds per document parse,
        translating to roughly 250 documents per minute. IATI has just under
        500k activities with at least one document. Assuming an average of 10 
        documents per activity, that means 
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(tqdm(executor.map(f, url_list), total=len(url_list)))
        return results

    def initialise_iati(self):
        # self.get_documents()

        # print('cleaning...')
        # self.clean_json()

        print('loading activities in dask...')
        activity_rows = dd.read_json('data/cleaned_out.json', blocksize=100)

        print("sampling...")
        print(activity_rows.sample(0.0001).head())

        # print('unpacking xml...')
        # activity_rows['document_dicts'] = activity_rows['document_link_xml'].map(self.unpack_xml)
        
        # print('unfolding document dataframe...')
        # document_dataframe = self.stitch_document_dicts(activity_rows)

        # print('parsing with tika...')
        # document_dataframe['tika_object'] = [self.lazy_tika_parse(x) for x in tqdm(document_dataframe['url'])] # series
        # # document_dataframe['tika_object'] = self.concurrent_run_tika(self.lazy_tika_parse, document_dataframe['url'])
        
        # document_dataframe['status'] = ['failed' if '_failure' in x.keys() else 'success' for x in document_dataframe['tika_object']]
        # document_dataframe.to_json('data/document_dataframe.gzip', compression='gzip')

def main():
    """
    Primary logic for wrangling, including some messy scripting
    """
    acquisition = IATIAcquisition()

    acquisition.initialise_iati()

    # loaded_doc_df = pd.read_json('data/document_dataframe.gzip', compression='gzip')

    pass # debug waypoint

if __name__ == '__main__':
    client = Client(threads_per_worker=2,
                n_workers=2,
                memory_limit='3GB')

    print(client)
    
    main()