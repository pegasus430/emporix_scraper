from dotenv import load_dotenv
from collections import defaultdict
from google.cloud import bigquery
import os
import requests
import logging
import gcsfs
import os
import gc


if os.path.isfile('cert/icecat-demo-612ccdfd6436.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cert/icecat-demo-612ccdfd6436.json"

g_mixin = defaultdict(list)
gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
'''
Process only English data
'''

load_dotenv()

ENDPPOINT_URL = "https://api.emporix.io"

class IceCat(object):
    """
    Base Class for all Ice Cat Mappings. Do not call this class directly.
    :param log: optional logging.getLogger() instance
    :param FILENAME: XML product index file. If None the file will be downloaded from the Ice Cat web site.
    :param auth: Username and password touple, as needed for Ice Cat website authentication
    :param data_dir: Directory to hold downloaded reference and product xml files
    """

    def __init__(self, log=None, FILENAME=None, auth=(os.environ.get("ICECAT_USERNAME"), os.environ.get("ICECAT_PASSWORD")), data_dir='_data/', lang_id='1'):
        
        self.lang_id = lang_id
        self.gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")

        logging.basicConfig(filename='IceCat_Catalogs.log', encoding='utf-8', level=logging.INFO)
        self.log = logging.getLogger()
        self.auth = auth
        self.data_dir = data_dir
        self.xml_file = self.FILENAME
        self.gcs_file_path = "gs://" + os.environ.get("GOOGLE_PRODUCT_BUCKET")+ "/"+ self.xml_file
        
        if self.gcs_file_system.exists(self.gcs_file_path) :
            self._parse(self.gcs_file_path, lang_id)
        else:
            xml_file = self._download()
            self._parse(self.gcs_file_path, lang_id)
        
        del self.gcs_file_system
        gc.collect()

    def _download(self):
        
        self.log.info("Downloading {} from {}".format(self.TYPE, self.baseurl + self.FILENAME))
 
        # check if the file exist
        try:
            f = self.gcs_file_system.open(self.gcs_file_path)
            return self.gcs_file_path
        except FileNotFoundError:
            res = requests.get(self.baseurl + self.FILENAME, auth=self.auth, stream=True)
            with self.gcs_file_system.open(self.gcs_file_path, 'wb') as f:
                for chunk in res.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                f.close()
            self.log.debug("Got headers: {}".format(res.headers))

            if 200 <= res.status_code < 299:
                return self.gcs_file_path
            else:
                self.log.error("Did not receive good status code: {}".format(res.status_code))
                return False

class IceCatDatabase(IceCat):
    """
    Searching by query from the bigquery
    """
    def __init__(self,  *args, **kwargs):
               
        self.suppliers = None
        self.categories = None
        self.extended_categoryIds = []
        self.languages = None
        self.catalogs = None
        self.key_count = 0
        self._namespaces = {
            'Product_ID': 'product_id',
            'Updated': 'updated',
            'Quality': 'quality',
            'Supplier_id': 'supplier_id',
            'Prod_ID': 'prod_id',
            'Catid': 'catid',
            'On_Market': 'on_market',
            'Model_Name': 'model_name',
            'Product_View': 'product_view',
            'HighPic': 'highpic',
            'HighPicSize': 'highpicsize',
            'HighPicWidth': 'highpicwidth',
            'HighPicHeight': 'highpicheight',
            'Date_Added': 'date_added',
        }
  
    def getIndexFromDB(self, search_data):
        search_region = search_data.search_region
        search_string = search_data.search_string
        search_string = search_string.lower()
        client = bigquery.Client()

        if search_string != "":
            if search_region == "category":
                query = f"SELECT * FROM icecat_data_dataset.category WHERE lower(category_name) like '%{search_string}%' LIMIT 1000"
            else:
                query = f"SELECT * FROM icecat_data_dataset.supplier WHERE lower(supplier_name) like '%{search_string}%' LIMIT 1000"
        else:
            if search_region == "category":
                query = f"SELECT * FROM icecat_data_dataset.category LIMIT 1000"
            else:
                query = f"SELECT * FROM icecat_data_dataset.supplier LIMIT 1000"

        query_job = client.query(query)

        results = query_job.result() 

        if  results.total_rows:  
            results_str = []
            for row in results:
                if search_region ==  "category":
                    results_str.append({"category_id" : row.category_id , "category_name": row.category_name, "parent_category_id" : row.parent_cat_id})
                    
                else:
                    results_str.append({ "supplier_id" : row.supplier_id  , "supplier_name" : row.supplier_name})
                    
                    

            if search_string == "":
                if search_region == "category":
                    query = f"SELECT count(*) as total FROM icecat_data_dataset.category"
                else:
                    query = f"SELECT count(*) as total FROM icecat_data_dataset.supplier"
                
                query_job = client.query(query)

                results = query_job.result() 

                if  results.total_rows:  
                    for row in results:
                        total = row.total

                    return {'total' : total , 'result' : results_str}
                    
            else:
                return {'result' : results_str}
        else:
            return { "inform" : "There are not any matched items"}

    def getNumberOfCatalogs(self, categoryIds, supplierIds ):
        
        client = bigquery.Client()
        for category in categoryIds:
            subCategoryIds = []
            query = f'with recursive category_tree as ( \
                SELECT * FROM `icecat-demo.icecat_data_dataset.category`  \
                where category_id = "{category}" \
                UNION ALL \
                SELECT c.* FROM `icecat-demo.icecat_data_dataset.category` as c, category_tree as tree  \
                where c.parent_cat_id = tree.category_id \
                ) \
            select * from category_tree'
            
            query_job = client.query(query)

            results = query_job.result() 

            if  results.total_rows:  
                for row in results:
                    subCategoryIds.append(row['category_id'])

            self.extended_categoryIds.extend(subCategoryIds)

        # print(self.extended_categoryIds)

        if self.extended_categoryIds:
            str_category = ",".join(f"'{w}'" for w in self.extended_categoryIds)
        else:
            str_category = "''"

        if supplierIds:
            str_supplier = ",".join(f"'{w}'" for w in supplierIds)
        else:
            str_supplier = "''"
       

        if self.extended_categoryIds and supplierIds:  
            query = f"SELECT count(*) FROM icecat_data_dataset.catalog WHERE catid in ({str_category}) and supplier_id in ({str_supplier}) "
        else:
            query = f"SELECT count(*) FROM icecat_data_dataset.catalog WHERE catid in ({str_category}) or supplier_id in ({str_supplier}) "
        

        query_job = client.query(query)
        results = query_job.result() 

        if  results.total_rows:  
            for row in results:
                number = row[0]
                

            return { "count" : number,  "success": f"There are {number} products matches with given IDs"}
        else:
            return {"count" : number,  "notfound": "No matches for these category and supplier IDs"}
            
    def getLanguageFromDB(self):
        client = bigquery.Client()
        query = f"SELECT * FROM icecat_data_dataset.language ORDER BY code"
        query_job = client.query(query)
        results = query_job.result() 

        if  results.total_rows:  
            results_str = []
            for row in results:
                results_str.append({ "language_id" : row.language_id  , "code" : row.code , "short_code" : row.short_code})

            return {'result' : results_str}
        else:
            return { "inform" : "There are not any  items"}
    

        self.extended_categoryIds = []
        client = bigquery.Client()
        for category in categoryIds:
            subCategoryIds = []
            query = f'with recursive category_tree as ( \
                SELECT * FROM `icecat-demo.icecat_data_dataset.category`  \
                where category_id = "{category}" \
                UNION ALL \
                SELECT c.* FROM `icecat-demo.icecat_data_dataset.category` as c, category_tree as tree  \
                where c.parent_cat_id = tree.category_id \
                ) \
            select * from category_tree'
            
            query_job = client.query(query)
            results = query_job.result() 
            if  results.total_rows:  
                for row in results:
                    subCategoryIds.append(row['category_id'])

            self.extended_categoryIds.extend(subCategoryIds)

        if self.extended_categoryIds:
            str_category = ",".join(f"'{w}'" for w in self.extended_categoryIds)
        
        query = f'SELECT catalog.supplier_id, suppliers.supplier_name, count(1) as num_prods \
            FROM `icecat-demo.icecat_data_dataset.catalog` as catalog, `icecat-demo.icecat_data_dataset.supplier` as suppliers \
            where catalog.catid in ({str_category}) \
            and catalog.supplier_id = suppliers.supplier_id \
            group by catalog.supplier_id, suppliers.supplier_name \
            order by num_prods desc \
            LIMIT 100'
        
        query_job = client.query(query)
        results = query_job.result() 

        if  results.total_rows:  
            result_str = []
            for row in results:
                result_str.append({"supplier_id" : row['supplier_id'] , "supplier_name" : row['supplier_name'] , 'num_prods': row['num_prods'] })
            return {'result' : result_str}
        else:
            return {"inform" : "nothing"}