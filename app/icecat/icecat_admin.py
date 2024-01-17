
from dotenv import load_dotenv
from collections import defaultdict
from google.cloud import bigquery
import gzip
import json
import os
import xml.etree.cElementTree as ET
import progressbar
import requests
import logging
import gcsfs
import os
import gc
import uuid


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

class IceCatFeatureGroupList(IceCat):
    """
    create the dict of feature group from icecat FeatureGroupsList.xml
    it is used for making mixin schema for each unique category feature
    """
    baseurl = 'https://data.Icecat.biz/export/freexml/refs/'
    FILENAME = 'FeatureGroupsList.xml.gz'
    TYPE = 'FeatureGroupList'
    
    def _parse(self, xml_file, lang_id):

        gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
        if xml_file.endswith('.gz'):
            with gcs_file_system.open(xml_file) as gzip_xml_file:
                with gzip.open(gzip_xml_file, 'rb', 'unicode') as f:
                    data = ET.parse(f).getroot()
        else:
            with gcs_file_system.open(xml_file) as f:
                data = ET.parse(f).getroot()

        '''
        Data is an XML ElementTree Object
        '''
        self.id_map = []
        self.name = ''
        self.findpath = 'Name[@langid="' + lang_id + '"]'
        self.id = ''

        for elem in data.iter('FeatureGroup'):
            self.id = elem.attrib['ID']
            
            for name in elem.iterfind(self.findpath):
                self.name = name.attrib['Value']
                break

            self.id_map.append({ 'id': self.id, 'name': self.name })
        
        del gcs_file_system
        gc.collect()

class IceCatCategoryFeatureList(IceCat):
    """
    create the dict of Category feature from icecat CategoryFeaturesList.xml
    it is used for making mixin schema for each unique category feature
    format : schema_name : all propery array
    """
    baseurl = 'https://data.Icecat.biz/export/freexml/refs/'
    FILENAME = 'CategoryFeaturesList.xml.gz'
    TYPE = 'CategoryFeaturesList'  

    def parse_xml(self, file_name, lang_id):
        events = ("start", "end")
        self.categoryfeaturegroup_matching_list = []
        self.items = {}
        self.feature_element_flag = False
        self.feature_CategoryFeature_ID = None
        self.parent_featuregrup = None
        self.top_categoryId = None
        self.feature_type = None
        self.feature_measure = False
        context = ET.iterparse(file_name, events=events)
        return self.parsefeature(context,  lang_id)

   
    def parsefeature(self,context, lang_id , cur_elem = None):
        if cur_elem:
            if cur_elem.tag == "Category":
                self.top_categoryId = cur_elem.attrib['ID']
                self.items.update({self.top_categoryId : {}})

            if cur_elem.tag == "CategoryFeatureGroup":
                featureGroup = cur_elem.find('FeatureGroup')

                featureGroup_id = featureGroup.attrib["ID"]
                self.categoryfeaturegroup_matching_list.append({cur_elem.attrib["ID"]: featureGroup_id})           
                
                # if items don't have featureGroup, and then insert it
                if self.items[self.top_categoryId].get(featureGroup_id) == None:
                    self.items[self.top_categoryId].update({featureGroup_id : {}})

            if cur_elem.tag == "Feature":
                self.key_count += 1
                self.bar.update(self.key_count)
                for feature in self.categoryfeaturegroup_matching_list:
                    if feature.get(cur_elem.attrib['CategoryFeatureGroup_ID']) != None :
                        self.parent_featuregrup_id = feature[cur_elem.attrib['CategoryFeatureGroup_ID']] 
                        break
                
                self.feature_CategoryFeature_ID = cur_elem.attrib['CategoryFeature_ID']
                self.feature_type = cur_elem.attrib['Type']

            if cur_elem.tag == "Measure":
                if cur_elem.attrib['Sign'] != "":
                    self.feature_measure = True
                else:
                    self.feature_measure = False


        for action, elem in context:
            if elem.tag != "Signs":
                if elem.tag == "Name" and self.feature_element_flag:
                        if elem.attrib['langid'] == lang_id:
                            feature_name =  elem.attrib["Value"].lower().replace(" ", "_").replace("/", "_").replace(".", "_").replace("(", "_").replace(")", "_")
                            description_text = elem.attrib["Value"]
                            try:
                                if self.items[self.top_categoryId].get(self.parent_featuregrup_id).get(feature_name) == None  or self.items[self.top_categoryId].get(self.parent_featuregrup_id).get(feature_name) == {} :
                                    
                                    if self.feature_type == "numerical":
                                        if self.feature_measure:
                                            json_content = {
                                                feature_name:{
                                                    "$ref": "https://storage.googleapis.com/"+ os.environ.get("GOOGLE_BUCKET_NAME")+ "/atomic_uom.v3",
                                                    "description": description_text
                                            }}
                                        else:
                                            json_content = {
                                                feature_name:{
                                                    "type" : ["number" , "null"],
                                                    "description" : description_text
                                            
                                                }
                                            }
                                    elif self.feature_type == "y_n":
                                        json_content = {
                                            feature_name:{
                                                "type" :["boolean" , "null"],
                                                "default" : False , 
                                                "description" : description_text
                                        
                                        }}
                                    elif self.feature_type == "range":
                                        json_content = {
                                            feature_name:{
                                                "$ref": "https://storage.googleapis.com/"+ os.environ.get("GOOGLE_BUCKET_NAME")+ "/range_uom.v3",
                                                "description": description_text
                                        }}
                                    else:
                                        json_content = {
                                            feature_name:{
                                                "type" : ["string" , "null"],
                                                "description" : description_text
                                            
                                        }}
                                    
                                    # inserting schema into json 
                                    self.items[self.top_categoryId].get(self.parent_featuregrup_id).update(json_content)
                            except:
                                self.log.error(f" - top_categoryId {self.top_categoryId} parent_featuregrup_id {self.parent_featuregrup_id}" )

                if action == "start" :
                    if elem.tag =="Feature":
                        self.feature_element_flag = True
                    self.parsefeature(context,lang_id , elem)
                elif action == "end":
                    if elem.tag =="Feature":
                        self.feature_element_flag = False
                    elem.clear()
                    break    
            
        return self.items

    def _parse(self, xml_file, lang_id):
        self.key_count = 0
        with progressbar.ProgressBar(max_value=progressbar.UnknownLength) as self.bar:
            gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
            if xml_file.endswith('.gz'):
                with gcs_file_system.open(xml_file) as gzip_file:
                    with gzip.open(gzip_file, 'rb', 'unicode') as f:
                        self.parse_xml(f, lang_id)
            else:
                with gcs_file_system.open(xml_file) as f:
                    self.parse_xml(f, lang_id)

class IceCatCategoryMapping(IceCat):
    """
    Create a dict of product category IDs to category names
    Refer to IceCat class for arguments
    """
    baseurl = 'https://data.icecat.biz/export/freexml/refs/'
    FILENAME = 'CategoriesList.xml.gz'
    TYPE = 'Categories List'
    gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
    def _parse(self, xml_file, lang_id):
        
        if xml_file.endswith('.gz'):	      
            with gcs_file_system.open(xml_file) as gzip_xml_file:	
                with gzip.open(gzip_xml_file, 'rb', 'unicode') as f:	
                    data = ET.parse(f).getroot()	    
        else:	
            with gcs_file_system.open(xml_file) as f:
                 data = ET.parse(f).getroot()
        
        '''
        Data is an XML ElementTree Object
        '''
        self.id_map = []
        self.catid = ''
        self.catname = ''
        self.description = ''
        self.findpath = 'Name[@langid="' + lang_id + '"]'
        self.parentCategoryID = ''
        self.parentName = ''
        print(" - parsing CategoriesList.xml.gz")
        for elem in data.iter('Category'):
            self.catid = elem.attrib['ID']

            for description in elem.iterfind('Description[@langid="' + lang_id + '"]'):
                self.description = description.attrib['Value']

            for name in elem.iterfind(self.findpath):  
                self.catname = name.attrib['Value']
                # only need one match
                break
            if not self.catname:
                self.catname = "Unknown"
            
            for parent in elem.iter("ParentCategory"):
                self.parentCategoryID = parent.attrib['ID']
                break
            
            self.id_map.append({ 'ID': self.catid, 'Name': self.catname, "ParentID": self.parentCategoryID, "ParentName": self.parentName , 'Description' : self.description})
            elem.clear()
        data.clear()
  
class IceCatMixin(IceCat):
    """
    Create Emporix mixin json files from icecat featurelist on local.
    """
    def __init__(self,  *args, **kwargs): 
        self.features = None
        self.categoryfeatures = None
        self.category = None
        self.data_dir = "_data/mixin/"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def makeMixin(self):
        makeCount = 0

        if not self.features:
            self.categoryfeatures = IceCatCategoryFeatureList()
            self.categoryfeatures = self.categoryfeatures.items
            self.features = IceCatFeatureGroupList()
            self.category = IceCatCategoryMapping()
            self.category = self.category.id_map

            with progressbar.ProgressBar(max_value=progressbar.UnknownLength) as bar:
                for category in self.category:
                    category_id = category['ID']
                    category_name_desciption = category['Name']
                    category_name = category_name_desciption.lower().replace(" ", "_").replace("/", "_").replace(".", "_").replace("(", "_").replace(")", "_")

                    for feature in self.features.id_map:
                        description = "Mixin schema for "  + feature['name'].lower() + " of Category : "  +category_name_desciption
                        schema_name = feature['name'].lower().replace(" ", "_").replace("/", "_").replace(".", "_").replace("(", "_").replace(")", "_")
                        feature_id = feature['id']

                        if self.categoryfeatures.get(category_id) != None and self.categoryfeatures.get(category_id) != {}:
                            if self.categoryfeatures.get(category_id).get(feature_id) != None and self.categoryfeatures.get(category_id).get(feature_id) != {}:
                                json_schema_property = self.categoryfeatures.get(category_id).get(feature_id)
                                json_schema = {
                                    "$schema": "http://json-schema.org/draft-04/schema#",
                                    "type": "object",
                                    "description": description,
                                    "properties": 
                                        json_schema_property  
                                }
                                gcs_json_path = "gs://" + os.environ.get("GOOGLE_BUCKET_NAME")+ "/"+ category_name + "-" + schema_name +".json"
                                with gcs_file_system.open(gcs_json_path, 'w') as f:
                                    json.dump(json_schema, f , indent = 4)
                                    makeCount += 1
                                    bar.update(makeCount)
                            
        return { "Success": f"You have generated {makeCount} mixin json file successfully!"}

class IceCatLanguageMapping(IceCat):
    """
    Create a dict of Language IDs to Language names
    Refer to IceCat class for arguments
    """
    baseurl = 'https://data.Icecat.biz/export/freexml/refs/'
    FILENAME = 'LanguageList.xml.gz'
    TYPE = 'Language List'
    
    def _parse(self, xml_file, lang_id):
        gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
        if xml_file.endswith('.gz'):
            with gcs_file_system.open(xml_file) as gzip_xml_file:
                with gzip.open(gzip_xml_file, 'rb', 'unicode') as f:
                    data = ET.parse(f).getroot()
                    
        else:
            with gcs_file_system.open(xml_file) as f:
                data = ET.parse(f).getroot()

        '''
        Data is an XML ElementTree Object
        '''
        self.id_map = []
        self.code = ''
        self.short_code = ''
        self.findpath = 'Name[@langid="' + lang_id + '"]'
        self.langulage_id = ''
        self.language_name = ''

        for elem in data.iter('Language'):
            self.langulage_id = elem.attrib['ID']
            self.code = elem.attrib['Code']
            self.short_code = elem.attrib['ShortCode']
            self.id_map.append({ 'ID': self.langulage_id, 'Code': self.code, "short_code": self.short_code })

        del gcs_file_system
        gc.collect()

class IceCatSupplierList(IceCat):
    baseurl = 'https://data.Icecat.biz/export/freexml/refs/'
    FILENAME = 'SuppliersList.xml.gz'
    TYPE = 'SuppliersList'
    
    def _parse(self, xml_file, lang_id):
        gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
        if xml_file.endswith('.gz'):	      
            with gcs_file_system.open(xml_file) as gzip_xml_file:	
                with gzip.open(gzip_xml_file, 'rb', 'unicode') as f:
                    data = ET.parse(f).getroot()	
        else:	      
            with gcs_file_system.open(xml_file) as f:
                data = ET.parse(f).getroot()

        '''
        Data is an XML ElementTree Object
        '''
        self.id_map = []
        self.name = ''
        self.findpath = 'Name[@langid="' + lang_id + '"]'
        self.pic = ''
        self.description = ''
        self.id = ''

        print(" - parsing SuppliersList.xml.gz ")
        for elem in data.iter('Supplier'):
            self.id = elem.attrib['ID']
            self.name = elem.attrib['Name']
            self.pic = elem.attrib['LogoOriginal']
            self.id_map.append({ 'ID': self.id, 'name': self.name, "pic": self.pic })
            elem.clear()
        
        data.clear()

class IceCatDatabase(IceCat):
    """
    Syncing icecat catalogs to bigquery
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

    def parse_xml(self, file_name):
        events = ("start", "end")
        context = ET.iterparse(file_name, events=events)
        return self.pt(context)

    def pt(self, context, cur_elem=None):
        items = defaultdict(list)
        if cur_elem:
            if cur_elem.tag == "file":
                for k, v in cur_elem.attrib.items():
                    new_key = self._namespaces[k] if k in self._namespaces else k
                    items.update({new_key : v})
                cms = cur_elem.find("Country_Markets")
                cms_list = []
                if cms: 
                    for cm in cms.iter("Country_Market"):
                        try:
                            cms_list.append(cm.attrib['Value'])
                        except:
                            print("    not found Value in this elemeent")
                items.update({"country_markets": cms_list})
                self.key_count += 1
                self.bar.update(self.key_count)
            cur_elem.clear()
           
        text = ""

        for action, elem in context:
            if elem.tag == 'EAN_UPC' or elem.tag == 'EAN_UPCS' or elem.tag == 'Country_Market' or elem.tag == 'Country_Markets':
                elem.clear()
            else:
                if action == "start":
                    items[elem.tag].append(self.pt(context, elem))
                elif action == "end":   
                    text = elem.text.strip() if elem.text else ""
                    elem.clear()
                    break

                if len(items) == 0:
                    return text
        del context

        return { k.lower() : v[0] if len(v) == 1 and k != "country_markets" else v for k, v in items.items() }

    def divide_list_by_chunksize(self , list, chunk_size):
        for index in range(0, len(list) , chunk_size):
            yield list[index:index + chunk_size]

    def syncCatalogIndexDatabase(self):
        baseurl = 'https://data.icecat.biz/export/freexml/EN/'
        fileName = 'files.index.xml'
        file_type = 'Catalog Index'
        auth = (os.environ.get("ICECAT_USERNAME"), os.environ.get("ICECAT_PASSWORD"))
        print(" - Downloading {} from {}".format(file_type, baseurl + fileName))
 
        # download the file into local
        download_file_name = uuid.uuid4().hex +".xml"

        res = requests.get(baseurl + fileName, auth = auth, stream=True)
        with open(download_file_name, 'wb') as f:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
            f.close()
        

        if 200 <= res.status_code < 299:
            print(" - file has been downloaded successfully")
            with progressbar.ProgressBar(max_value=progressbar.UnknownLength) as self.bar:	        
                self.catalogs = self.parse_xml(download_file_name)
                self.catalogs = self.catalogs['icecat-interface']['files.index']['file']
                
            print(" - Parsed {} products from IceCat catalog".format(str(len(self.catalogs))))   
            chunked_catalogs = self.divide_list_by_chunksize(self.catalogs, 10000)

            client = bigquery.Client()
            schema = [
                bigquery.SchemaField("path" ,           "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("limited" ,        "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("highpic" ,        "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("highpicsize" ,    "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("highpicwidth" ,   "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("highpicheight" ,  "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("product_id" ,     "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("updated" ,        "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("quality" ,        "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("prod_id" ,        "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("supplier_id" ,    "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("catid" ,          "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("on_market" ,      "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("model_name" ,     "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("product_view" ,   "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("date_added" ,     "STRING" ,     mode = "REQUIRED") , 
                bigquery.SchemaField("country_markets" ,"STRING" ,     mode = "REQUIRED") , 
            ]
            
            for each_chunk_catalogs in chunked_catalogs:
            
                temp_table_name = 'temp_catalog'+ uuid.uuid4().hex
                table_id = 'icecat-demo.icecat_data_dataset.' + temp_table_name
                table = bigquery.Table(table_id, schema= schema)
                table = client.create_table(table)

                print(
                    "  -- Created temp catalog table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
                )

                if table :
                    print("  -- making insert rows from catalog list")
                    rows_to_insert = []
                    count = 0
                    with progressbar.ProgressBar(max_value= len(each_chunk_catalogs)) as bar:
                        for catalog in each_chunk_catalogs:
                            if 'path' in catalog:
                                rows_to_insert.append({
                                        "path" :   catalog['path']  if 'path' in catalog else "",
                                        "limited" :catalog['limited'] if 'limited' in catalog else "",
                                        "highpic" :catalog['highpic'] if 'highpic' in catalog else "",
                                        "highpicsize" :catalog['highpicsize'] if 'highpicsize' in catalog else "",
                                        "highpicwidth" :catalog['highpicwidth'] if 'highpicwidth' in catalog else "",
                                        "highpicheight" :catalog['highpicheight'] if 'highpicheight' in catalog else "",
                                        "product_id" :catalog['product_id'] if 'product_id' in catalog else "",
                                        "updated" :catalog['updated'] if 'updated' in catalog else "",
                                        "quality" :catalog['quality'] if 'quality' in catalog else "",
                                        "prod_id" :catalog['prod_id'] if 'prod_id' in catalog else "",
                                        "supplier_id" :catalog['supplier_id'] if 'supplier_id' in catalog else "",
                                        "catid" : catalog['catid'] if 'catid' in catalog else "" ,
                                        "on_market" :catalog['on_market'] if 'on_market' in catalog else "",
                                        "model_name" :catalog['model_name'] if 'model_name' in catalog else "",
                                        "product_view" :catalog['product_view'] if 'product_view' in catalog else "",
                                        "date_added" :catalog['date_added'] if 'date_added' in catalog else "",
                                        "country_markets" :','.join(catalog['country_markets'] if 'country_markets' in catalog else "")
                                    }
                                )
                            count += 1
                            bar.update(count)
                    
                    errors = client.insert_rows_json(table_id , rows_to_insert)
                    if errors == []:   # successfully inserted
                        query = f"MERGE icecat_data_dataset.catalog T \
                            USING icecat_data_dataset.{temp_table_name} S \
                            ON T.product_id = S.product_id and T.path = S.path \
                            WHEN MATCHED THEN \
                            UPDATE SET path = S.path , limited = S.limited , highpic = S.highpic , highpicsize = S.highpicsize , highpicwidth = S.highpicwidth , \
                                highpicheight = S.highpicheight , product_id = S.product_id , updated = S.updated , quality = S.quality , prod_id = S.prod_id,  \
                                supplier_id = S.supplier_id , catid = S.catid , on_market = S.on_market , model_name = S.model_name , product_view = S.product_view ,  \
                                date_added = S.date_added , country_markets = S.country_markets    \
                            WHEN NOT MATCHED THEN \
                            INSERT (path, limited, highpic, highpicsize, highpicwidth, highpicheight, product_id, updated, quality, prod_id, supplier_id , catid, on_market, model_name, product_view, date_added, country_markets)  \
                                VALUES (path, limited, highpic, highpicsize, highpicwidth, highpicheight, product_id, updated, quality, prod_id, supplier_id , catid, on_market, model_name, product_view, date_added, country_markets)"
                        query_job = client.query(query)    
                        result = query_job.result()
                        print("Updated catalog table successfully")
      
                client.delete_table(table_id, not_found_ok = True)
            
            os.remove(download_file_name)
            return {'success': "Updated catalog table successfully."}
        else:
            return {"error" : "Did not receive good status code: {} while downloading the daily.index.xml".format(res.status_code)}
            
        # parsing file
        os.remove(download_file_name)
        
    def syncSearchIndexDatabase(self):
        self.suppliers = IceCatSupplierList()
        self.categories = IceCatCategoryMapping(lang_id="1")
        self.languages = IceCatLanguageMapping()
        client = bigquery.Client()

        count = 0
        
        print("importing Language into dataset")

        # creating tempo language table

        schema = [
            bigquery.SchemaField("language_id" , "STRING" , mode = "REQUIRED") , 
            bigquery.SchemaField("code" , "STRING" , mode = "REQUIRED") , 
            bigquery.SchemaField("short_code" , "STRING" , mode = "REQUIRED") , 
        ]
        temp_table_name = 'temp_language'+ uuid.uuid4().hex
        table_id = 'icecat-demo.icecat_data_dataset.' + temp_table_name
        table = bigquery.Table(table_id, schema= schema)
        table = client.create_table(table)

        print(
            " -- Created temp language table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

        # mersing temp table to main table 
        
        if table :
            
            rows_to_insert = []
            with progressbar.ProgressBar(max_value= len(self.languages.id_map)) as bar:
                for language in self.languages.id_map:
                    id = language['ID']
                    code = language['Code']
                    short_code = language['short_code']
                    rows_to_insert.append(
                        {'language_id' : id, "code": code , "short_code" : short_code}
                    )
            
            errors = client.insert_rows_json(table_id , rows_to_insert)
            if errors == []:   # successfully inserted
                query = f"MERGE icecat_data_dataset.language T \
                    USING icecat_data_dataset.{temp_table_name} S \
                    ON T.language_id = S.language_id \
                    WHEN MATCHED THEN \
                    UPDATE SET code = S.code , short_code = S.short_code \
                    WHEN NOT MATCHED THEN \
                    INSERT (language_id, code, short_code ) VALUES(language_id, code, short_code)"
                query_job = client.query(query)    
                result = query_job.result()
                print("Updated language table successfully")
            
            client.delete_table(table_id, not_found_ok = True)

        
        count = 0
        print("importing supplier list into dataset")

        # creating tempo supplier table

        schema = [
            bigquery.SchemaField("supplier_id" , "STRING" , mode = "REQUIRED") , 
            bigquery.SchemaField("supplier_name" , "STRING" , mode = "REQUIRED") , 
        ]
        temp_table_name = 'temp_supplier'+ uuid.uuid4().hex
        table_id = 'icecat-demo.icecat_data_dataset.' + temp_table_name
        table = bigquery.Table(table_id, schema= schema)
        table = client.create_table(table)

        print(
            " -- Created temp supplier table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

        # mersing temp table to main table 
        if table:
            rows_to_insert = []
            with progressbar.ProgressBar(max_value= len(self.suppliers.id_map)) as bar:
                for supplier in self.suppliers.id_map:
                    id = supplier['ID']
                    name = supplier['name']
                    rows_to_insert.append(
                        {'supplier_id' : id, "supplier_name": name}
                    )
            
            errors = client.insert_rows_json(table_id , rows_to_insert)
            if errors == []:   # successfully inserted
                query = f"MERGE icecat_data_dataset.supplier T \
                    USING icecat_data_dataset.{temp_table_name} S \
                    ON T.supplier_id = S.supplier_id \
                    WHEN MATCHED THEN \
                    UPDATE SET supplier_name = S.supplier_name \
                    WHEN NOT MATCHED THEN \
                    INSERT (supplier_id, supplier_name) VALUES(supplier_id, supplier_name)"
                query_job = client.query(query)    
                result = query_job.result()
                print("Updated supplier table successfully")
            
            client.delete_table(table_id, not_found_ok = True)

            count = 0
            print("importing category list into dataset")

        # creating tempo supplier table

        schema = [
            bigquery.SchemaField("category_id" , "STRING" , mode = "REQUIRED") , 
            bigquery.SchemaField("category_name" , "STRING" , mode = "REQUIRED") , 
            bigquery.SchemaField("parent_cat_id" , "STRING" , mode = "REQUIRED") , 
            bigquery.SchemaField("description" , "STRING" , mode = "REQUIRED") , 
        ]
        temp_table_name = 'temp_category' + uuid.uuid4().hex
        table_id = 'icecat-demo.icecat_data_dataset.' + temp_table_name
        table = bigquery.Table(table_id, schema= schema)
        table = client.create_table(table)

        print(
            " -- Created temp category table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

        # merging temp table to main table 
        if table:
            rows_to_insert = []
            with progressbar.ProgressBar(max_value= len(self.categories.id_map)) as bar:
                for category in self.categories.id_map:
                    id = category['ID']
                    name = category['Name']
                    parent_id = category['ParentID']
                    description = category['Description']
                    rows_to_insert.append(
                        {'category_id' : id, "category_name": name , "parent_cat_id": parent_id , 'description' : description}
                    )
            
            errors = client.insert_rows_json(table_id , rows_to_insert)
            if errors == []:   # successfully inserted
                query = f"MERGE icecat_data_dataset.category T \
                    USING icecat_data_dataset.{temp_table_name} S \
                    ON T.category_id = S.category_id \
                    WHEN MATCHED THEN \
                    UPDATE SET category_name = S.category_name , parent_cat_id = S.parent_cat_id , description = S.description \
                    WHEN NOT MATCHED THEN \
                    INSERT (category_id, category_name, parent_cat_id , description) VALUES(category_id, category_name, parent_cat_id, description)"
                query_job = client.query(query)    
                result = query_job.result()
                print("Updated supplier table successfully")
            
            client.delete_table(table_id, not_found_ok = True)

       
        return {"success" : "updated dataset in bigquery" }

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