import chunk
from math import prod
from multiprocessing.spawn import import_main_path
from traceback import print_tb
from app.icecat import bulk_downloader
from dotenv import load_dotenv
from app import main
from datetime import datetime
from collections import defaultdict
from google.cloud import storage
from google.cloud import bigquery
import xmltodict
import collections
import gzip
import json
import os
import sys
import xml.etree.cElementTree as ET
import progressbar
import requests
import logging
import polling
import concurrent.futures
import gcsfs
import os
import gc
import uuid
import time
import calendar
import random

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

class IceCatSupplierMapping(IceCat):
    """
    Create a dict of product supplier IDs to supplier names
    Refer to IceCat class for arguments
    """

    baseurl = 'https://data.icecat.biz/export/freeurls/'
    FILENAME = 'supplier_mapping.xml'
    TYPE = 'Supplier Mapping'

    def _parse(self, xml_file, lang_id):
        gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
        if xml_file:	       
            with gcs_file_system.open(xml_file) as f:	 
                data = ET.parse(f).getroot()
                
        else:
            self.log.error("Failed to retrieve suppliers")
            return False

        '''
        Data is an XML ElementTree Object
        '''
        self.id_map = {}
        self.catid = ''
        self.catname = ''
        print(" - parsing supplier_mapping.xml")
        for elem in data.iter('SupplierMapping'):
            self.mfrid = elem.attrib['supplier_id']
            self.mfrname = elem.attrib['name']
            if not self.mfrname:
                self.mfrname = "Unknown"
            self.id_map[self.mfrid] = self.mfrname
            elem.clear()
        data.clear()
      
        self.log.info("Parsed {} Manufacturers from IceCat Supplier Map".format(str(len(self.id_map.keys()))))

    def get_mfr_byId(self, mfr_id):
        """
        Return a Product Supplier or False if no match
        :param mfr_id: Supplier ID
        """

        if mfr_id in self.id_map:
            return self.id_map[mfr_id]
        # return False
        return ""

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
        # self.id_map = {}
        self.id_map = []
        self.catid = ''
        self.catname = ''
        self.findpath = 'Name[@langid="' + lang_id + '"]'
        self.parentCategoryID = ''
        self.parentName = ''
        print(" - parsing CategoriesList.xml.gz")
        for elem in data.iter('Category'):
            self.catid = elem.attrib['ID']

            for name in elem.iterfind(self.findpath):
              
                self.catname = name.attrib['Value']
                # only need one match
                break
            if not self.catname:
                self.catname = "Unknown"
            
            for parent in elem.iter("ParentCategory"):
                self.parentCategoryID = parent.attrib['ID']
                break
            
            
            # self.id_map[self.catid] = self.catname
            self.id_map.append({ 'ID': self.catid, 'Name': self.catname, "ParentID": self.parentCategoryID, "ParentName": self.parentName })
            elem.clear()
        data.clear()
  
class IceCatLanguageMapping(IceCat):
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
        # self.id_map = {}
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
            
            # for name in elem.iterfind(self.findpath):
                # self.language_name = name.attrib['Value']
                # only need one match
                # break
            # if not self.catname:
            #     self.catname = "Unknown"
            
            
            # self.id_map[self.catid] = self.catname
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
        # self.id_map = {}
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

class IceCatProductDetails(IceCat):
    """
    Extract product detail data. It's unusual to call this class directly. Used by add_product_details..()
    :param keys: a list of product detail keys. Refer to Basic Usage Example
    :param cleanup_data_files: whether to delete xml files after parsing.
    :param filename: xml file with the product details
    Refer to IceCat class for additional arguments
    """

    def __init__(self, keys, cleanup_data_files=True, xml_file=None, *args, **kwargs):
        self.keys = keys
        self.FILENAME = xml_file
        self.cleanup_data_files = cleanup_data_files
        
        logging.basicConfig(filename='IceCat_Catalogs.log', encoding='utf-8', level=logging.INFO)
        self.log = logging.getLogger()
        self._parse(xml_file)

    baseurl = 'https://data.icecat.biz/'
    TYPE = 'Product details'

    o = {}

    def parseMixin(self, product_feature):
        category_name = self.product_dict.get('Category').get("Name").get("Value").lower().replace(" ", "_").replace("/", "_")
        category_featurename = self.category_feature_matching_list[product_feature.get('CategoryFeatureGroup_ID')]
            
        gcs_json_path = "gs://" + os.environ.get("GOOGLE_BUCKET_NAME")+ "/"+ category_name + "-" + category_featurename +".json"
    
        json_url = "https://storage.googleapis.com/" + os.environ.get("GOOGLE_BUCKET_NAME")+ "/" + category_name + "-"  + category_featurename + ".json"
        
        try:
            if g_mixin.get(category_name + "-" + category_featurename) == None:
                gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
                with  gcs_file_system.open(gcs_json_path) as f:
                    mixin_schema = json.load(f)
                    g_mixin.update({category_name + "-" + category_featurename : mixin_schema})
            else:
                
                mixin_schema = g_mixin.get(category_name + "-" + category_featurename)

            if self.metadata_mixin.get(category_featurename) == None:
                self.metadata_mixin.update({category_featurename : json_url })
                self.mixin.update({category_featurename : {}})

            Feature_elem = product_feature.get('Feature')
            self.feature_id_list.append(Feature_elem.get('ID'))
            nameElem = Feature_elem.get("Name")
            
            featurename = nameElem.get('Value').lower().replace(" ", "_").replace("/", "_")
            
            if self.mixin[category_featurename].get(featurename) == None:
                    feature_schema_detail = mixin_schema['properties'][featurename]
                    
                    #checking the schema is range or text
                    if feature_schema_detail.get('$ref') != None:

                        ref = feature_schema_detail['$ref']
                        localValue = product_feature.get('LocalValue').get('Value')
                        # sign =product_feature.find('LocalValue').find('Measure').find('Signs').find('Sign')
                        # uom = sign.text
                        sign = product_feature.get('Feature').get("Measure").get('Signs')
                        if sign == None:
                            uom = ""
                        else:
                            uom = sign.get('Sign').get('#text')
                        if uom == "":
                            presentationValue= product_feature.get('Presentation_Value')
                            uom = presentationValue.split(localValue)[1].strip()
                        
                        if "atomic_uom" in ref:
                            
                            self.mixin[category_featurename].update({featurename : {"value" : float(localValue), "uom" : uom}})
                        else:                    # range_uom
                            
                            splitString = localValue.split('-')
                            if len(splitString) == 2:
                                firstValue = float(splitString[0].strip())
                                toValue = float(splitString[0].strip())
                            else:
                                firstValue = float(splitString[0].strip())
                                toValue = 1
                                self.mixin[category_featurename].update({featurename : {"firstValue" : firstValue, "toValue": toValue , "uom" : uom}})
                    else:

                        if feature_schema_detail['type'][0] == "string":
                            value = product_feature.get('Presentation_Value')
                            self.mixin[category_featurename].update({featurename : value})

                        elif feature_schema_detail['type'][0] == "number":
                            value = product_feature.get('Presentation_Value')
                            self.mixin[category_featurename].update({featurename : int(value)})

                        else: # boolean
                            value = product_feature.get('Presentation_Value')
                            value = True if value == "Y" else False
                            self.mixin[category_featurename].update({featurename : value})

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            # print(message)
            self.log.error(message)
            

    def _parse(self, xml_file, lang_id = "1"):
       
        gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
        
        with  gcs_file_system.open(xml_file) as f:
            self.xml_file = xml_file
            self.product_dict = xmltodict.parse(f, attr_prefix = '' )
            self.product_dict = self.product_dict['ICECAT-interface']['Product']

        self.category_feature_matching_list = {}
        self.metadata_mixin = {}
        self.mixin = {}
        self.feature_id_list = []
        
        # get general features
    
        if self.product_dict.get('EndOfLifeDate') != None:
            self.o.update({'end_of_life_date': self.product_dict.get('EndOfLifeDate').get('Date').get('Value')})
       
        if self.product_dict.get('ReasonsToBuy')!= None:
            if type(self.product_dict.get('ReasonsToBuy').get('ReasonToBuy')) == list:
                reason = self.product_dict.get('ReasonsToBuy').get('ReasonToBuy')
                self.o.update({'reasons_tobuy': reason[0].get('Value')})
            else :
                self.o.update({'reasons_tobuy': self.product_dict.get('ReasonsToBuy').get('ReasonToBuy').get('Value')})
       
        if self.product_dict.get('BulletPoints')!= None:
            if type(self.product_dict.get('BulletPoints').get('BulletPoint')) == list:
                bullet = self.product_dict.get('BulletPoints').get('BulletPoint')
                self.o.update({'bullet_points': bullet[0].get('Value')})
            else:
                self.o.update({'bullet_points': self.product_dict.get('BulletPoints').get('BulletPoint').get('Value')})
      
        if self.product_dict.get("ReleaseDate") != None:
            if type(self.product_dict.get("ReleaseDate")) == list:
                date = self.product_dict.get("ReleaseDate")
                self.o.update({'releasedate' : date[0]})
            else:
                self.o.update({'releasedate' : self.product_dict.get("ReleaseDate")})
        ean_list = []
       
        if self.product_dict.get("EANCode") != None :

            if type(self.product_dict.get("EANCode")) == list:
                for ean in self.product_dict.get("EANCode"):
                    ean_list.append(ean.get('EAN'))
            else:
                ean_list.append(self.product_dict.get("EANCode").get('EAN'))
        self.o.update({'eans': ean_list})
        
        

        if self.product_dict.get("SummaryDescription").get("LongSummaryDescription").get('#text') != None:
            self.o.update({'longsummarydesciption' : self.product_dict.get("SummaryDescription").get("LongSummaryDescription").get('#text')})
       
        if self.product_dict.get('ProductDescription') != None:
            self.o.update({"longdesc" : self.product_dict.get('ProductDescription').get("LongDesc"),
                "manualpdfurl" : self.product_dict.get('ProductDescription').get("ManulPDFURL") if self.product_dict.get('ProductDescription').get("ManulPDFURL") != None else "",
                "pdfurl" : self.product_dict.get('ProductDescription').get("PDFURL"),
                "warranty_info" : self.product_dict.get('ProductDescription').get("WarrantyInfo")})
  
        if self.product_dict.get("GeneratedIntTitle") != None:
            self.o.update({'generatedinttitle' : self.product_dict.get("GeneratedIntTitle")})
        if self.product_dict.get("ProductGallery") != None:
            medias = []
            if type(self.product_dict.get("ProductGallery").get("ProductPicture")) == list:
                for picture in self.product_dict.get("ProductGallery").get("ProductPicture"):
                    image_attr = {}
                    image_attr['no'] = picture.get('No')
                    image_attr['original'] = picture.get('Original')
                    medias.append(image_attr)
            else:
                image_attr = {}
                image_attr['no'] = self.product_dict.get("ProductGallery").get("ProductPicture").get('No')
                image_attr['original'] = self.product_dict.get("ProductGallery").get("ProductPicture").get('Original')
                medias.append(image_attr)
            self.o.update({ "medias" : medias})


        for category_feature_group in self.product_dict.get('CategoryFeatureGroup'):
            self.category_feature_matching_list.update({category_feature_group['ID'] : category_feature_group.get('FeatureGroup').get('Name').get('Value').lower().replace(" ", "_").replace("/", "_")})
        
       
        if self.product_dict.get("ProductFeature") != None:
            if type(self.product_dict.get('ProductFeature')) == list:
                for product_feature in self.product_dict.get("ProductFeature"):
                    self.parseMixin(product_feature)
            else:
                self.parseMixin(self.product_dict.get("ProductFeature")   )    
 
        self.o.update({'metadata': self.metadata_mixin})
        self.o.update({'mixins': self.mixin})
        self.o.update({'feature_id_list' : self.feature_id_list})

        self.log.debug("Parsed product details for {}".format(xml_file))
        
        if self.cleanup_data_files:
            try:
                os.remove(xml_file)
            except:
                self.log.warning("Unable to delete temp file {}".format(xml_file))
        del gcs_file_system 
        

    def get_data(self):
        return self.o

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
        # self.id_map = {}
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
    FILENAME = 'CategoryFeaturesList.xml'
    # FILENAME = 'CategoryFeaturesList_re.xml'
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
                          
                            feature_name =  elem.attrib["Value"].lower().replace(" ", "_").replace("/", "_")
                            
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
            # self.o = self.parse_xml(xml_file)
            gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
            if xml_file.endswith('.gz'):
                with gcs_file_system.open(xml_file) as gzip_file:
                    with gzip.open(gzip_file, 'rb', 'unicode') as f:
                        self.parse_xml(f, lang_id)
            else:
                with gcs_file_system.open(xml_file) as f:
                    self.parse_xml(f, lang_id)

class IceCatFeatureLogosList(IceCat):
    baseurl = 'https://data.Icecat.biz/export/freexml/refs/'
    FILENAME = 'FeatureLogosList.xml.gz'
    TYPE = 'FeatureLogosList'
    
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
        # self.id_map = {}
        self.id_map = []
        self.name = ''
        self.description_findpath = 'Description[@langid="' + lang_id + '"]'
        self.pic = ''
        self.description = ''
        
        self.feature_id = ''

        print(" - parsing FeatureLogosList.xml.gz ")
        for elem in data.iter('FeatureLogo'):
            feature_category_list = []
            id = elem.get('ID')
            self.feature_id = elem.get('Feature_ID')
            description_elem = elem.find('Descriptions')
            
            if description_elem:
                for description in description_elem.iterfind(self.description_findpath):
                   
                    self.description = description.text
                  
                    break

            # self.name = elem.attrib['Name']
            self.pic = elem.attrib.get('LogoPic')

            for featurelogo_category in elem.iter('FeatureLogoCategory'):
                feature_category_list.append(featurelogo_category.attrib['catid'])

            logo_features = elem.find("FeatureLogoFeatures")
            if logo_features:
                for logo_feature in logo_features.iterfind("FeatureLogoFeature[@ID='"+ self.feature_id +"']"):
                    logo_values = logo_feature.find('FeatureLogoValues')
                    if logo_values:
                        self.name = logo_values.find('FeatureLogoValue').text
                        break
            
            self.id_map.append({ 'id': id, 'feature_id': self.feature_id, 'name': self.name,  "image": self.pic  , "description" : self.description, "cat_id_list" : feature_category_list})
            elem.clear()
        
        data.clear()             
class IceCatCatalog(IceCat):
    """
    Parse Ice Cat catalog index file.
    Special handling of the input data is based on IceCAT OCI Revision date: April 24, 2015, Version 2.46:
         - resolve supplier ID, and Category ID to their english names
         - unroll ean_upcs nested structure to flat value, or list
         - convert attribute names according to the table (to lower case)
         - drop keys in the exclude_list, default ['Country_Markets']
         - discard parent layers above 'file' key
    :param suppliers: IceCatSupplierMapping object. If None specified a mapping is instantiated inside the class.
    :param categories: IceCatCategoryMapping object. If None specified a mapping is instantiated inside the class.
    :param exclude_keys: a list of keys to omit from the product index.
    :param fullcatalog: Set to True to download full product catalog. 64-bit python is required for this option
                        because of >2GB memory footprint. You will need ~4.5 GB of virtual memory to process a 500k
                        item catalog.
    Refer to IceCat class for additional arguments
    """

    def __init__(self, suppliers=None, categories=None, languages=None, exclude_keys=None, fullcatalog=False, lang_id=None, supplierIds = None, categoryIds = None, tenant = None, hook_url = None, env = None , secret = None, client_id = None, job_id = None, max_images = "1", max_import_products = 1000, *args, **kwargs):
        
        if exclude_keys is None:
            exclude_keys = ['Country_Markets']
        self.catalogs = []
        self.payload_categoryIds = categoryIds
        self.extended_categoryIds = []
        self.extended_categoryIds.extend(self.payload_categoryIds)
        self.payload_supplierIds = supplierIds
        self.suppliers = suppliers
        self.categories = categories
        self.languages = languages
        self.lang_id = lang_id
        self.brands = None
        self.features = None
        self.featurelogo = None
        self.job_mode = "sync"
        if max_images == "":   # no limit
            self.max_images = 0
        else:
            self.max_images = int(max_images)

        self.max_products = max_import_products
         

        self.tenant = tenant
        if env == "stage":
            global ENDPPOINT_URL
            ENDPPOINT_URL = os.environ.get("STAGE_API_URL")
       
        
        # language_code = []
 
        authurl = ENDPPOINT_URL + "/oauth/token"
        
        authbody = {
            "client_id": client_id,
            "client_secret": secret,
            "grant_type": "client_credentials",
            "scope": "product.product_create product.product_publish category.category_create category.category_publish category.category_update saasag.brand_manage product.product_update product.product_publish product.product_delete product.product_delete_all import.import_view import.import_manage saasag.label_manage",
        }

        authRequest = requests.post(authurl, data = authbody)
        
        try:
            authResponse = authRequest.json()    
        except:
            return { "Error": "You have wrong credential." }
       
        self.access_token = authResponse['access_token']
      
        if hook_url != "" and self.access_token:
            self.webhook_url = hook_url
            self.job_mode = 'async'

            self.job_id = job_id
            
            # saving job to bigquery table
            client = bigquery.Client()
            query = f"insert into `icecat-demo.icecat_data_dataset.jobs` (id, tenant_name , status , created_time ) values ('{self.job_id}', '{self.tenant}' , 'IN_PROGRESS', '{time.strftime('%Y-%m-%d %H:%M:%S')}')"
            query_job = client.query(query)
            results = query_job.result() 

            if  results.total_rows:  
                for row in results:
                    print(row)

            print(" - insert job id into bigquery as in_progress")

            # making payload for inital confirm
            payload = {
                'type'  : 'INITIAL_CONFIRM' ,
                "job_id"  : self.job_id ,
                "tenant"  : tenant,
                "suppliers" : supplierIds ,
                "categories" : categoryIds
            }
            self.create_webhook( payload = payload)
        

        self.cursor = None
        self.cnxn = None
        self.categoryfeatures = None

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
        

        self.exclude_keys = exclude_keys
        if fullcatalog:
            self.FILENAME = 'files.index.xml'
        else:
            self.FILENAME = 'daily.index.xml'
        super(IceCatCatalog, self).__init__(lang_id=str(lang_id[0]), *args, **kwargs)

    baseurl = 'https://data.icecat.biz/export/freexml/EN/'
    TYPE = 'Catalog Index'

    InjectedCategories = []

    catalogs = dict()

    def _postprocessor(self, path, key, value):
        
        if key == "file":
            '''
            Look up supplier id and category
            '''
            
            try:
                value.update({'supplier': self.suppliers.get_mfr_byId(value['supplier_id'])})
            except:
                self.log.info("Unable to find supplier for supplier_id: {}".format(value['supplier_id']))

            try:
                value.update({'category': self.categories.get_cat_byId(value['catid']).title()})
            except:
                self.log.info("Unable to find category for catid: {}".format(value['catid']))

            # unroll ean_upcs. sometimes this is a list of single value dicts, other times it's a string.
    
            if 'ean_upcs' in value:
                try:
                    value['ean_upcs'] = [value['ean_upcs']['ean_upc']['Value']]
                except TypeError:
                    upcs = []
                    for item in value['ean_upcs']['ean_upc']:
                        upcs.append(list(item.values())[0])
                    value['ean_upcs'] = upcs
                except:
                    # something bad happened with upcs
                    self.log.info(
                        "Unable to unroll ean_upcs {} for product_id: {}".format(sys.exc_info(), value['product_id']))

            self.key_count += 1
            self.bar.update(self.key_count)



        # skip keys we are not interested in.
        elif key in self.exclude_keys:
            return None

        return key.lower(), value
    # used to flatten a nested structure if needed
    def _flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
        
    def parse_xml(self, file_name):
        events = ("start", "end")
        context = ET.iterparse(file_name, events=events)
        
        return self.pt(context)

    def pt(self, context, cur_elem=None):

        items = defaultdict(list)

        if cur_elem:
            if cur_elem.tag == "file":
                if self.payload_categoryIds and self.payload_supplierIds:        
                    if cur_elem.attrib['Catid'] in self.extended_categoryIds and cur_elem.attrib['Supplier_id'] in self.payload_supplierIds:
                        for k, v in cur_elem.attrib.items():
                            new_key = self._namespaces[k] if k in self._namespaces else k
                            items.update({new_key : v})

                        try:
                            items.update({'supplier': self.suppliers.get_mfr_byId(items['supplier_id'])})
                        except:
                            self.log.info("Unable to find supplier for supplier_id: {}".format(items['supplier_id']))

                        cms = cur_elem.find("Country_Markets")
                        cms_list = []
                        if cms:
                            
                            for cm in cms.iter("Country_Market"):
                                
                                try:
                                    cms_list.append(cm.attrib['Value'])
                                except:
                                    print("    not found Value in this elemeent")
                        
                        items.update({"country_markets": cms_list})


                else:
                    if cur_elem.attrib['Catid'] in self.extended_categoryIds or cur_elem.attrib['Supplier_id'] in self.payload_supplierIds:
                        
                        for k, v in cur_elem.attrib.items():
                            new_key = self._namespaces[k] if k in self._namespaces else k
                            items.update({new_key : v})
                        try:
                            items.update({'supplier': self.suppliers.get_mfr_byId(items['supplier_id'])})
                        except:
                            self.log.info("Unable to find supplier for supplier_id: {}".format(items['supplier_id']))
                
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
            # else:
            #     items.update(cur_elem.attrib)

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

    def get_child_catIdList(self, catList, parent_id):
        childIdList = []
        for item in catList:
            if item.get('ParentID') == parent_id:
                _ID = item.get('ID')
                if _ID != parent_id:
                    self.log.info("  Adding sub category id: " + _ID  + " its parent id : " + parent_id )
                    childIdList.append(_ID)
                    childIdList.extend(self.get_child_catIdList(catList, _ID))


        return childIdList

    def extend_categories_withChild(self):
        for payload_parent_id in self.payload_categoryIds:
            childList = self.get_child_catIdList(self.categories.id_map , payload_parent_id)
            self.extended_categoryIds.extend(childList)

        self.log.info(" ---------  extended categoryIds ---------------- ")
        self.log.info(len(self.extended_categoryIds))
        self.log.info(self.extended_categoryIds)

    def _parse(self, xml_file, lang_id):
        self.xml_file = xml_file
        self.key_count = 0

        client = bigquery.Client()

        ##############################################################################
        
        # Lets ignore for now , it is not used for now.

        # if not self.suppliers:
        #     self.suppliers = IceCatSupplierMapping(log=self.log, auth=self.auth, data_dir=self.data_dir)

        ##############################################################################
        # if not self.categories:
        #     self.categories = IceCatCategoryMapping(log=self.log, data_dir=self.data_dir, auth=self.auth, lang_id=lang_id)

        # self.extend_categories_withChild()
        ###############################################################################
        if not self.categories:
            self.categories = []
            print(" - getting categories from bigquery")

            query = "select * from `icecat-demo.icecat_data_dataset.category`"
            query_job = client.query(query)

            results = query_job.result() 

            if  results.total_rows:  
                for row in results:
                    self.categories.append({ 'ID': row['category_id'], 'Name': row['category_name'], "ParentID": row['parent_cat_id'], "ParentName": "" })


        for category in self.payload_categoryIds:
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
        
        ###################################################################################

        if not self.languages:
            self.languages = IceCatLanguageMapping(log=self.log, data_dir=self.data_dir, auth=self.auth, lang_id=lang_id)

        if not self.featurelogo:
            self.featurelogo = IceCatFeatureLogosList(log = self.log, data_dir=self.data_dir , auth = self.auth, lang_id=lang_id)

        # self.log.warning(self.featurelogo.id_map)

        if not self.brands: 
            self.brands = IceCatSupplierList(log=self.log, data_dir=self.data_dir, auth=self.auth, lang_id=lang_id)

        ##############################################################################################################
        # get catalogs from gcs bucket
 
        # gcs_file_system = gcsfs.GCSFileSystem(project="icecat-demo")
        
        # storage_client = storage.Client()
        # bucket = storage_client.bucket("icecat_productxml")
        # blob = bucket.blob("files.index.xml")
        # download_file_name = uuid.uuid4().hex +".xml"
        # blob.download_to_filename(download_file_name)

        # if os.path.isfile(download_file_name):

        #     with progressbar.ProgressBar(max_value=progressbar.UnknownLength) as self.bar:	        
        #         self.o = self.parse_xml(download_file_name)
        #         self.o = self.o['icecat-interface']['files.index']['file']

        #         self.catalogs = self.o
        #         self.log.info("Parsed {} products from IceCat catalog".format(str(len(self.o))))   
        #         self.log.info(" ----- catalogs parsed from IceCat catalog")   
        #         # self.log.warning(self.catalogs)
        #         self.log.warning(len(self.catalogs))
        
        # os.remove(download_file_name)

        ############################################################################################################
        # get catalogs from bigquery
       
        print(" - getting catalgos from bigquery")
        
        if self.extended_categoryIds:
            str_category = ",".join(f"'{w}'" for w in self.extended_categoryIds)
        else:
            str_category = "''"

        if self.payload_supplierIds:
            str_supplier = ",".join(f"'{w}'" for w in self.payload_supplierIds)
        else:
            str_supplier = "''"
       

        if self.payload_categoryIds and self.payload_supplierIds:  
            query = f"SELECT * FROM icecat_data_dataset.catalog WHERE catid in ({str_category}) and supplier_id in ({str_supplier}) order by cast (product_view as int) desc limit {self.max_products}"
        else:
            query = f"SELECT * FROM icecat_data_dataset.catalog WHERE catid in ({str_category}) or supplier_id in ({str_supplier}) order by cast (product_view as int) desc limit {self.max_products}"
        

        query_job = client.query(query)

        results = query_job.result() 

        if  results.total_rows:  
            
            for row in results:
                self.catalogs.append({
                    'path' : row['path'],
                    'limited' : row['limited'],
                    'highpic' : row['highpic'],
                    'highpicsize' : row['highpicsize'],
                    'highpicwidth' : row['highpicwidth'],
                    'highpicheight' : row['highpicheight'],
                    'product_id' : row['product_id'],
                    'updated' : row['updated'],
                    'quality' : row['quality'],
                    'prod_id' : row['prod_id'],
                    'supplier_id' : row['supplier_id'],
                    'catid' : row['catid'],
                    'on_market' : row['on_market'],
                    'model_name' : row['model_name'],
                    'product_view' : row['product_view'],
                    'date_added' : row['date_added'],
                    'country_markets' : row['country_markets'].split(","),
                })
            self.log.info("Parsed {} products from IceCat catalog".format(str(len(self.catalogs))))   
            print("Parsed {} products from IceCat catalog".format(str(len(self.catalogs))))   
            self.log.info(" ----- catalogs parsed from IceCat catalog")      
        

        return len(self.catalogs)

    def divide_list_by_chunksize(self , list, chunk_size):
        for index in range(0, len(list) , chunk_size):
            yield list[index:index + chunk_size]

    def full_import_staff(self, categoryIds, supplierIds, secret_id, client_id, tenant , env , low_stock_max, medium_stock_max, high_stock_max , generatePrices , max_products ):
        
        self.low_stock_max = int(low_stock_max)
        self.medium_stock_max = int(medium_stock_max)
        self.high_stock_max = int(high_stock_max)
        self.max_products = max_products
        self.prices = generatePrices    
        
        # sort the price by the count of key desc
        if len(self.prices):
            self.prices.sort(key=lambda k: (len(k)), reverse=True)


        # creating log file
        if not os.path.exists('_logs'):
            os.makedirs('_logs')
        dateTimeObj = datetime.now()
        ts = datetime.timestamp(dateTimeObj)
        self._log_file = '_logs/'+str(ts)+'.txt'
        f = open(self._log_file, "a")  
        
        language_code = [x for x in self.languages.id_map if x['ID'] == self.lang_id]
   
        self.headers = {"Authorization": "Bearer " + self.access_token, "Content-Type": "application/json", "Content-Language": language_code[0]['short_code'].lower(), "X-Version":os.environ.get("CATEGORY_API_VERSION") }
       
        if (not categoryIds and not supplierIds):
            return { "Error": "Please input category id or supplier id." }
        
        categoiresFromProducts = []

        print(" -  Checking categories")

        if(not categoryIds):
            for catalog in self.catalogs:
                if catalog.get('supplier_id') != None:
                    if(catalog['supplier_id'] in supplierIds):
                        categoiresFromProducts.append(catalog['catid'])
                        self.explode_categories(self.categories, catalog['catid'])
        else:
            for categoryid in self.extended_categoryIds:  
                self.explode_categories(self.categories, categoryid)
        
        self.InjectedCategories.reverse()         

        # remove duplication from the injected category list
        pureCategoriesList = []
        for i in self.InjectedCategories:
            if i not in pureCategoriesList:
                pureCategoriesList.append(i)

        if (not pureCategoriesList):
            return { "Error": "The category does not exist." }


        InjectedCategories = []

        parentCategoryIds = {}

        for inJectCategory in pureCategoriesList:

            data = {"ecn" : inJectCategory['categoryID']}
            getCategorysResponse = requests.get(url = ENDPPOINT_URL + "/category/"+tenant+"/categories", params = data , headers=self.headers)
            self.log.warning("  get category response from this tennat")
            self.log.warning(getCategorysResponse.json())

            try:
                category_info = getCategorysResponse.json()
                id = category_info[0].get('id')   # when the category is already in the tenant, it returns the id of the emporix uuid4
                InjectedCategories.append({ "categoryID": id, "Name": inJectCategory['Name'], "parentCategoryId": inJectCategory['parentCategoryId'], 'IsRequest': False, 'catId': inJectCategory['categoryID']})
                parentCategoryIds.update({inJectCategory['categoryID'] : id})

            except Exception as ex:
                InjectedCategories.append({ "categoryID": inJectCategory['categoryID'], "Name": inJectCategory['Name'], "parentCategoryId": inJectCategory['parentCategoryId'], 'IsRequest': True, 'catId': inJectCategory['categoryID'] })
        

        print(" -  Finishing to get injected categories")
        print(InjectedCategories)
        self.log.warning("injected categories")
        self.log.warning(InjectedCategories)

        self.imported_category_list = []
        parentCategoryIndex = 0

        # assign categories to Emporix tenant
        print(" -  assign categories to Emporix tenant")

        
       
        self.assignmentedCategories = {}
       
        for categoryInfo in InjectedCategories:
            
            self.log.warning("parentCategoryIds")
            self.log.warning(parentCategoryIds)
            
            if(categoryInfo['parentCategoryId'] == '1'):
               
                # parentCategoryIndex = 0
                
                if(os.environ.get("CATEGORY_API_VERSION") == "v1"):
                    parentId = os.environ.get("PRODUCT_ROOT_CATEGORY_ID")
                else:
                    parentId = 'root'
            
            else:
                parentId = parentCategoryIds.get(categoryInfo['parentCategoryId'])

            # if(parentCategoryIndex == 0):
            #     if(os.environ.get("CATEGORY_API_VERSION") == "v1"):
            #         parentId = os.environ.get("PRODUCT_ROOT_CATEGORY_ID")
            #     else:
            #         parentId = 'root'
            # else: 
                
            #     parentId = parentCategoryIds[parentCategoryIndex]

            if(categoryInfo['IsRequest'] is True):
                print("    - importing or updating category to the tenant" ,categoryInfo['catId'] )
                createCategoryBody = { 
                    "localizedName": {
                       language_code[0]['short_code'].lower(): categoryInfo['Name']
                    },
                    "name": categoryInfo['Name'],
                    "position": None,
                    "published": True,
                    "parentId": parentId ,
                    'ecn' : [categoryInfo['catId']]

                } 

                Json_createCategoryBody = json.dumps(createCategoryBody, indent = 4) 

                # print(Json_createCategoryBody)
                self.log.warning(Json_createCategoryBody)
                
                createCategoryRequest = requests.post(url = ENDPPOINT_URL + "/category/"+tenant+"/categories?publish=true", data = Json_createCategoryBody, headers=self.headers)

                
                try:
                    res = createCategoryRequest.json()   
                    self.log.warning("   create category response from this tenant")
                    self.log.warning(res)

                except:
                    print("You have wrong credential.")
                    self.log.warning(" - Error. wrong credential or something in requesting create category. ")
                    return { "Error": "You have wrong credential." }

                
                if('id' in res):
                    print("    - created category to the tenant with standard option" ,categoryInfo['catId'] )
                    self.log.info("  category id {} was created in emporix".format(categoryInfo['catId']))
                    self.imported_category_list.append(categoryInfo['catId'])
                    try:
                
                        
                        parentCategoryIds.update({  categoryInfo['catId']  : res['id'] })
                        
                        if(not categoryIds):
                            if(categoryInfo['catId'] in categoiresFromProducts):
                                self.assignmentedCategories[categoryInfo['catId']] = res['id']
                        else:
                            if(categoryInfo['catId'] in self.extended_categoryIds): 
                               self. assignmentedCategories[categoryInfo['catId']] = res['id']
                
                    except:
                        #parentCategoryIds.append(None)
                        pass
                
                else:
                    
                    if('code' in res and res['code'] == 400):
                        print("  standard create category fail and recall with direct lanaguage EN option")
                        # Recall in case the request does not support current content-language  
                        DefaultContentLanguageheader = {"Authorization": "Bearer " + self.access_token, "Content-Type": "application/json", "X-Version": os.environ.get("CATEGORY_API_VERSION") }
                        defaultCategoryBody = { 
                            "localizedName": {
                                "en": categoryInfo['Name']
                            },
                            "name": categoryInfo['Name'],
                            "position": None,
                            "published": True,
                            "parentId": parentId ,
                            'ecn' : [categoryInfo['catId']]
                        } 
                        Json_default_createCategoryBody = json.dumps(defaultCategoryBody, indent = 4) 
                        # print(Json_default_createCategoryBody)
                        self.log.warning("standard create category fail and recall with direct lanaguage EN option")
                        self.log.warning(Json_default_createCategoryBody)

                        createCategoryRequest = requests.post(url = ENDPPOINT_URL + "/category/"+tenant+"/categories?publish=true", data = Json_default_createCategoryBody, headers=DefaultContentLanguageheader)
                        res = createCategoryRequest.json()
                        self.log.info(res)

                        if('id' in res):
                            print("    - created category to the tenant with remade language option" ,categoryInfo['catId'] )
                            self.log.info("  category id {} was created in emporix".format(categoryInfo['catId']))
                            self.imported_category_list.append(categoryInfo['catId'])
                            # parentCategoryIds.append(res['id'])
                            parentCategoryIds.update({  categoryInfo['catId']  : res['id'] })
                            if(not categoryIds):
                                if(categoryInfo['catId'] in categoiresFromProducts):
                                    self.assignmentedCategories[categoryInfo['catId']] = res['id']
                            else:
                                if(categoryInfo['catId'] in self.extended_categoryIds):
                                    self.assignmentedCategories[categoryInfo['catId']] = res['id']
                        else:
                            # parentCategoryIds.append(None)
                            pass
                    else:
                        # parentCategoryIds.append(None)
                        pass
            
            else:
                print("    - this category is already in the tenant" ,categoryInfo['catId'] )
                self.log.info(" this category {} is already in the tenant.".format(categoryInfo['catId']))
                # parentCategoryIds.append(categoryInfo['categoryID'])
                parentCategoryIds.update({  categoryInfo['catId']  : categoryInfo['categoryID'] })

                if(not categoryIds):
                    if(categoryInfo['catId'] in categoiresFromProducts):    
                        self.assignmentedCategories[categoryInfo['catId']] = categoryInfo['categoryID']
                else:
                    if(categoryInfo['catId'] in self.extended_categoryIds):       
                        self.assignmentedCategories[categoryInfo['catId']] = categoryInfo['categoryID']

            parentCategoryIndex = parentCategoryIndex + 1

       
        error = False
        
        self.log.info("   self.assignmented categories")
        self.log.info(self.assignmentedCategories)
        message = {}

        print(" -  Finished to assign categories to Emporix tenant")
        print(self.assignmentedCategories)
       
        self.numberOfProducts = 0
        
        # assign brands  in emporix brands 

        print(" - assign brands  in emporix brands")

        for brandId in supplierIds:
            brandItem = [x for x in self.brands.id_map if x['ID'] == brandId]
            if(len(brandItem) == 0):
                print("Not match")
            else:
                createBrandRequestBody = {
                    "name": brandItem[0]['name'],
                    "id": brandId,
                    "image": brandItem[0]['pic']
                }
                Json_createBrandRequestBody = json.dumps(createBrandRequestBody, indent = 4) 
                createBrandRequest = requests.post(url = ENDPPOINT_URL + "/brand/brands", data = Json_createBrandRequestBody, headers=self.headers)
                res = createBrandRequest.json()
                if("id" in res):
                    self.log.warning("[Brand ID]" + brandId +" has been created!")
                    f.write("[Brand ID] {} has been created!\n".format(brandId))
       
        f.close()  

        # get products matched for given supplier and category ids again

        if(supplierIds and categoryIds):          
            self.catalogs = [x for x in self.catalogs if x.get("supplier_id") in supplierIds and x.get("catid") in self.extended_categoryIds]

        else:
            self.catalogs = [x for x in self.catalogs if x.get("supplier_id") in supplierIds or x.get("catid") in self.extended_categoryIds]
        
        
        # downloading products selected , adding details into catalog list
        self.log.warning("---------------------  self.catalogs with details start----------------------")
        self.add_product_details_parallel(lang_id=language_code[0]['short_code'], keys=['ProductDescription', 'Product', 'LongSummaryDescription','ProductPicture']) 
        self.log.warning( len(self.catalogs))
        # self.log.warning( self.catalogs)
        self.log.warning("--------------------- self.catalogs with details end----------------------")
        
        

        self.numberOfFailedProducts = 0
        self.failed_product_list = []
        self.catalog_id_list = []
        import_feature_logo_list = []

        # importing labels for the products given category and supplier features
        print("Importing Labels :")

        for product in self.catalogs: 
            cat_id = product['catid']
            product_id = product['product_id']
            feature_id_list = []
            if 'feature_id_list' in product:
                feature_id_list = product['feature_id_list']
            

            for feature_id in feature_id_list:
                for feature_logo in self.featurelogo.id_map:
                    if feature_id == feature_logo['feature_id'] and cat_id in feature_logo['cat_id_list']:
                        import_feature_logo = {
                            "id" : feature_logo['id'],
                            "name" : feature_logo['name'], 
                            "image" : feature_logo['image'] , 
                            'overlay' : {"position" :  0} ,
                            'description' : feature_logo['description']
                        }
                        if not any(x['name'] == import_feature_logo['name'] for x in import_feature_logo_list):
                            import_feature_logo_list.append(import_feature_logo)

        self.log.warning("--------------------- The length of Importing labels ---------------------")
        self.log.warning(len(import_feature_logo_list))

        if len(import_feature_logo_list)  == 0:
            print("  There are no labels matched with given product")
            self.log.warning("  There are no labels matched with given product")
        else:

            key_count = 0
            headers = {"Authorization": "Bearer " + self.access_token, "Content-Type": "application/json" }
            with progressbar.ProgressBar(max_value = len(import_feature_logo_list)) as self.bar:
                for feature_logo in import_feature_logo_list:
                    self.log.warning(self.headers)
                    res = requests.post(url = ENDPPOINT_URL +"/label/labels" , data = json.dumps(feature_logo) , headers = headers)

                    try:
                        res_json = res.json()
                        self.log.warning(res_json)
                        key_count += 1
                        self.bar.update(key_count)
                        
                    except:
                        self.log.warning(" error in response from importing labels")
                
        
        batch_product_size = 500
        
        self.chunked_catalogs =self.divide_list_by_chunksize(self.catalogs , batch_product_size)
        
        # importing products with batches given chunk size

        print("###### start Importing jobs with batches:")
        self.log.info("###### start Importing jobs with batches:")
        start = time.time()
        
        authurl = ENDPPOINT_URL + "/oauth/token"          
        authbody = {
            "client_id": client_id,
            "client_secret": secret_id,
            "grant_type": "client_credentials",
            "scope": "import.import_admin import.import_view import.import_manage",
            "Content-Type": "application/json",
        }

        authRequest = requests.post(authurl, data = authbody)
        
        try:
            authResponse = authRequest.json()  
            self.log.warning("   ### auth response ")
            self.log.warning(authResponse)
            self.log.warning("   ### access_token is " + authResponse['access_token'] )
        except:
            return { "Error": "You have wrong credential." }

        for chunked_catalog in self.chunked_catalogs:
            self.imported_batches_products_idlist = []
            print("    - importing products in batches")
            self.log.info("    - importing products in batches")

            # checking there is already import job or not

            import_job_access_token = authResponse['access_token']

            import_job_url = ENDPPOINT_URL + "/import/" + tenant + "/jobs"
            import_job_headers = {"Authorization": "Bearer " + import_job_access_token  , "Content-Type": "application/json"}
            
            importJobRequest = requests.get(import_job_url,  headers = import_job_headers)
            job_list_response = importJobRequest.json()
            
            self.log.warning(" job list")
            # self.log.warning(job_list_response)   
            pending_job_exist = False
            if len(job_list_response):
                for job in job_list_response:
                    if job["status"] == "IN_PROGRESS":
                        prod_job_id = job['id']
                        pending_job_exist = True
                        break
                if not pending_job_exist:
                    prod_job_id = self.create_import_job( import_job_url , import_job_headers)

            else:
                # creating the new job for importing products
                prod_job_id = self.create_import_job( import_job_url , import_job_headers)

            # send webhook for notification of product import start
            if self.job_mode == 'async':
                payload = {
                    'type'  : 'PRODUCT_IMPORT_START' ,
                    'job_id' : self.job_id ,
                    "import_job_id"  : prod_job_id ,
                    "product_id" : [product['product_id'] for product in chunked_catalog]
                }
                self.create_webhook( payload = payload)

            # making  products to be imported  in batches
            data_list = []
            for catalog in chunked_catalog:
                if "mixins" in catalog:
                    product_data = self.get_product_detail_data(catalog)
                    data_list.append(product_data)
                else:
                    print("  -- no mixins in this product" + catalog['product_id'])
                

            import_data = {
                "data" : data_list,
                "metadata" : {
                    "updateStrategies" : {
                       
                    }
                }
            }

            import_data = json.dumps(import_data, indent=4)

            self.log.warning("   ### Products to be imported")
            # self.log.warning(import_data)

            # import products using importer

            import_products_url = ENDPPOINT_URL + "/import/" + tenant + "/jobs/" +prod_job_id + "/data/products"
           
            import_products_Request = requests.post(import_products_url, data = import_data,  headers = import_job_headers)
            try:
                res = import_products_Request.json()
                self.log.warning(" the response after importing products request")
                self.log.warning(res)              
            except Exception as ex:
                print(ex)

            

            # Mark the impor job as finished

            url = ENDPPOINT_URL + "/import/" + tenant + '/jobs/' + prod_job_id 
            self.mark_import_job_finished(url , import_job_headers)

            # get the status of importing products

            
            number_of_imported_products = 0
            checking_import_products_url = ENDPPOINT_URL + "/import/" + tenant + "/jobs/" + prod_job_id
            req = polling.poll(lambda: requests.get(checking_import_products_url, headers = import_job_headers) , check_success= self.check_product_poll_status , step = 1,  poll_forever=True)
            try:
                res = req.json()
                self.log.warning(" the poll status of importing products request")
                self.log.warning(res)

                number_of_imported_products = res["statistics"]['products']['numberOfSuccessfullyImported']
                self.numberOfProducts += number_of_imported_products
                self.numberOfFailedProducts += res["statistics"]['products']['numberOfFailures']
            except Exception as ex:
                print(ex)
            
            
            if self.job_mode == 'async':
                payload = {
                    'type'  : 'PRODUCT_IMPORT_COMPLETE' ,
                    'job_id' : self.job_id ,
                    "import_job_id"  : prod_job_id ,
                    "number_successful_products" :number_of_imported_products,
                    "number_failed_products" : res["statistics"]['products']['numberOfFailures']
                }
                self.create_webhook( payload = payload)

            # checking the logs of the imported products

            import_job_logs_url = ENDPPOINT_URL + "/import/" + tenant + '/jobs/' + prod_job_id + "/logs"
            import_job_logs_request = requests.get(import_job_logs_url , headers = import_job_headers)
            try: 
                log_res = import_job_logs_request.json()
                self.log.warning("   ## logs of the product import")
                self.log.warning(log_res)
                for log in log_res:
                    if log['logLevel'] == 'INFO' :       # product import successed ? not sure yet!!!!
                       self.imported_batches_products_idlist.append(log['productId'])
                    else: 
                        self.failed_product_list.append(log['productId'])
            except Exception as ex:
                print(ex)
            
            print("     successful imported")
            print(len(self.imported_batches_products_idlist))

            print("     failed imported")
            print(len(self.failed_product_list))

            ###############################################################################################
            # get imported products successfully in batches
            imported_success_batched_products = []

            if  number_of_imported_products == 0:
                print("     # importing products failed, no need to try importing others")
                self.log.info("     # importing products failed, no need to try importing others")
                if self.job_mode == 'async':
                    payload = {
                        'type'  : 'FAILED' ,
                        "job_id"  : self.job_id ,
                        "number_successful_products" : self.numberOfProducts,
                        "number_failed_products" : self.numberOfFailedProducts ,
                        "failed_products_list" :  self.failed_product_list , 
                        "imported_category_list" : self.imported_category_list , 
                        

                    }
                    self.create_webhook( payload = payload)   

                    client = bigquery.Client()
                    query = f"UPDATE `icecat-demo.icecat_data_dataset.jobs` SET status =  'FAILED' where id = '{self.job_id}'"
                    query_job = client.query(query)
                    print(" - update job id into bigquery as FAILED")
            else:
                # imported_success_batched_products = self.imported_batches_products_idlist

                
            
                if len(self.imported_batches_products_idlist):
                    
                    if number_of_imported_products == len(chunked_catalog):
                        imported_success_batched_products = chunked_catalog
                    else:
                        for catalog in chunked_catalog:
                            if catalog['product_id'] in self.imported_batches_products_idlist:
                                imported_success_batched_products.append(catalog)

            ###################################################################################################
                # assign products to categories
                if self.job_mode == 'async':
                    payload = {
                        'type'  : 'ASSIGN_PRODUCTS_START' ,
                        'job_id' : self.job_id ,
                        "import_job_id"  : prod_job_id ,
                        
                    }
                    self.create_webhook( payload = payload)

                self.assign_products_to_products = 0
                self.product_count = 0
                self.import_batch_list = []
                print("    - Assigning products to categories start: " )
                with progressbar.ProgressBar(max_value=len(imported_success_batched_products)) as self.bar:
                    with concurrent.futures.ThreadPoolExecutor(max_workers= int(os.environ.get("EMPORIX_API_WORKERS"))) as threads:
                        t_res = threads.map(self.assign_products_to_category_worker , imported_success_batched_products)

                print("        Assigning products to categories completed: " , self.assign_products_to_products)
                if self.job_mode == 'async':
                    payload = {
                        'type'  : 'ASSIGN_PRODUCTS_COMPLETED' ,
                        'job_id' : self.job_id, 
                        "import_job_id"  : prod_job_id ,
                        "number_successful_products" :self.assign_products_to_products,
                        "product_id" : self.import_batch_list
                    }
                    self.create_webhook( payload = payload)

            ###################################################################################################
                # imporing images sync using thread
            
                if self.job_mode == 'async':
                    payload = {
                        'type'  : 'IMAGE_IMPORT_START' ,
                        'job_id' : self.job_id ,
                        "import_job_id"  : prod_job_id ,
                        "product_id" : self.imported_batches_products_idlist
                    }
                    self.create_webhook( payload = payload)
                
                self.number_of_import_images_batch = 0
                self.image_count = 0
                self.import_images_batch_list = []
                print("    - importing images in batch start: ")
                with progressbar.ProgressBar(max_value=len(imported_success_batched_products)) as self.bar:
                    with concurrent.futures.ThreadPoolExecutor(max_workers= int(os.environ.get("EMPORIX_API_WORKERS"))) as threads:
                        t_res = threads.map(self.upload_product_image_worker , imported_success_batched_products)

                print("         importing images in batch completed: " , self.number_of_import_images_batch)
                if self.job_mode == 'async':
                    payload = {
                        'type'  : 'IMAGE_IMPORT_COMPLETED' ,
                        'job_id' : self.job_id, 
                        "import_job_id"  : prod_job_id ,
                        "number_successful_products" :self.number_of_import_images_batch,
                        "product_id" : self.import_images_batch_list
                    }
                    self.create_webhook( payload = payload)

                ###################################################################################################
                # importing prices

                print("    - Importing price data")
                if len(self.prices):

                    price_data_list = []
                    importing_price_product_id_list = []

                    for product in imported_success_batched_products:
                        for price_object in self.prices:
                            category_id = ""
                            supplier_id = ""
                            price = 0

                            if price_object.get('category'):
                                category_id = price_object.get('category')
                            if price_object.get('supplier'):
                                supplier_id = price_object.get('supplier')
                            min_value = price_object.get('from')
                            max_value = price_object.get('to')
                            
                            if len(price_object) == 4:  # category and supplier are given
                                if product['catid'] == category_id and product['supplier_id'] == supplier_id:
                                    price = self.get_random_price_with_given_range(min_value, max_value)
                                    break
                            elif len(price_object) == 3:  # only category is given
                                if product['catid'] == category_id:
                                    price = self.get_random_price_with_given_range(min_value, max_value)
                                    break
                            else:                       # both categor and supplier are not given
                                price = self.get_random_price_with_given_range(min_value, max_value)
                                break
                        
                        price_data = {
                            "prices": [
                                {
                                "type": "V1NO",
                                
                                "effectiveAmount": 1,
                                "dateRange": {
                                    "startDate": "2021-02-10T23:00:00",
                                    "endDate": "2039-01-01T22:59:59"
                                },
                                "basePrice": {
                                    "effectiveAmount": price,
                                    "originalAmount": price,
                                    
                                    "priceFactor": 1,
                                    "basePriceFactor": price
                                },
                                "presentationPrice": {
                                    "effectiveAmount": 1,
                                    "originalAmount": 1,
                                    
                                    "priceFactor": 1,
                                    "basePriceFactor": price
                                },
                                "originalAmount": 1,
                                "currency": "EUR",
                                
                                }
                            ],
                            "processMode": "MODIFY",
                            "siteCode": "main",
                            "productId": product['product_id']
                        }

                        price_data_list.append(price_data)
                        importing_price_product_id_list.append(product['product_id'])
                    
                    # generating price data
                    import_price_obj = {
                        'data' : price_data_list,
                        'metadata' : {
                            'updateStrategies' : {
                                'prices' : 'insert_update'
                            }
                        }
                    }
                    # self.log.info(json.dumps(import_price_obj , indent=4))
                    # creating the new job for importing stocks

                    price_job_id = self.create_import_job(import_job_url, import_job_headers)

                    if self.job_mode == 'async':
                        payload = {
                            'type' : 'PRICE_IMPORT_START' ,
                            'job_id' : self.job_id, 
                            'import_job_id' : price_job_id,
                            'product_id' : importing_price_product_id_list
                        }
                        self.create_webhook(payload=payload)

                    # importing prices using importer

                    import_price_url = ENDPPOINT_URL + '/import/' + tenant + '/jobs/' + price_job_id + '/data/siteprices'
                    import_price_req = requests.post(import_price_url, data = json.dumps(import_price_obj) , headers=import_job_headers)

                    try:
                        res = import_price_req.json()
                        self.log.warning("  # the response after importing price request")
                        self.log.info(res)
                        
                    except Exception as ex:
                        print(ex)

                    # get the status of importing prices

                    
                    checking_import_price_url = ENDPPOINT_URL + "/import/" + tenant + "/jobs/" +  price_job_id

             
                    req = polling.poll(lambda:requests.get(checking_import_price_url, headers = import_job_headers), check_success= self.check_price_poll_status , step = 1,  poll_forever=True)
                    try:
                        res = req.json()
                        self.log.warning(" the poll status of importing price request")
                        self.log.warning("    price imported . successful : " + str(res["statistics"]['prices']['numberOfSuccessfullyImportedPrices']) + ",  failed : " + str(res["statistics"]['prices']['numberOfFailedImportedPrices']))         
                        print("    price imported . successful : " + str(res["statistics"]['prices']['numberOfSuccessfullyImportedPrices']) + ",  failed : " + str(res["statistics"]['prices']['numberOfFailedImportedPrices']))    
                       

                        if self.job_mode == 'async':
                            payload = {
                                'type' : 'PRICE_IMPORT_COMPLETE' ,
                                'job_id' : self.job_id, 
                                'import_job_id' : price_job_id,
                                'number_success_price' : res["statistics"]['prices']['numberOfSuccessfullyImportedPrices'],
                                'number_failed_price' : res["statistics"]['prices']['numberOfFailedImportedPrices'],
                                'product_id' : importing_price_product_id_list
                            }
                            self.create_webhook(payload=payload)

                    except Exception as ex:
                        print(ex)
                    
                    # Mark the import job as finished

                    url = ENDPPOINT_URL + "/import/" + tenant + '/jobs/' + price_job_id 
                    self.mark_import_job_finished( url , import_job_headers)

                ###################################################################################################
                # importing stocks

                print("    - Importing stock level")
                self.log.info("    - Importing stock level")

                # creating the new job for importing stocks

                stock_job_id = self.create_import_job(import_job_url, import_job_headers)

                if self.job_mode == "async":
                    payload = {
                        'type' : 'STOCK_IMPORT_START',
                        'job_id' : self.job_id, 
                        'import_job_id' : stock_job_id , 
                        'product_id' : self.imported_batches_products_idlist
                    }
                    self.create_webhook(payload = payload)

                # making stock data with successfully imported products

                stock_list = []
                today = datetime.today().strftime('%Y-%m-%d')     
                # self.log.warning(imported_success_batched_products)
                for catalog in imported_success_batched_products:
                    if not catalog['on_market'] or catalog.get('releasedate') == None or catalog.get('releasedate') == "" or catalog.get('releasedate') > today:
                        stocklevel = 0
                    else:
                        rand_stock_level_index =  random.randint(0, 2) 
                        max_stock_level_value = 0
                        min_stock_level_value = 0
                        if rand_stock_level_index == 0:
                            min_stock_level_value = 0
                            max_stock_level_value = self.low_stock_max
                        elif rand_stock_level_index == 1:
                            min_stock_level_value = self.low_stock_max + 1
                            max_stock_level_value = self.medium_stock_max
                        else:
                            min_stock_level_value = self.medium_stock_max + 1
                            max_stock_level_value = self.high_stock_max

                        rand_stock_vlaue = random.randint(min_stock_level_value , max_stock_level_value)

                        stock_list.append({
                            "site" : 'main' , 
                            'productId' : catalog['product_id'] , 
                            'stockLevel' : rand_stock_vlaue
                        })

                stock_data = {
                    'data' : stock_list,
                    'metadata': 
                        {
                            'importType' : 'SITESTOCKLEVELS'
                        }
                    }
                
                stock_data = json.dumps(stock_data , indent= 4)

                # importing stocks using importer

                import_stock_request = requests.post(url = ENDPPOINT_URL + "/import/"+tenant+"/jobs/" + stock_job_id + "/data/sitestocklevels", 
                    data = stock_data, headers=import_job_headers)
                try:
                    res = import_stock_request.json()
                    self.log.warning(" the response after importing stocks request")
                    self.log.info(res)
                except Exception as ex:
                    print(ex)

                # get the status of importing stocks

                number_of_imported_products = 0
                checking_import_stocks_url = ENDPPOINT_URL + "/import/" + tenant + "/jobs/" + stock_job_id
                
                req = polling.poll(lambda:requests.get(checking_import_stocks_url, headers = import_job_headers), check_success= self.check_stock_poll_status , step = 1,  poll_forever=True)
                try:
                    res = req.json()
                    self.log.warning(" the poll status of importing stocks request")
                    self.log.warning("    Stock imported . successful : " + str(res["statistics"]['stocklevel']['numberOfSuccessfullyImportedStocklevels']) + ",  failed : " + str(res["statistics"]['stocklevel']['numberOfFailedImportedStocklevels']))         
                    print("    Stock imported . successful : " + str(res["statistics"]['stocklevel']['numberOfSuccessfullyImportedStocklevels']) + ",  failed : " + str(res["statistics"]['stocklevel']['numberOfFailedImportedStocklevels']))    

                    number_of_imported_stock = res["statistics"]['stocklevel']['numberOfSuccessfullyImportedStocklevels']
                    
                    
                    if self.job_mode == "async":
                            payload = {
                                'type' : 'STOCK_IMPORT_COMPLETE',
                                'job_id' : self.job_id ,
                                'import_job_id' : stock_job_id , 
                                'number_successful_stock' : number_of_imported_stock,
                                'number_failed_stock' : res["statistics"]['stocklevel']['numberOfFailedImportedStocklevels'],
                                'product_id' : self.imported_batches_products_idlist
                            }
                            self.create_webhook(payload = payload)

                except Exception as ex:
                    print(ex)
                
                # Mark the import job as finished

                url = ENDPPOINT_URL + "/import/" + tenant + '/jobs/' + stock_job_id 
                self.mark_import_job_finished( url , import_job_headers)

            
       
        # end of immporting products in batches
        upload_time = int(time.time() -  start)

        if self.job_mode == 'async':
            payload = {
                'type'  : 'COMPLETED' ,
                "job_id"  : self.job_id ,
                "number_successful_products" : self.numberOfProducts,
                "number_failed_products" : self.numberOfFailedProducts ,
                "failed_products_list" :  self.failed_product_list , 
                "imported_category_list" : self.imported_category_list , 
                "upload_product_time" : str(upload_time) + "s"

            }
            self.create_webhook( payload = payload)   

            client = bigquery.Client()
            query = f"UPDATE `icecat-demo.icecat_data_dataset.jobs` SET status =  'COMPLETED' where id = '{self.job_id}'"
            query_job = client.query(query)
            print(" - update job id into bigquery as completed")    
        
        
        if(error):
            return { "Error": message }
        else:
            return { "Success": "You have imported  {} products successfully!".format(str(self.numberOfProducts))}

    def create_import_job(self , import_job_url , import_job_headers):
        
        # Creating the import job anyway
        
        gmt = time.gmtime()
        data = {
            "importType" : 'PRODUCTS', 
            "timestamp" : str(calendar.timegm(gmt)) ,
            "updateType" : 'MODIFY'
        }
        json_object = json.dumps(data, indent = 4) 

        job_req = requests.post(url = import_job_url , headers = import_job_headers , data = json_object)
        job_list_response = job_req.json()
        self.log.warning("  new job created")
        self.log.warning(job_list_response)  
        job_id = job_list_response['id']
        self.log.warning("   ### job_id " + job_id)

        return job_id

    def mark_import_job_finished(self , url , import_job_headers):
        
        # Creating the import job anyway
       
        payload = json.dumps({"status": "UPLOAD_FINISHED" }, indent = 4) 
        req = requests.put(url = url , headers = import_job_headers , data = payload)
        response = req.json()
        self.log.warning("  # mark the job as finished")
        self.log.warning(response)  
        
    def check_product_poll_status(self, response):
        print("       product polling now")
        try:
            res  = response.json()
            number_of_imported_products = res["statistics"]['products']['numberOfSuccessfullyImported']
        except Exception as ex:
            print(ex)
        return number_of_imported_products

    def check_price_poll_status(self, response):
        print("       price polling now")
        try:
            res  = response.json()
            number_of_imported_products = res["statistics"]['prices']['numberOfSuccessfullyImportedPrices']
        except Exception as ex:
            print(ex)
        return number_of_imported_products

    def check_stock_poll_status(self, response):
        print("       stock polling now")
        try:
            res  = response.json()
            number_of_imported_products = res["statistics"]['stocklevel']['numberOfSuccessfullyImportedStocklevels']
        except Exception as ex:
            print(ex)
        return number_of_imported_products

    def upload_product_worker(self, product) :
       
        ean = ''
        longdesc = ''
        description = ''
        shortdesc = ''
        name = ""

        if("ean" in product):
            ean = product['ean']
        if("longdesc" in product):
            longdesc = product['longdesc']
        if("shortdesc" in product):
            shortdesc = product['shortdesc']

        

        if longdesc == "":
            if "longsummarydescription" in product: 
                description = product['longsummarydescription']
        else:
            description = longdesc

        if "generatedinttitle" in product:
            name = product['generatedinttitle']
        else:
            name = product['model_name']
        
        manual_pdf_url = ""
        if "manualpdfurl" in product:
            if product['manualpdfurl'] == "":
                manual_pdf_url= product['pdfurl']
            else:
                manual_pdf_url = product['manualpdfurl']

        popularity = 0
        if 'product_view' in product:
            popularity = int(product['product_view'])

        if popularity != 0:
            custom_attribute = {
                "productCustomAttributes":
                {
                    "brand": product["supplier_id"],
                    "gtin8": ean,
                    "longDescription": description ,
                    "popularity" : popularity
                }
            }
        else:
            custom_attribute = {
                "productCustomAttributes":
                {
                    "brand": product["supplier_id"],
                    "gtin8": ean,
                    "longDescription": description
                   
                }
            }

        metadata = product['metadata']
        metadata.update({"productCustomAttributes": "https://res.cloudinary.com/saas-ag/raw/upload/v1560527845/schemata/CAAS/productCustomAttributesMixIn-v38.json" ,
            "salePricesData": "https://res.cloudinary.com/saas-ag/raw/upload/schemata/salePriceData.json",
            "productBundle": "https://res.cloudinary.com/saas-ag/raw/upload/schemata/productBundleMixIn.v5.json" ,
            "generalFeatures": "https://storage.googleapis.com/icecat_mixin/icecat_general_mixin.json"
            })
        
        mixins = product['mixins']
        mixins.update(custom_attribute)
        mixins.update( {
             
            "generalFeatures":{
                "release_date" : product['releasedate'] if "releasedate" in product else "",
                "end_of_life_date": product['end_of_life_date'] if "end_of_life_date" in product else "",
                "reasons_tobuy" : product['reasons_tobuy'] if "reasons_tobuy" in product else "",
                "bullet_points" : product['bullet_points'] if "bullet_points" in product else "",
                "manual_pdf_url" : manual_pdf_url ,
                "warranty_info" : product['warrantyinfo'] if "warrantyinfo" in product else "",
                'on_market' : True if product['on_market'] == "1" else False ,
                "country_markets" : product['country_markets'] if "country_markets" in product else [],
                "ean_upc_list" : product['eans'] if "eans" in product else []
            } 
            
            })
        
        
        body = { 
            'name': name, 
            'code': product["product_id"], 
            'published': True if product['on_market'] == "1" else False, 
            "metadata": {
                "mixins": metadata
            },
            "mixins": mixins,
            "description": description,
            "id": product["product_id"]
        }

        
        json_object = json.dumps(body, indent = 4) 
        
        # self.log.warning(json_object)

        createProductResponse = requests.post(url = ENDPPOINT_URL + "/product/"+ self.tenant+"/products", data = json_object, headers=self.headers)
        createProductData = createProductResponse.json()

       

        if(createProductResponse.status_code == 201 or createProductResponse.status_code == 409):
            error = False
           
            self.numberOfProducts = self.numberOfProducts + 1

            if(createProductResponse.status_code == 201):
                assignmentBody = { "ref": { "id": createProductData['id'], "type": "PRODUCT" } }
                json_object = json.dumps(assignmentBody, indent = 4) 
                assignmentRes = requests.post(url = ENDPPOINT_URL + "/category/"+self.tenant+"/categories/"+self.assignmentedCategories[product["catid"]]+"/assignments", data = json_object, headers=self.headers)
                self.log.warning("[..assigning product ID "+ product['product_id'] +" to category ID"+ product["catid"] +"]")
                self.log.warning("Icecat Product ID (" + product["product_id"] + ") has been created")
                self.log.warning("Emporix Product ID (" + createProductData["id"] + ") has been created")

                # Injecting product media files
                if product.get('medias'):
                    if(len(product['medias']) != 0):
                        self.Injecting_product_medias(product['medias'], product["product_id"], self.headers, self.tenant, self._log_file)

            else:
                updateProductResponse = requests.patch(url=ENDPPOINT_URL + "/product/"+self.tenant+"/products/"+product["product_id"], data=json_object, headers= self.headers)
                self.log.warning("Emporix Product ID (" + product["product_id"] + ") has been updated")
                fu = open(self._log_file, 'a')
                fu.write("Emporix Product ID (" + product["product_id"] + ") has been updated. \n")
                fu.close() 
                if product.get('medias'):
                        if(len(product['medias']) != 0):
                                self.Injecting_product_medias(product['medias'], product["product_id"], self.headers, self.tenant, self._log_file)

            self.bar.update(self.numberOfProducts)

        else:
            error = True
            self.numberOfFailedProducts += 1
            self.failed_product_list.append(product["product_id"])

            message = createProductResponse.json()
            if(error):
               self.log.error(" error in uploading : ")
               self.log.error(json.dumps(message))

        self.catalog_id_list.append(product["product_id"])
        if self.job_mode == "async":
            if (self.numberOfProducts + self.numberOfFailedProducts) % int(os.environ.get("NOTIFICATION_IMPORT_PRODUCT")) == 0 or (self.numberOfProducts + self.numberOfFailedProducts) == len(self.catalogs):
                    
                    payload = {
                        'type'  : 'IMPORTING_PRODUCT' ,
                        "job_id"  : self.job_id ,
                        "product_ids" :  self.catalog_id_list
                    }
                    self.create_webhook( payload = payload)
                    self.catalog_id_list.clear()

    def get_product_detail_data(self, product) :
       
        ean = ''
        longdesc = ''
        description = ''
        shortdesc = ''
        name = ""

        if("ean" in product):
            ean = product['ean']
        if("longdesc" in product):
            longdesc = product['longdesc']
        if("shortdesc" in product):
            shortdesc = product['shortdesc']

        if longdesc == "":
            if "longsummarydescription" in product: 
                description = product['longsummarydescription']
        else:
            description = longdesc

        if "generatedinttitle" in product:
            name = product['generatedinttitle']
        else:
            name = product['model_name']
        
        manual_pdf_url = ""
        if "manualpdfurl" in product:
            if product['manualpdfurl'] == "":
                manual_pdf_url= product['pdfurl']
            else:
                manual_pdf_url = product['manualpdfurl']

        popularity = 0
        if 'product_view' in product:
            popularity = int(product['product_view'])

        if popularity != 0:
            custom_attribute = {
                "productCustomAttributes":
                {
                    "brand": product["supplier_id"],
                    "gtin8": ean,
                    "longDescription": description ,
                    "popularity" : popularity
                }
            }
        else:
            custom_attribute = {
                "productCustomAttributes":
                {
                    "brand": product["supplier_id"],
                    "gtin8": ean,
                    "longDescription": description
                   
                }
            }

        metadata = {}
        if "metadata" in product:
            metadata = product['metadata']
        metadata.update({
            "generalFeatures": "https://storage.googleapis.com/icecat_mixin/icecat_general_mixin.json" ,
            "productCustomAttributes": "https://res.cloudinary.com/saas-ag/raw/upload/v1560527845/schemata/CAAS/productCustomAttributesMixIn-v40.json" ,
            "salePricesData": "https://res.cloudinary.com/saas-ag/raw/upload/schemata/salePriceData.json",
            "productBundle": "https://res.cloudinary.com/saas-ag/raw/upload/schemata/productBundleMixIn.v5.json" ,
            "externalAttributes": "https://res.cloudinary.com/saas-ag/raw/upload/v1612513656/schemata/CAAS/externalAttributes-v7.json"
            })
        mixins = {}
        if 'mixins' in product:
            mixins = product['mixins']
        
        mixins.update(custom_attribute)
        mixins.update({
            "externalAttributes": {
                    "acn": [
                        {
                            "acn": product['catid']
                        }
                    ],
                    "supplier": "sample_supplier"
                },
        })
        mixins.update( {
            
        "generalFeatures":{
            "release_date" : product['releasedate'] if "releasedate" in product else "",
            "end_of_life_date": product['end_of_life_date'] if "end_of_life_date" in product else "",
            "reasons_tobuy" : product['reasons_tobuy'] if "reasons_tobuy" in product else "",
            "bullet_points" : product['bullet_points'] if "bullet_points" in product else "",
            "manual_pdf_url" : manual_pdf_url ,
            "warranty_info" : product['warrantyinfo'] if "warrantyinfo" in product else "",
            'on_market' : True if product['on_market'] == "1" else False ,
            "country_markets" : product['country_markets'] if "country_markets" in product else [],
            "ean_upc_list" : product['eans'] if "eans" in product else []
        } 
        
        })
        
        
        body = { 
            'name': name, 
            'code': product["product_id"], 
            'processMode' : 'MODIFY' ,
            "metadata": {
                "mixins": metadata
            },
            "mixins": mixins,
            "description": description,
            "published" : True if product['on_market'] == "1" else False,
            "id": product["product_id"] , 
            "supplier": {
                "id": "sample_supplier",
                "name": "sample_supplier",
                "supplierNo": "sample_supplier",
                "customerNo": "",
                "street": "",
                "zipCode": "",
                "city": "",
                "countryId": "",
                "contactPerson1": "",
                "phone1": "",
                "email1": "",
                "contactPerson2": "",
                "phone2": "",
                "email2": "",
                "website": "",
                "comment": "",
                "fax": "",
                "orderEmail1": "sample-suppliers@emporix.com",
                "orderEmail2": "",
                "orderChannel": [
                    "EMAIL"
                ],
                "orderMethod": "COLLECTED_PICKING"
            }
        }

        return body

    def assign_products_to_category_worker(self, product):
        
        
        assignmentBody = { 
            "ref": 
                { 
                    "id": product['product_id'], 
                    "url" : ENDPPOINT_URL + "/product/"+ self.tenant +"/products/"+ product['product_id'] ,
                    "type": "PRODUCT" 
                }
        }

        json_object = json.dumps(assignmentBody, indent = 4) 
        assignmentRes = requests.post(url = ENDPPOINT_URL + "/category/"+self.tenant+"/categories/"+self.assignmentedCategories[product["catid"]]+"/assignments", data = json_object, headers=self.headers)
        
        self.log.warning("[..assigning product ID "+ product['product_id'] +" to category ID "+ product["catid"] +"]")

        self.assign_products_to_products += 1
        self.import_batch_list.append(product['product_id'])

        self.product_count += 1
        self.bar.update(self.product_count)
        
        

    def upload_product_image_worker(self, product) :

        if product.get('medias'):
            if(len(product['medias']) != 0):
                result = self.Injecting_product_medias(product['medias'], product["product_id"], self.headers, self.tenant, self._log_file)
                if result == "success":
                    self.number_of_import_images_batch += 1
                    self.import_images_batch_list.append(product['product_id'])
        self.image_count += 1
        self.bar.update(self.image_count)

    def get_random_price_with_given_range(self, min, max) :
        if isinstance(min, int):   # given number is int
            return round(random.uniform(min, max), 2)
        else:                      # given number is float
            if min % 1 == 0.0:
                return random.randint(min, max)
            elif min % 1 > 0.96:     # given number if .99 eg 12.99
                return random.randint(int(min) , int(max)) + 0.99
            else:                   # given number if 0.95 eg. 12.95
                return random.randint(int(min) , int(max)) + 0.95
            
    def explode_categories(self, category_data, categoryId):
        
        for category in category_data:
            
            category_name = None
            
            if(category['ID'] == categoryId):
               
                category_name = category['Name']
                
                if(categoryId == "1"):
                    pass
                else:
                   
                    self.InjectedCategories.append({ "categoryID": categoryId, "Name": category_name, "parentCategoryId": category['ParentID']  })
                    
                    self.explode_categories(category_data, category['ParentID'])
      
    def adding_detail_worker(self ,   catalog ):
        
        item = catalog
        xml_file = "gs://" + os.environ.get("GOOGLE_PRODUCT_BUCKET")+ "/"+  os.path.basename(item['path'])
        
        try:
            product_detais = IceCatProductDetails(xml_file=xml_file, keys=self.keys, auth=self.auth, data_dir=self.xml_dir, log=self.log,cleanup_data_files=False)
            item.update(product_detais.get_data())
            self.key_count += 1
            self.bar.update(self.key_count)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.log.error(message)
            self.log.error(
                "Could not obtain product details from IceCat for product_id {}".format(item['path']))
            self.key_count += 1
            self.bar.update(self.key_count)
            
    def add_product_details_parallel(self, keys=['ProductDescription'], connections=5, lang_id="EN"):
        """
        Download and parse product details, using threads.

        :param keys: List of Ice Cat product detail XML keys to include in the output.  Refer to Basic Usage Example.
        :param connections: Number of simultanious download threads.  Do not go over 100.
        """
       
        self.keys = keys
        self.connections = connections
        baseurl = 'https://data.Icecat.biz/export/freexml/'
        TYPE = 'Product details'
        urls = []
        self.xml_dir = self.data_dir + 'product_xml/'
        if not os.path.exists(self.xml_dir):
            os.makedirs(self.xml_dir)

        for item in self.catalogs:
            urls.append(baseurl + lang_id + '/' +item['product_id']+'.xml')

        self.log.info("Downloading detail data with {} connections".format(self.connections))
       
        download = bulk_downloader.FetchURLs(log=self.log, urls=urls, auth=self.auth, connections=self.connections, data_dir=self.xml_dir)

        if self.job_mode == 'async':
           
            payload = {
                'type'  : 'NUMBER_OF_PRODUCTS' ,
                "job_id"  : self.job_id ,
                "number of downloaded products" : len(urls)
            }
            self.create_webhook( payload = payload)

        self.key_count = 0
        print("Adding product details:")
        
        with progressbar.ProgressBar(max_value=len(self.catalogs)) as self.bar:     
            with concurrent.futures.ThreadPoolExecutor(max_workers= int(os.environ.get("ADDING_DETAIL_WORKERS"))) as threads:
                t_res = threads.map(self.adding_detail_worker , self.catalogs)
        
    def add_product_details(self, keys=['ProductDescription']):
        """
        Download and parse product details.  Use add_product_details_parallel() instead, for a much improved performance.
        :param keys: List of Ice Cat product detail XML keys to include in the output.  Refer to Basic Usage Example.
        """
        self.keys = keys
        for item in self.catalogs:
            try:
                product_detais = IceCatProductDetails(filename=item['path'], keys=self.keys, auth=self.auth, data_dir=self.data_dir, log=self.log, cleanup_data_files=True)
                item.update(product_detais.get_data())
            except:
                self.log.error("Could not obtain product details from IceCat for product_id {}".format(item['path']))

    def get_data(self):
        """
        Return ordered list of product attributes
        """
        return self.o

    def dump_to_file(self, filename=None):
        """
        Save product attributes to a JSON file
        :param filename: File name
        """
        if filename:
            self.json_file = filename
        else:
            # change extension for the JSON output
            self.json_file = os.path.splitext(self.xml_file)[0] + '.json'
        with open(self.json_file, 'w') as f:
            f.write(json.dumps(self.o, indent=2))
        self.log.info("JSON output written to {}".format(self.json_file))
        f.closed

    def Injecting_product_medias(self, medias, product_id, headers, tenant, _log_file):
        fm = open(_log_file, 'a')
        result = ""

        if self.max_images == 0:   # no limit
            import_medias = medias
        else:
            if self.max_images > len(medias):
                import_medias = medias
            else:
                import_medias = medias[0:self.max_images]

        for media in import_medias:
            try:                
                contentType = ''
                cloudinaryImageID = media['original'].split('img/')[1]
                emporixImageID = cloudinaryImageID.replace("/", "~")
                h = requests.head(media['original'])
                contentType = h.headers.get('content-type')
                mediaCreateBody = {
                    "url": "https://res.cloudinary.com/saas-ag/image/upload/icecatimgstage/icecatproducts/" + cloudinaryImageID,
                    "position": media['no'],
                    "contentType": contentType,
                    "tags": [
                        "product"
                    ],
                    "customAttributes": {
                        "name": "image for product "+product_id,
                        "type": "image/jpeg",
                        "uploadLink": "https://res.cloudinary.com/saas-ag/image/upload/icecatimgstage/icecatproducts/" + cloudinaryImageID,
                        "commitLink": "notUsing",
                        "id": emporixImageID

                    }
                }

                json_mediaCreateBody = json.dumps(mediaCreateBody, indent = 4)
                # Deleting Exsting media files
                delelet_media_respons = requests.delete(url=ENDPPOINT_URL + "/product/"+tenant+"/products/"+ product_id +"/media/"+emporixImageID, headers=headers )

                media_create_response = requests.post(url = ENDPPOINT_URL + "/product/"+tenant+"/products/"+ product_id +"/media2", data = json_mediaCreateBody, headers=headers)
                self.log.warning("[Media ID]: " + emporixImageID  + " Has been created to ProductID "+ product_id +"!")
                # fm.write("[Media ID]: gallery_raw"+media['original'].split('gallery_raw')[1] +" Has been created to ProductID "+ product_id +"! \n")
                result = "success"
            except Exception as ex:
                self.log.warning("media create failed!") 
                self.log.warning(ex)
                result = "fail"
        fm.close()
        return result

    def create_webhook(self, payload):
        response = requests.post(self.webhook_url, payload)
              
class IceCatMixin(IceCat):
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

            for category in self.category:
                category_id = category['ID']
                category_name_desciption = category['Name']
                category_name = category_name_desciption.lower().replace(" ", "_").replace("/", "_")

                for feature in self.features.id_map:
                    
                    description = "Mixin schema for "  + feature['name'].lower() + " of Category : "  +category_name_desciption
                    
                    schema_name = feature['name'].lower().replace(" ", "_").replace("/", "_")
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

                            with open(self.data_dir  +category_name + "-" + schema_name + '.json', 'w') as outfile:
                                json.dump(json_schema, outfile, indent=4)
                                makeCount += 1
                            
                            

        return { "Success": f"You have generated {makeCount} mixin json file successfully!"}

class IceCatDatabase(IceCat):
    
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

                # try:
                #     items.update({'supplier': self.suppliers.get_mfr_byId(items['supplier_id'])})
                # except:
                #     self.log.info("Unable to find supplier for supplier_id: {}".format(items['supplier_id']))

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
                                    "country_markets" :','.join(catalog['country_markets'] if 'country_markets' in catalog else ""
                                    )
                                
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
                    rows_to_insert.append(
                        {'category_id' : id, "category_name": name , "parent_cat_id": parent_id}
                    )
            
            errors = client.insert_rows_json(table_id , rows_to_insert)
            if errors == []:   # successfully inserted
                query = f"MERGE icecat_data_dataset.category T \
                    USING icecat_data_dataset.{temp_table_name} S \
                    ON T.category_id = S.category_id \
                    WHEN MATCHED THEN \
                    UPDATE SET category_name = S.category_name , parent_cat_id = S.parent_cat_id \
                    WHEN NOT MATCHED THEN \
                    INSERT (category_id, category_name, parent_cat_id) VALUES(category_id, category_name, parent_cat_id)"
                query_job = client.query(query)    
                result = query_job.result()
                print("Updated supplier table successfully")
            
            client.delete_table(table_id, not_found_ok = True)

       
        return {"success" : "updated dataset in bigquery" }

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
    
    def getJobsFromTenant(self, tenant_name):
        
        client = bigquery.Client()

        query = f"SELECT * FROM `icecat-demo.icecat_data_dataset.jobs` where tenant_name = '{tenant_name}' order by created_time desc"

        query_job = client.query(query)

        results = query_job.result() 

        if  results.total_rows:  
            results_str = []
            for row in results:
                results_str.append({ "job_id" : row.id  , "tenant_name" : row.tenant_name , "status" : row.status, "created_time": row.created_time})

            return {'result' : results_str}
        else:
            return { "inform" : "There are not any items"}

    def getJobEvents(self, job_id):
        client = bigquery.Client()

        query = f"SELECT * FROM `icecat-demo.icecat_data_dataset.job_events` where job_id = '{job_id}' order by created_time desc"

        query_job = client.query(query)

        results = query_job.result() 

        if  results.total_rows:  
            results_str = []
            for row in results:
                results_str.append({ "job_id" : row.job_id  , "import_job_id" : row.import_job_id , "event_type" : row.event_type, "event_data": json.loads(row.event_data) , "created_time": row.created_time})

            return {'result' : results_str}
        else:
            return { "inform" : "There are not any items"}

