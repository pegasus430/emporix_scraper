import secrets
from xmlrpc.client import Boolean
from fastapi import Depends, FastAPI, APIRouter , BackgroundTasks , HTTPException , status
from fastapi.openapi.utils import get_openapi
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware
import requests
from .icecat import icecat_product
from .icecat import icecat_admin
from .icecat import icecat_jobs
from .icecat import icecat_search
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import List
from .customer import Customer
import uuid

load_dotenv()

app = FastAPI()
security = HTTPBasic()
router = APIRouter()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

job_id = None

class Item(BaseModel):
    categoryIds: List[str | None] = []
    supplierIds: List[str | None] = []
    secret: str
    client_id: str
    tenant: str
    env:str = ""
    hook_url:str = ""
    languages: List[str | None] = ["1"]
    max_images:str  = "1"
    low_stock_max : str = "10"
    medium_stock_max : str = "50"
    high_stock_max : str = "100"
    generatePrices: List [object] = []
    max_products: int = 1000

class CatalogSearchItem(BaseModel) :
    categoryIds: List[str | None] = []
    supplierIds: List[str | None] = []

class CustomerItem(BaseModel):
    number: int = 1
    currency: str = "USD"
    locales: List[str | None ] = ["en_US" , "it_IT"]
    password : str = "password123"
    tenant: str = ""
    storefront_client: str = ""
    storefront_secret : str = ""
    emporix_client : str = ""
    emporix_secret : str = ""
    env : str = "stage"
    preferred_site : str = "main"

class SearchItem(BaseModel):
    search_region:str = ""
    search_string: str = ""

class TenantItem(BaseModel):
    tenant_name : str = ""

class ImportJobID(BaseModel):
    job_id : str = ""

class SuggestCategoryIds(BaseModel):
    categoryIds: List[str | None] = []


def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = os.environ.get("AUTH_USERNAME").encode("utf-8")
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = correct_username_bytes = os.environ.get("AUTH_PASSWORD").encode("utf-8")
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

def background_import_staff(body: Item):
    if(len(body.languages) == 0):
        lang_data = ['1']
    else:
        lang_data = body.languages

    downloadCatalog = icecat_product.IceCatCatalog(fullcatalog= True, lang_id = lang_data , categoryIds = body.categoryIds, supplierIds = body.supplierIds, tenant = body.tenant, hook_url = body.hook_url , env = body.env , secret = body.secret, client_id = body.client_id , job_id = job_id , max_images= body.max_images, max_import_products = body.max_products)
    res = downloadCatalog.full_import_staff(body.categoryIds, body.supplierIds, body.secret, body.client_id, body.tenant, body.env , body.low_stock_max, body.medium_stock_max, body.high_stock_max , body.generatePrices , body.max_products)
    json_compatible_item_data = jsonable_encoder(res)
    return JSONResponse(content=json_compatible_item_data)

@app.post("/")
async def inject_Products_Emporix(body: Item, background_tasks: BackgroundTasks , authorized: Boolean = Depends(get_current_username)):

    # validate stock max values
    if authorized:
        low_stock_max = int(body.low_stock_max)
        medium_stock_max = int(body.medium_stock_max)
        high_stock_max = int(body.high_stock_max)

        if low_stock_max < medium_stock_max and medium_stock_max < high_stock_max:
            pass
        else:
            raise HTTPException(status_code=500, detail="Pls validate the stock max values!")
        
        # validate authentication with given client id and seceret 
        ENDPPOINT_URL = "https://api.emporix.io"
        
        if body.env == "stage":
            ENDPPOINT_URL = os.environ.get("STAGE_API_URL")

        authurl = ENDPPOINT_URL + "/oauth/token"
        
        authbody = {
            "client_id": body.client_id,
            "client_secret": body.secret,
            "grant_type": "client_credentials",
            "scope": "product.product_create product.product_publish category.category_create category.category_publish category.category_update saasag.brand_manage product.product_update product.product_publish product.product_delete product.product_delete_all",
        }

        authRequest = requests.post(authurl, data = authbody)
        
        try:
            authResponse = authRequest.json()   
            res_scope = authResponse['scope']
            tenant_name = res_scope.split("tenant=")[1]
            scopes = res_scope.split("tenant=")[0]

            if body.tenant != tenant_name:
                raise HTTPException(status_code=401, detail="credentials were invalid or Incorrect tenant name!")

        except:
            raise HTTPException(status_code=401, detail="credentials were invalid or Incorrect tenant name!")
        

        if(len(body.languages) == 0):
            lang_data = ['1']
        else:
            lang_data = body.languages
    
        # if hook url is given, the system run as background mode for hook 

        if body.hook_url != "":
           pass
        else:
            body.hook_url = os.environ.get('DEFAULT_WEBHOOK')
        
        global job_id
        job_id = str(uuid.uuid1())
        background_tasks.add_task(background_import_staff , body)      
        return JSONResponse({"job_id": job_id , "success" : True, "message" : "The app is working in async mode. you can check the webhook endpoint!"})
            
@app.get("/create-mixin_json")
async def create_Mixin_Json( authorized: Boolean = Depends(get_current_username)):
    if authorized:
        res = icecat_admin.IceCatMixin().makeMixin()
        json_compatible_item_data = jsonable_encoder(res)
        return JSONResponse(content=json_compatible_item_data)

@app.post("/create-customers")
async def create_Fake_Customers(body: CustomerItem , authorized: Boolean = Depends(get_current_username)):
    if authorized:
        _customer = Customer(body)
        result = _customer.import_customer(body)
        json_compatible_item_data = jsonable_encoder(result)
        return JSONResponse(content=json_compatible_item_data)

@app.get('/sync_search_index_from_Icecat')
async def handle_Supplier_Category_Language_Database( authorized: Boolean = Depends(get_current_username)):
    if authorized:
        res = icecat_admin.IceCatDatabase().syncSearchIndexDatabase()
        json_compatible_item_data = jsonable_encoder(res)
        return JSONResponse(content=json_compatible_item_data)

@app.get('/sync_catalog_index_from_Icecat')
async def handle_CatalogIndex_Database( authorized: Boolean = Depends(get_current_username)):
    if authorized:
        res = icecat_admin.IceCatDatabase().syncCatalogIndexDatabase()
        json_compatible_item_data = jsonable_encoder(res)
        return JSONResponse(content=json_compatible_item_data)        

@app.post('/search_supplier_category_index')
async def search_Supplier_Category_Index(body: SearchItem, authorized: Boolean = Depends(get_current_username)):

    if authorized:
        search_region = body.search_region
        search_string = body.search_string

        if search_region ==  "":
            raise HTTPException(status_code=500, detail="You must put at least one region. eg. 'category' or 'supplier'.")
        else:
            res = icecat_search.IceCatDatabase().getIndexFromDB(search_data = body)
            json_compatible_item_data = jsonable_encoder(res)
            return JSONResponse(content=json_compatible_item_data)

@app.get("/search_language_index")
async def search_Language( authorized: Boolean = Depends(get_current_username)):
    if authorized:
        res = icecat_search.IceCatDatabase().getLanguageFromDB()
        json_compatible_item_data = jsonable_encoder(res)
        return JSONResponse(content=json_compatible_item_data)

@app.post("/search_Catalog_index")
async def search_Number_Of_Catalogs(body: CatalogSearchItem, authorized: Boolean = Depends(get_current_username)):
    if authorized:

        if len(body.categoryIds) == 0 and len(body.supplierIds) == 0:
            raise HTTPException(status_code=500, detail="Please enter at least one of the IDs .")
            
        else:
            res = icecat_search.IceCatDatabase().getNumberOfCatalogs(categoryIds = body.categoryIds, supplierIds = body.supplierIds)
            json_compatible_item_data = jsonable_encoder(res)
            return JSONResponse(content=json_compatible_item_data)

@app.post("/search_jobs_for_tenant")
async def search_jobs_for_tenant(body: TenantItem, authorized:Boolean = Depends(get_current_username) ):
    if authorized:
        if body.tenant_name == "":
            raise HTTPException(status_code=500, detail="Please put the tenant name!")
        else:
            res = icecat_jobs.IceCatDatabase().getJobsFromTenant(tenant_name = body.tenant_name)
            json_compatible_item_data = jsonable_encoder(res)
            return JSONResponse(content=json_compatible_item_data)

@app.post('/search_job_events_for_jobID')
async def search_jobevents(body: ImportJobID, authorized:Boolean = Depends(get_current_username) ):
    if authorized:
        if body.job_id == "":
            raise HTTPException(status_code=500, detail="Please put the job id !")
        else:
            res = icecat_jobs.IceCatDatabase().getJobEvents(job_id = body.job_id)
            json_compatible_item_data = jsonable_encoder(res)
            return JSONResponse(content=json_compatible_item_data)

@app.post('/suggest_suppliers')
async def search_Suggest_Suppliers(body : SuggestCategoryIds , authorized : Boolean = Depends(get_current_username)):
    if authorized:
        categoryIds = body.categoryIds
        if len(categoryIds) ==  0:
            raise HTTPException(status_code=500, detail="Expected at least one category id")
        else:
            res = icecat_jobs.IceCatDatabase().getSuggestSuppliers(categoryIds = categoryIds)
            json_compatible_item_data = jsonable_encoder(res)
            return JSONResponse(content=json_compatible_item_data)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Icecat data",
        version="2.5.0",
        description="This is the open API for emporix",
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }

    openapi_schema['components']['schemas']['SearchItem']['properties']['search_region'] = {
            "title": "Search Region",
            "type": "string",
            "enum" : [ 'category' , 'supplier'] ,
            'description' : 'You can put category or supplier for the search region'
          }

    openapi_schema['paths']['/']['post'].update({'tags' : ['IceCat']})
    openapi_schema['paths']['/search_language_index']['get'].update({'tags' : ['IceCat']})
    openapi_schema['paths']['/search_supplier_category_index']['post'].update({'tags' : ['IceCat']})
    openapi_schema['paths']['/search_Catalog_index']['post'].update({'tags' : ['IceCat']})
    openapi_schema['paths']['/search_jobs_for_tenant']['post'].update({'tags' : ['IceCat']})
    openapi_schema['paths']['/search_job_events_for_jobID']['post'].update({'tags' : ['IceCat']})
    openapi_schema['paths']['/suggest_suppliers']['post'].update({'tags' : ['IceCat']})
    openapi_schema['paths']['/create-mixin_json']['get'].update({'tags' : ['Admin']})
    openapi_schema['paths']['/sync_search_index_from_Icecat']['get'].update({'tags' : ['Admin']})
    openapi_schema['paths']['/sync_catalog_index_from_Icecat']['get'].update({'tags' : ['Admin']})
    openapi_schema['paths']['/create-customers']['post'].update({'tags' : ['Sample Data']})

    app.openapi_schema = openapi_schema
    
    return app.openapi_schema

app.openapi = custom_openapi
