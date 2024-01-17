from urllib.request import Request
from fastapi import HTTPException
import json
import uuid
from dotenv import load_dotenv
import progressbar
import concurrent.futures
import os
from faker import Faker
import requests
import random

load_dotenv()

class Customer :
    """
    Class for importing fake customer to emporix.io
    """

    def __init__(self ,body):
       
        if body.storefront_secret != "" and body.storefront_client != "":
            if body.env ==  "stage":
                self.endpoint_url = os.environ.get('STAGE_API_URL')
            else:
                self.endpoint_url = "https://api.emporix.io"

            self.authurl = self.endpoint_url + "/customerlogin/auth/anonymous/login"

            self.client_id = body.storefront_client
            self.secret = body.storefront_secret
            self.emporix_client = body.emporix_client
            self.emporix_secret = body.emporix_secret

            self.customerRequestUrl =  self.endpoint_url + "/customer/"+ body.tenant +"/signup"
            self.body = body
       

    def import_customer(self, body):
        if body.storefront_secret != "" and body.storefront_client != "" and body.emporix_secret != "" and body.emporix_client != "":

            session_id = str(uuid.uuid1())
            self.tenant = body.tenant
            authBody = {
                "tenant" :body.tenant ,
                "session-id" : session_id,
                "client_id" : body.storefront_client  
            }

            self.success_added_number = 0
            x = requests.get(self.authurl, params = authBody)
            response = x.json()

            if 'access_token' not in response:
                 raise HTTPException(status_code=401, detail="storefront credentials were invalid")
            access_token = response['access_token']
            
            self.headers = {"Authorization": "Bearer " + access_token,  "session-id" :session_id  , "Content-Type": "application/json" }

            self.faker_list = []
            self.locale_list = []

            # create the faker list
            for i in range(0 , body.number):
                if len(body.locales) > 1:
                    rand_index = random.randint(0, len(body.locales) - 1)
                else:
                    rand_index = 0
                locale = body.locales[rand_index]
                self.locale_list.append(locale)
                self.faker_list.append( Faker(locale)) 

            with progressbar.ProgressBar(max_value= body.number) as self.bar: 
                with concurrent.futures.ThreadPoolExecutor(max_workers= int(os.environ.get("EMPORIX_API_WORKERS"))) as threads:
                    t_res = threads.map(self.import_customer_worker , self.faker_list)
                
            result = {"success" : "We've made {} customers successfuly".format(str(self.success_added_number))}
        

        else:
            print(" need client and secret id")
            result = {"warning" : "You need to put the inputs!"}

        return result
    
    def import_customer_worker(self, fake):
        gender_list = ["Male", "Female"]
        gender = gender_list[random.randint(0 , 1)]
       
        if gender == "Male":
            firstName = fake.first_name_male()
            lastName = fake.first_name_male()
            fullName = firstName + " " + lastName
            title = fake.prefix_male()
        else:
            firstName = fake.first_name_female()
            lastName = fake.first_name_female()
            fullName = firstName + " " + lastName
            title = fake.prefix_female()
        
        language = self.locale_list[self.faker_list.index(fake)]
        # country_code = language.split('_')[1]
        country_code=  fake.current_country_code()

        email = fake.email()
        phone_number = fake.phone_number()
        company_name = fake.company()

        customer_data = {
            "email":email,
            "password": "password123",
            "customerDetails": {
                "title": title,
                "firstName": firstName,
                "middleName": "",
                "lastName": lastName,
                "contactEmail": email,
                "contactPhone":phone_number ,
                "company": company_name,
                "preferredLanguage": language,
                "preferredCurrency": self.body.currency,
                "preferredSite": self.body.preferred_site
            },
            
        }

        customer_data = json.dumps(customer_data, indent = 4)
        res = requests.post(self.customerRequestUrl, data = customer_data , headers=self.headers)
        response = res.json()

        if res.status_code == 201:
            customer_id = response.get('id')
            # Adding a customer address seprately
            customerAddress = {
                "contactName": fullName,
                "companyName": company_name,
                "street": fake.street_name(),  
                "streetNumber" : fake.building_number(),
                "zipCode" : fake.postcode() ,
                "city" : fake.city(),   
                "country": country_code,                  
                "contactPhone": phone_number ,                
                "isDefault": True
                
            }
            
            customerAddressRequestUrl =  self.endpoint_url + "/customer/"+ self.tenant +"/customers/" + customer_id + "/addresses"
            
            authurl =  self.endpoint_url + "/oauth/token"
        
            authbody = {
                "client_id": self.emporix_client ,
                "client_secret": self.emporix_secret,
                "grant_type": "client_credentials",
                "scope": "customer.consent_manage customer.customer_update",
            }

            authRequest = requests.post(authurl, data = authbody)
            
            authResponse = authRequest.json() 

            if 'access_token' not in authResponse:
                 raise HTTPException(status_code=401, detail="emporix credentials were invalid")
           
            access_token = authResponse['access_token']

            headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/json" }
            
            customerAddress = json.dumps(customerAddress, indent = 4)
            res = requests.post(customerAddressRequestUrl, data = customerAddress , headers = headers)
            response = res.json()
          
            if res.status_code == 201:
                address_id = response.get('id')
                customerAddressTagsRequestUrl = self.endpoint_url + "/customer/" + self.tenant + "/customers/" + customer_id + "/addresses/" + address_id + "/tags"
                params = { "tags" : "BILLING,SHIPPING"}
                res =  requests.post(customerAddressTagsRequestUrl,  headers= headers , params=params)
                self.success_added_number += 1
                self.bar.update(self.success_added_number)
       
