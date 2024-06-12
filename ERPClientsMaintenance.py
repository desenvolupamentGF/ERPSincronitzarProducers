# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
ENVIRONMENT = 1
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!

# for logging purposes
import logging

# for hash/encrypt reasons
import hashlib

# Import to work with json data
import json

# for RabbitMQ messaging between publishers and consumers
import pika

# Import needed library for HTTP requests
import requests

# extra imports
import sys
import datetime
from utils import send_email, connectMySQL, disconnectMySQL, replaceCharacters
import os

# End points URLs
URL_PERSONS = "/persons"
URL_API_PIPEDRIVE = os.environ['URL_API_PIPEDRIVE']
TOKEN_API_PIPEDRIVE = os.environ['TOKEN_API_PIPEDRIVE']

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']
GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID']
GLAMSUITE_DEFAULT_LANGUAGE_CATALA = os.environ['GLAMSUITE_DEFAULT_LANGUAGE_CATALA']

# Rabbit constants for messaging
RABBIT_URL = os.environ['RABBIT_URL']
RABBIT_PORT = os.environ['RABBIT_PORT']
RABBIT_QUEUE = os.environ['RABBIT_QUEUE']

# Database constants
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']

# Other constants
CONN_TIMEOUT = 50

def get_value_from_database(mycursor, correlation_id: str, url, endPoint, origin):
    mycursor.execute("SELECT erpGFId, hash FROM gfintranet.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    hash = None
    for x in myresult:
        erpGFId = str(x[0])
        hash = str(x[1])

    return erpGFId, hash

def get_value_from_database_helper(mycursor, endPoint, origin, helper):
    mycursor.execute("SELECT erpGFId, hash FROM gfintranet.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND deploy = " + str(ENVIRONMENT) + " AND helper = '" + str(helper) + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    hash = None
    for x in myresult:
        erpGFId = str(x[0])
        hash = str(x[1])

    return erpGFId, hash

class RabbitPublisherService:

    def __init__(self, rabbit_url: str, rabbit_port: str, queue_name: str):
        self.rabbit_url = rabbit_url
        self.rabbit_port = rabbit_port
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_url, port=self.rabbit_port))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def publish_message(self, message: str):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def close(self):
        if self.connection is not None and self.connection.is_open:
            self.connection.close()

def synchronize_clients(now, myCursorEmmegi):
    logging.info('   Processing clients from origin ERP (Pipedrive)')

    try:
        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        endProcess = False
        start = 0
        limit = 100
        i = 0
        j = 0
        while not endProcess:
            get_req = requests.get(URL_API_PIPEDRIVE + URL_PERSONS + TOKEN_API_PIPEDRIVE + "&start=" + str(start) + "&limit=" + str(limit), 
                                   verify=False, timeout=CONN_TIMEOUT)
            response = get_req.json()

            if not response["success"]:
                logging.error('   Not success call to Pipedrive api')
                send_email("ERPClientsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
                sys.exit(1)
            else:                
                for data in response["data"]:

                    # We need to get the GUID of the organization using the name of the organization. Hard but we try via removing special characters and comparing uppercase values.
                    helper = replaceCharacters(str(data["org_name"]).strip(), ['.',',','-',' '], True)    
                    glam_id, old_data_hash = get_value_from_database_helper(myCursorEmmegi, 'Organizations ERP GF', 'Sage', helper)
                    if glam_id is None: 
                        continue # if not found, this contact is not used. Next!

                    _phone = "No informat"
                    for phone in data["phone"]:
                        if phone["value"] != "":
                            _phone = phone["value"]
                            break

                    _email = "No informat"
                    for email in data["email"]:
                        if email["value"] != "":
                            _email = email["value"]
                            break
                    
                    _position = ""
                    if data["d24090ac1279a03b5a189832ced766a1462edd8f"] != None:
                        _position = str(data["d24090ac1279a03b5a189832ced766a1462edd8f"]).strip()

                    data={
                        "queueType": "CLIENTS_CONTACTES",
                        "name": str(data["name"]).strip(),
                        "organizationId": glam_id,
                        "phone": str(_phone).strip(),
                        "email": str(_email).strip(),
                        "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
                        "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                        "position": _position,
                        "comments": str("").strip(),
                        "correlationId": str(data["id"]).strip()                    
                    }

                    #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                    data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                    glam_id, old_data_hash = get_value_from_database(myCursorEmmegi, data["correlationId"], URL_PERSONS, "Clients ERP GF", "Pipedrive")

                    if glam_id is None or str(old_data_hash) != str(data_hash):

                        logging.info('      Processing contact name: ' + data["name"] + ' ...') 

                        # Sending message to queue
                        myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                        j += 1

                    i += 1
                    if i % 1000 == 0:
                        logging.info('      ' + str(i) + ' synchronized clients...')    

                pagination = response["additional_data"]["pagination"]
                if pagination["more_items_in_collection"]:
                    start = pagination["next_start"]
                else:
                    endProcess = True

        logging.info('      Total synchronized clients: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing clients from original ERP (Pipedrive): ' + str(e))
        send_email("ERPClientsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPClientsMaintenance'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Clients Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to Emmegi database (MySQL)
    dbEmmegi = None
    try:
        dbEmmegi = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        myCursorEmmegi = dbEmmegi.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL emmegi database: ' + str(e))
        send_email("ERPClientsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbEmmegi)
        sys.exit(1)

    synchronize_clients(now, myCursorEmmegi)    

    # Send email with execution summary
    send_email("ERPClientsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Clients Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing databases
    dbEmmegi.close()
    myCursorEmmegi.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()