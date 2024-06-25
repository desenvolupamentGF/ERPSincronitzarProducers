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
from utils import send_email, connectMySQL, disconnectMySQL
import os

# End points URLs
URL_CALENDARS = "/calendars"
URL_DEPARTMENTS = "/departments"
URL_TIMETABLES = "/timetable"
URL_WORKFORCES = "/workforces"

# FELIX-IMPORTANT - API Sesame at https://apidocs.sesametime.com/    (with region "eu2")
URL_CALENDARS_SESAME = "/schedule/v1/holiday-calendar"
URL_DEPARTMENTS_SESAME = "/core/v3/departments"
URL_TIMETABLES_SESAME = "/schedule/v1/schedule-templates"
URL_EMPLOYEES_SESAME = "/core/v3/employees"
URL_API_SESAME = os.environ['URL_API_SESAME']
TOKEN_API_SESAME = os.environ['TOKEN_API_SESAME']

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
    mycursor.execute("SELECT erpGFId, hash FROM ERP_GF.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
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

def synchronize_calendarisLaborals(now, myCursor):
    logging.info('   Processing calendaris laborals from origin ERP (Sesame)')

    try:
        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        endProcess = False
        page = 1
        while not endProcess:

            headers = {
                "Authorization": "Bearer " + TOKEN_API_SESAME, 
                "Content-Type": "application/json"
            }

            get_req = requests.get(URL_API_SESAME + URL_CALENDARS_SESAME + "?page=" + str(page), headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
            response = get_req.json()

            for data in response["data"]:

                _id = data["id"]
                _name = data["name"]
                _companyId = GLAMSUITE_DEFAULT_COMPANY_ID
                    
                _holidays = []
                for holiday in data["daysOff"]:
                    _holiday={
                        "date": holiday["date"],
                        "reasonId": 1,
                        "correlationId": holiday["id"]                       
                    }
                    _holidays.append(_holiday)

                data={
                    "queueType": "RRHH_CALENDARISLABORALS",
                    "name": str(_name).strip(),
                    "companyId": str(_companyId).strip(),
                    "holidays": _holidays,
                    "correlationId": str(_id).strip()                    
                }

                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                glam_id, old_data_hash = get_value_from_database(myCursor, data["correlationId"], URL_CALENDARS, "Recursos Humans ERP GF", "Sesame")

                if glam_id is None or str(old_data_hash) != str(data_hash):

                    logging.info('      Processing calendari laboral: ' + data["name"] + ' ...') 

                    # Sending message to queue
                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                    j += 1

                i += 1
                if i % 1000 == 0:
                    logging.info('      ' + str(i) + ' synchronized calendars...')    
        
            meta = response["meta"]
            if str(meta["lastPage"]) == str(page):
                endProcess = True
            else:
                page = page + 1

        logging.info('      Total synchronized calendars: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing calendars from original ERP (Sesame): ' + str(e))
        send_email("ERPRecursosHumansMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        sys.exit(1)

def synchronize_departments(now, myCursor):
    logging.info('   Processing departments from origin ERP (Sesame)')

    try:
        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        endProcess = False
        page = 1        
        while not endProcess:

            headers = {
                "Authorization": "Bearer " + TOKEN_API_SESAME, 
                "Content-Type": "application/json"
            }

            get_req = requests.get(URL_API_SESAME + URL_DEPARTMENTS_SESAME + "?page=" + str(page), headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
            response = get_req.json()

            for data in response["data"]:

                _id = data["id"]
                _name = data["name"]
                _companyId = GLAMSUITE_DEFAULT_COMPANY_ID
                    
                data={
                    "queueType": "RRHH_DEPARTMENTS",
                    "name": str(_name).strip(),
                    "companyId": str(_companyId).strip(),
                    "correlationId": str(_id).strip()                    
                }

                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                glam_id, old_data_hash = get_value_from_database(myCursor, data["correlationId"], URL_DEPARTMENTS, "Recursos Humans ERP GF", "Sesame")

                if glam_id is None or str(old_data_hash) != str(data_hash):

                    logging.info('      Processing department: ' + data["name"] + ' ...') 

                    # Sending message to queue
                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                    j += 1

                i += 1
                if i % 1000 == 0:
                    logging.info('      ' + str(i) + ' synchronized departments...')    

            meta = response["meta"]
            if str(meta["lastPage"]) == str(page):
                endProcess = True
            else:
                page = page + 1

        logging.info('      Total synchronized departments: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing departments from original ERP (Sesame): ' + str(e))
        send_email("ERPRecursosHumansMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        sys.exit(1)

def synchronize_timetables(now, myCursor):
    logging.info('   Processing timetables from origin ERP (Sesame)')

    try:
        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        endProcess = False
        page = 1        
        while not endProcess:

            headers = {
                "Authorization": "Bearer " + TOKEN_API_SESAME, 
                "Content-Type": "application/json"
            }

            get_req = requests.get(URL_API_SESAME + URL_TIMETABLES_SESAME + "?page=" + str(page), headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
            response = get_req.json()

            for data in response["data"]:

                _id = data["id"]
                _name = data["name"]
                _companyId = GLAMSUITE_DEFAULT_COMPANY_ID
                    
                data={
                    "queueType": "RRHH_TIMETABLES",
                    "name": str(_name).strip(),
                    "companyId": str(_companyId).strip(),
                    "correlationId": str(_id).strip()                    
                }

                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                glam_id, old_data_hash = get_value_from_database(myCursor, data["correlationId"], URL_TIMETABLES, "Recursos Humans ERP GF", "Sesame")

                if glam_id is None or str(old_data_hash) != str(data_hash):

                    logging.info('      Processing timetable: ' + data["name"] + ' ...') 

                    # Sending message to queue
                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                    j += 1

                i += 1
                if i % 1000 == 0:
                    logging.info('      ' + str(i) + ' synchronized timetables...')    

            meta = response["meta"]
            if str(meta["lastPage"]) == str(page):
                endProcess = True
            else:
                page = page + 1

        logging.info('      Total synchronized timetables: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing timetables from original ERP (Sesame): ' + str(e))
        send_email("ERPRecursosHumansMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        sys.exit(1)

def synchronize_workforces(now, myCursor):
    logging.info('   Processing workforces from origin ERP (Sesame)')

    try:
        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        endProcess = False
        page = 1        
        while not endProcess:

            headers = {
                "Authorization": "Bearer " + TOKEN_API_SESAME, 
                "Content-Type": "application/json"
            }

            get_req = requests.get(URL_API_SESAME + URL_EMPLOYEES_SESAME + "?page=" + str(page), headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
            response = get_req.json()

            for data in response["data"]:

                _name = data["jobChargeName"]
                _companyId = GLAMSUITE_DEFAULT_COMPANY_ID
                    
                data={
                    "queueType": "RRHH_WORKFORCES",
                    "name": str(_name).strip(),
                    "companyId": str(_companyId).strip(),
                    "correlationId": str(_name).strip()                    
                }

                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                glam_id, old_data_hash = get_value_from_database(myCursor, data["correlationId"], URL_WORKFORCES, "Recursos Humans ERP GF", "Sesame")

                if glam_id is None or str(old_data_hash) != str(data_hash):

                    logging.info('      Processing workforce: ' + data["name"] + ' ...') 

                    # Sending message to queue
                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                    j += 1

                i += 1
                if i % 1000 == 0:
                    logging.info('      ' + str(i) + ' synchronized workforces...')    

            meta = response["meta"]
            if str(meta["lastPage"]) == str(page):
                endProcess = True
            else:
                page = page + 1

        logging.info('      Total synchronized workforces: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing workforces from original ERP (Sesame): ' + str(e))
        send_email("ERPRecursosHumansMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPRecursosHumansMaintenance'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP RRHH Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (MySQL)
    db = None
    try:
        db = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        myCursor = db.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL database: ' + str(e))
        send_email("ERPRecursosHumansMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(db)
        sys.exit(1)

    synchronize_calendarisLaborals(now, myCursor)    
    synchronize_departments(now, myCursor)    
    synchronize_timetables(now, myCursor)    
    synchronize_workforces(now, myCursor)    

    # Send email with execution summary
    send_email("ERPRecursosHumansMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP RRHH Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing databases
    db.close()
    myCursor.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()