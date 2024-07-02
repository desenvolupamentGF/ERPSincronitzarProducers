# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
ENVIRONMENT = 1
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!

# for logging purposes
import logging

# to deal with excel files
import pandas

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
URL_PERSONS = "/persons"

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

# Database constants
EMMEGI_MYSQL_USER = os.environ['EMMEGI_MYSQL_USER']
EMMEGI_MYSQL_PASSWORD = os.environ['EMMEGI_MYSQL_PASSWORD']
EMMEGI_MYSQL_HOST = os.environ['EMMEGI_MYSQL_HOST']
EMMEGI_MYSQL_DATABASE = os.environ['EMMEGI_MYSQL_DATABASE']

# Other constants
CONN_TIMEOUT = 50

GLAMSUITE_TIPUS_ABC_A = "4665e311-b6d6-45d1-fe25-08dc985fd06e"
GLAMSUITE_TIPUS_ABC_B = "ef08e2f1-f1ce-4f80-fe26-08dc985fd06e"
GLAMSUITE_TIPUS_ABC_C = "4ee26581-07cc-4300-fe27-08dc985fd06e"

GLAMSUITE_PAGAMENTS_ABC_A = "ccf5acab-c803-4666-fe28-08dc985fd06e"
GLAMSUITE_PAGAMENTS_ABC_B = "00e6b0d9-b817-4a87-fe29-08dc985fd06e"
GLAMSUITE_PAGAMENTS_ABC_C = "0002b94c-5ad1-4769-fe2a-08dc985fd06e"

GLAMSUITE_LLIURAMENT_ABC_A = "d7484937-5c20-4074-fe2b-08dc985fd06e"
GLAMSUITE_LLIURAMENT_ABC_B = "b92d8d76-2cba-4103-fe2c-08dc985fd06e"
GLAMSUITE_LLIURAMENT_ABC_C = "d0e5a7cf-7803-452e-fe2d-08dc985fd06e"

GLAMSUITE_PREUS_ABC_A = "a6cea6c7-59ac-4fc5-fe2e-08dc985fd06e"
GLAMSUITE_PREUS_ABC_B = "7f0858ef-d126-4934-fe2f-08dc985fd06e"
GLAMSUITE_PREUS_ABC_C = "5b079a85-4b5b-485c-fe30-08dc985fd06e"

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

# INICI CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024) 
#
#
#def synchronize_contactesProveidors_TEMP(now, myCursor, suppliersExcel):
#    logging.info('   Processing contactes proveïdors from origin ERP (Excel)')
#
#    try:
#        # Preparing message queue
#        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)
#
#        i = 0
#        j = 0
#        _emailAnt = ""
#        for index, row in suppliersExcel[0:1000].iterrows():
#            _idContact = index + 1
#            _idSupplier = str(row["CIF"])
#            if _idSupplier == "nan":
#                continue # skip to next iteration
#            _name = str(row["CONTACTE"])
#            if _name == "nan":
#                continue # skip to next iteration
#            _position = str(row["CÀRREC"])
#            if _position == "nan":
#                _position = ""
#            _phone = str(row["MÒBIL"])
#            if _phone == "nan":
#                _phone = "No informat"
#            _email = str(row["CORREU ELECTRÒNIC"])
#            if _email == "nan":
#                _email = "No informat"
#            _comments = str(row["OBSERVACIONS"])
#            if _comments == "nan":
#                _comments = ""
#
#            data={
#                "queueType": "PROVEIDORS_CONTACTES_TEMP",
#                "name": str(_name).strip(),
#                "nif": str(_idSupplier).strip(),
#                "phone": str(_phone).strip(),
#                "email": str(_email).strip(),
#                "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
#                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
#                "position": _position,
#                "comments": str(_comments).strip(),
#                "correlationId": "CONTACTE_" + str(_idContact).strip()                    
#            }
#
#            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
#            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
#            glam_id, old_data_hash = get_value_from_database(myCursor, "CONTACTE_" + str(_idContact).strip(), URL_PERSONS, "Proveidors ERP GF", "Excel")
#
#            if glam_id is None or str(old_data_hash) != str(data_hash):
#
#                logging.info('      Processing contact: ' + str(_name).strip() + ' ...') 
#
#                # Sending message to queue
#                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String
#
#                j += 1
#
#            _enviamentComandes = str(row["ENVIAMENT COMANDES"])
#            if _enviamentComandes != "nan" and _enviamentComandes != "" and _enviamentComandes != _emailAnt:
#                _emailAnt = _enviamentComandes
#                data={
#                    "queueType": "PROVEIDORS_CONTACTES_TEMP",
#                    "name": str(_enviamentComandes).strip(),
#                    "nif": str(_idSupplier).strip(),
#                    "phone": "No informat",
#                    "email": str(_enviamentComandes).strip(),
#                    "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
#                    "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
#                    "position": "Atenció al client (enviament de comandes)",
#                    "comments": "",
#                    "correlationId": "CONTACTE_A" + str(_idContact).strip()
#                }
#
#                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
#                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
#                glam_id, old_data_hash = get_value_from_database(myCursor, "CONTACTE_A" + str(_idContact).strip(), URL_PERSONS, "Proveidors ERP GF", "Excel")
#
#                if glam_id is None or str(old_data_hash) != str(data_hash):
#
#                    logging.info('      Processing contact: ' + str(_enviamentComandes).strip() + ' ...') 
#
#                    # Sending message to queue
#                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String
#
#                    j += 1
#
#            _reclamacions = str(row["RECLAMACIONS"])
#            if _reclamacions != "nan" and _reclamacions != "" and _reclamacions != _emailAnt:
#                _emailAnt = _reclamacions                
#                data={
#                    "queueType": "PROVEIDORS_CONTACTES_TEMP",
#                    "name": str(_reclamacions).strip(),
#                    "nif": str(_idSupplier).strip(),
#                    "phone": "No informat",
#                    "email": str(_reclamacions).strip(),
#                    "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
#                    "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
#                    "position": "Atenció al client (reclamacions)",
#                    "comments": "",
#                    "correlationId": "CONTACTE_B" + str(_idContact).strip()
#                }
#
#                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
#                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
#                glam_id, old_data_hash = get_value_from_database(myCursor, "CONTACTE_B" + str(_idContact).strip(), URL_PERSONS, "Proveidors ERP GF", "Excel")
#
#                if glam_id is None or str(old_data_hash) != str(data_hash):
#
#                    logging.info('      Processing contact: ' + str(_reclamacions).strip() + ' ...') 
#
#                    # Sending message to queue
#                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String
#
#                    j += 1
#
#            _reclamacionsUrgents = str(row["RECLAMACIONS URGENTS"])
#            if _reclamacionsUrgents != "nan" and _reclamacionsUrgents != "" and _reclamacionsUrgents != _emailAnt:
#                _emailAnt = _reclamacionsUrgents
#                data={
#                    "queueType": "PROVEIDORS_CONTACTES_TEMP",
#                    "name": str(_reclamacionsUrgents).strip(),
#                    "nif": str(_idSupplier).strip(),
#                    "phone": "No informat",
#                    "email": str(_reclamacionsUrgents).strip(),
#                    "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
#                    "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
#                    "position": "Atenció al client (reclamacions urgents)",
#                    "comments": "",
#                    "correlationId": "CONTACTE_C" + str(_idContact).strip()
#                }
#
#                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
#                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
#                glam_id, old_data_hash = get_value_from_database(myCursor, "CONTACTE_C" + str(_idContact).strip(), URL_PERSONS, "Proveidors ERP GF", "Excel")
#
#                if glam_id is None or str(old_data_hash) != str(data_hash):
#
#                    logging.info('      Processing contact: ' + str(_reclamacionsUrgents).strip() + ' ...') 
#
#                    # Sending message to queue
#                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String
#
#                    j += 1
#
#            _reclamacionsCritiques = str(row["RECLAMACIONS CRITIQUES"])
#            if _reclamacionsCritiques != "nan" and _reclamacionsCritiques != "" and _reclamacionsCritiques != _emailAnt:
#                _emailAnt = _reclamacionsCritiques
#                data={
#                    "queueType": "PROVEIDORS_CONTACTES_TEMP",
#                    "name": str(_reclamacionsCritiques).strip(),
#                    "nif": str(_idSupplier).strip(),
#                    "phone": "No informat",
#                    "email": str(_reclamacionsCritiques).strip(),
#                    "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
#                    "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
#                    "position": "Atenció al client (reclamacions crítiques)",
#                    "comments": "",
#                    "correlationId": "CONTACTE_D" + str(_idContact).strip()
#                }
#
#                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
#                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
#                glam_id, old_data_hash = get_value_from_database(myCursor, "CONTACTE_D" + str(_idContact).strip(), URL_PERSONS, "Proveidors ERP GF", "Excel")
#
#                if glam_id is None or str(old_data_hash) != str(data_hash):
#
#                    logging.info('      Processing contact: ' + str(_reclamacionsCritiques).strip() + ' ...') 
#
#                    # Sending message to queue
#                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String
#
#                    j += 1
#
#            i += 1
#            if i % 1000 == 0:
#                logging.info('      ' + str(i) + ' synchronized contactes proveïdors...')    
#        logging.info('      Total synchronized contactes proveïdors: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           
#
#        # Closing queue
#        myRabbitPublisherService.close()
#
#    except Exception as e:
#        logging.error('   Unexpected error when processing contactes proveïdors from original ERP (Excel): ' + str(e))
#        send_email("ERPProveidorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
#        sys.exit(1)
#
#def synchronize_campsPersonalitzatsProveidors_TEMP(now, myCursor, suppliersExcel):
#    logging.info('   Processing camps personalitzats proveïdors from origin ERP (Excel)')
#
#    try:
#        # Preparing message queue
#        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)
#
#        i = 0
#        j = 0
#        for index, row in suppliersExcel[0:1000].iterrows():
#            _idPersonalitzat = index + 1
#            _idSupplier = str(row["CIF"])
#            if _idSupplier == "nan":
#                continue # skip to next iteration
#            _tipus = str(row["TIPUS (A-B-C)                                     A-Proveïdors amb més volum de compres històric                                B-Proveïdors alternatius que alguna vegada hem comprat                                         C-Proveïdors amb poques compres "])
#            if _tipus == "nan":
#                _tipus = ""
#            else:
#                if _tipus == "A":
#                    _tipus = GLAMSUITE_TIPUS_ABC_A
#                elif _tipus == "B":
#                    _tipus = GLAMSUITE_TIPUS_ABC_B
#                elif _tipus == "C":
#                    _tipus = GLAMSUITE_TIPUS_ABC_C
#                else:
#                    _tipus = ""
#            _pagaments = str(row["PAGAMENTS (A-B-C)                  A-Condicions de cobrament flexibles                                    B-Condicions de cobrament estàndard                                  C- Cobrament de comptats o terminis immediats"])
#            if _pagaments == "nan":
#                _pagaments = ""
#            else:
#                if _pagaments == "A":
#                    _pagaments = GLAMSUITE_PAGAMENTS_ABC_A
#                elif _pagaments == "B":
#                    _pagaments = GLAMSUITE_PAGAMENTS_ABC_B
#                elif _pagaments == "C":
#                    _pagaments = GLAMSUITE_PAGAMENTS_ABC_C
#                else:
#                    _pagaments = ""
#            _lliurament = str(row["TERMINIS DE LLIURAMENT"])
#            if _lliurament == "nan":
#                _lliurament = ""
#            else:
#                if _lliurament == "A":
#                    _lliurament = GLAMSUITE_LLIURAMENT_ABC_A
#                elif _lliurament == "B":
#                    _lliurament = GLAMSUITE_LLIURAMENT_ABC_B
#                elif _lliurament == "C":
#                    _lliurament = GLAMSUITE_LLIURAMENT_ABC_C
#                else:
#                    _lliurament = ""
#            _preus = str(row["PREUS"])
#            if _preus == "nan":
#                _preus = ""
#            else:
#                if _preus == "A":
#                    _preus = GLAMSUITE_PREUS_ABC_A
#                elif _preus == "B":
#                    _preus = GLAMSUITE_PREUS_ABC_B
#                elif _preus == "C":
#                    _preus = GLAMSUITE_PREUS_ABC_C
#                else:
#                    _preus = ""
#            _familia = str(row["FAMILIA"])
#            if _familia == "nan":
#                _familia = ""
#            _producte = str(row["PRODUCTE"])
#            if _producte == "nan":
#                _producte = ""
#            _fabrica = str(row["DISTRIBUIDOR"])
#            if _fabrica == "nan":
#                _fabrica = ""
#            _web = str(row["Pàgina web"])
#            if _web == "nan":
#                _web = ""
#    
#            data={
#                "queueType": "PROVEIDORS_CAMPSPERSONALITZATS_TEMP",
#                "nif": str(_idSupplier).strip(),
#                "Tipus_ABC": str(_tipus).strip(), 
#                "Pagaments_ABC": str(_pagaments).strip(), 
#                "Terminis_de_lliurament_ABC": str(_lliurament).strip(), 
#                "Preus_ABC": str(_preus).strip(), 
#                "Família": str(_familia).strip(), 
#                "Producte": str(_producte).strip(), 
#                "Fàbrica": str(_fabrica).strip(), 
#                "Web": str(_web).strip(),
#                "correlationId": "PERSONALITZAT_" + str(_idPersonalitzat).strip()                    
#            }
#
#            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
#            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
#            glam_id, old_data_hash = get_value_from_database(myCursor, "PERSONALITZAT_" + str(_idPersonalitzat).strip(), URL_PERSONS, "Proveidors ERP GF", "Excel")
#
#            if glam_id is None or str(old_data_hash) != str(data_hash):
#
#                logging.info('      Processing camp personalitzat: ' + str(_idSupplier).strip() + ' ...') 
#
#                # Sending message to queue
#                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String
#
#                j += 1
#
#            i += 1
#            if i % 1000 == 0:
#                logging.info('      ' + str(i) + ' synchronized camps personalitzats proveïdors...')    
#        logging.info('      Total synchronized camps personalitzats proveïdors: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           
#
#        # Closing queue
#        myRabbitPublisherService.close()
#
#    except Exception as e:
#        logging.error('   Unexpected error when processing camps personalitzats proveïdors from original ERP (Excel): ' + str(e))
#        send_email("ERPProveidorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
#        sys.exit(1)
#
#
# FINAL CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024) 

def synchronize_contactesProveidors(dbEmmegi, myCursorEmmegi, now, myCursor):
    logging.info('   Processing contactes proveïdors from origin ERP (Emmegi/GFIntranet)')

    try:
        # loop over the contacts
        myCursorEmmegi.execute("SELECT a.email_id as id, a.email, b.cfian, b.denan FROM gfintranet.gfi_prov_emails a, fpsuitedb.anagrafiche b WHERE a.prov_id = b.idAn AND a.activo = 1 AND b.cfian <> '' AND a.email <> '' ") 

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _id, _email, _nif, _nomProveidor in myCursorEmmegi.fetchall():

            data={
                "queueType": "PROVEIDORS_CONTACTES",
                "name": str(_email).strip(),
                "nif": str(_nif).strip(),
                "phone": "No informat",
                "email": str(_email).strip(),
                "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                "position": "Atenció al client (enviament de comandes)",
                "comments": "",
                "correlationId": str(_id).strip()
            }

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(myCursor, str(_id).strip(), URL_PERSONS, "Proveidors ERP GF", "Emmegi/GFIntranet")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing contact ' + str(_id).strip() + ' / ' + str(_email).strip() + ' / ' + str(_nif).strip() + ' / ' + str(_nomProveidor).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized contactes proveïdors...')
        logging.info('      Total synchronized contactes proveïdors: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')        

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing contactes proveïdors from original ERP (Emmegi/GFIntranet): ' + str(e))
        send_email("ERPProveidorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbEmmegi)
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPProveidorsMaintenance'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Proveïdors Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (MySQL)
    db = None
    try:
        db = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        myCursor = db.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL database: ' + str(e))
        send_email("ERPProveidorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(db)
        sys.exit(1)

    # connecting to origin database (Emmegi - MySQL)
    dbEmmegi = None
    try:
        dbEmmegi = connectMySQL(EMMEGI_MYSQL_USER, EMMEGI_MYSQL_PASSWORD, EMMEGI_MYSQL_HOST, EMMEGI_MYSQL_DATABASE)
        myCursorEmmegi = dbEmmegi.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to Emmegi MySQL database: ' + str(e))
        send_email("ERPProveidorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbEmmegi)
        sys.exit(1)

    # INICI CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024) 
    #
    # 
    # L'excel ERP-DEFINITIU MACROS.xlsmx s'ha carregat una única vegada. Comento codi perquè no es necessitarà mai més
    #logging.info('   Opening Excel file ERP-DEFINITIU MACROS.xlsmx')
    #
    # opening Excel file
    #try:
    #    suppliersExcel = pandas.read_excel('excel/ERP-DEFINITIU MACROS.xlsm', sheet_name="Proveïdors", usecols="A:X")
    #except FileNotFoundError as e:
    #    logging.info('   File ERP-DEFINITIU MACROS.xlsmx does not exist. Exiting...')
    #    logging.info('END ERP Proveïdors Maintenance')
    #    send_email("ERPProveidorsMaintenance", now, datetime.now(), "ERROR")
    #    disconnectMySQL(db)
    #    sys.exit(1)
    #except Exception as e:
    #    logging.error('   Unexpected error when opening Excel file: ' + str(e))
    #    send_email("ERPProveidorsMaintenance", now, datetime.now(), "ERROR")
    #    disconnectMySQL(db)    
    #    sys.exit(1)                           
    #
    #synchronize_contactesProveidors_TEMP(now, myCursor, suppliersExcel)    
    #synchronize_campsPersonalitzatsProveidors_TEMP(now, myCursor, suppliersExcel)    
    #
    #
    # FINAL CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024) 

    synchronize_contactesProveidors(dbEmmegi, myCursorEmmegi, now, myCursor)    

    # Send email with execution summary
    send_email("ERPProveidorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Proveïdors Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
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