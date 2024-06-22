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

# extra imports
import sys
import datetime
from utils import send_email, connectMySQL, disconnectMySQL
import os

# End points URLs
URL_FAMILIES = '/productFamilies'
URL_LOCATIONS = '/locations'
URL_PRODUCTS = '/products'

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']
GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID']
GLAMSUITE_DEFAULT_FAMILY_PARENT_ID = os.environ['GLAMSUITE_DEFAULT_FAMILY_PARENT_ID']

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

def synchronize_families(dbOrigin, mycursor, now):
    logging.info('   Processing families from origin ERP (Emmegi)')

    # processing families from origin ERP (Emmegi)
    try:
        # loop over the families
        mycursor.execute("SELECT IdClasse, Descrizione FROM fpsuitedb.articoliclassi")

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _code, _description in mycursor.fetchall():
            data={
                "queueType": "MERCADERIES_FAMILIES",
                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                "parentFamilyId": GLAMSUITE_DEFAULT_FAMILY_PARENT_ID,
                "productTypeId": 1,                         # S'ha acordat internament que posarem tipus 'Material'.
                "batchTraceabilityId": 1,                   # S'ha acordat internament que posarem tipus 'Batch'.
                "batchSelectionCriteriaId": 1,              # S'ha acordat internament que posarem tipus 'FIFO'.
                "code": str(_code).strip(),
                "name": str(_description).strip(),
                "correlationId": str(_code).strip(),
            }

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(mycursor, str(_code).strip(), URL_FAMILIES, "Mercaderies ERP GF", "Emmegi")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing family ' + str(_code).strip() + ' / ' + str(_description).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized families...')
        logging.info('      Total synchronized families: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')               

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing families from original ERP (Emmegi): ' + str(e))
        send_email("ERPMercaderiesMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)

def synchronize_projects(dbOrigin, mycursor, now):
    logging.info('   Processing projects from origin ERP (Emmegi)') 

    # processing projects from origin ERP (Emmegi)
    try:
        # loop over the projects
        mycursor.execute("SELECT pkid, ragsoc FROM fp_pro32.fp_codtabindirizzi")

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _id, _description in mycursor.fetchall():

            data={
                "queueType": "MERCADERIES_PROJECTES",
                "correlationId": str(_id).strip(),
                "zoneId": str(GLAMSUITE_DEFAULT_ZONE_ID),
                "containerTypeId": str(GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID),
                "description": str(_description).strip(),
                "aisle": "A",
                "rack": "A",
                "shelf": "A",                
                "position": str(_id).strip(),
                "preferential": False
            }       

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(mycursor, str(_id).strip(), URL_LOCATIONS, "Mercaderies ERP GF", "Emmegi")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing projects ' + str(_id).strip() + ' / ' + str(_description).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized projects...')
        logging.info('      Total synchronized projects: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')        

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing projects from original ERP (Emmegi): ' + str(e))
        send_email("ERPMercaderiesMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)

def synchronize_products(dbOrigin, mycursor, now):
    logging.info('   Processing products from origin ERP (Emmegi)')

    # processing products from origin ERP (Emmegi)
    try:
        # ----------------------------------------------------------------------------------------------
        #   Les famílies estan a la mateixa tala que els articles. Les excloem.
        #   Els costos els agafem de l'última compra.
        # ----------------------------------------------------------------------------------------------

        # TAL I COM ACORDAT AMB GARCIA FAURA, DE TOTS ELS ARTICLES AGAFEM EL PREU DE COST DE LA FITXA DEL PROVEÏDOR
        # PRINCIPAL. SI NO HI HA PROVEÏDOR PRINCIPAL, DEL PRIMER QUE TROBEM. AQUEST PREU L'ENTREM COM A PREU A 31/12/21

        costos = {}

        # loop over the costs
        mycursor.execute("""
            SELECT a.Codice, case when afc.PrezzoUnitSc = 0 then ifnull(ava.Costo,0) else afc.PrezzoUnitSc end, max(afc.PrezzoUnitSc * af.Pred)
            from fpsuitedb.articoli a
            join fpsuitedb.articolifornitori af on a.Codice = af.Codice
            join fpsuitedb.articolifornitoricosti afc on af.Codice = afc.CodArt and af.IdFor = afc.IdFor
            left join fpsuitedb.articolivariantiammesse ava on ava.codtip = a.Codice        
            where
                a.UM = af.UMFor 
                and left(a.Codice,1) <> '-'                         
            group by a.Codice
            order by a.Codice, af.Pred desc
                         """)

        for _code, _price, _referencia in mycursor.fetchall():
            if _code not in costos:
                costos[_code] = []
            costos[_code].append(
                {
                    "date": '2021-12-31',  # Tots els preus de proveïdor es posen a aquesta data
                    "cost": _price
                }
            )

        # ARA AGAFEM TOTS ELS PREUS DE COMPRA DELS ARTICLES I ELS UTILITZAREM COM A ÚLTIM COST

        mycursor.execute("""
            select 
                date(o.DataOrd), 
                ov.Codice, 
                round(sum((ov.PrezzoTip * ov.QtaUMFor + ov.PrezzoVar) * ov.Qta * (1 - ov.Sconto / 100)) / sum(ov.Qta),5) Prezzo
            from fpsuitedb.ordini o
            join fpsuitedb.ordinivoci ov on o.NroOrd = ov.NroOrd and o.AnnoOrd = ov.AnnoOrd
            join fpsuitedb.articoli a on ov.Codice = a.Codice
            where
                ov.Codice not in (
                    '--', 'ACABADOS', 'LACADO', 'INCREMENTO', 'IMPORTE-MINIMO', 'GALVANIZAR', 'CARGO', 
                    'GESTION DE RESIDUOS', 'ZINCADO', 'REPARACION', 'RC', 'PORTES', 'P-ELEVADORA', 'MANIPULACIO'
                )
                and left(ov.Codice,1) <> '-'            -- son articles varis
                and right(o.RiferimentoOrd,3) <> 'RSC'  -- son albarans de "reposició sense càrreg"
                and date(o.DataOrd) >= '2022-01-01'     -- Convingut amb Jordi Dilmé
                and (ov.PrezzoTip + ov.PrezzoVar) > 0
                and ov.Qta > 0
                and ov.UM = a.UM                        -- Això és per rtreure l'error.
            group by date(o.DataOrd), ov.Codice
                                     """)

        for _data, _code, _price in mycursor.fetchall():
            if _code not in costos:
                costos[_code] = []
            costos[_code].append(
                {
                    "date": str(_data),
                    "cost": _price
                }
            )

        logging.info('      Synchronizing Products')

        mycursor.execute("""
                select 
                    Codice,
                    Case when trim(Descrizione) = "" then Codice else Descrizione end Descrizione, 
                    IdClasse,
                    trim(UM) UM
                from fpsuitedb.articoli 
                where
                    Parent is not null
                    and left(Codice,1) <> '-'       -- L'Amador ens diu que els que comencen amb "-" són varis
                    and Codice not in (
                        select Codice               --  | 
                        from fpsuitedb.articoli     --  |
                        where Codice in (           --  |
                            select parent           --  |  Aquesta select retrona les famílies i superfamílies
                            from fpsuitedb.articoli --  |  (descarta els articles)
                            group by parent         --  |
                        )
                    )
                                     """
        )

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0 
        for _code, _description, _family_id, _format_code in mycursor.fetchall():
            cost_list = costos.get(_code, [])
            data={
                "queueType": "MERCADERIES_PRODUCTES",
                "correlationId": str(_code).strip(),
                "code": str(_code).strip(),
                "name": str(_description).strip(),
                "description": str(_description).strip(),
                "familyCorrelationId": str(_family_id).strip(),
                "costs": cost_list,
                "formats": [
                    {
                        "quantity": 1,
                        "barcodes": [],
                        "formatCorrelationId": _format_code
                    }
                ]
            }

            if data['familyCorrelationId'] is None or str(data['familyCorrelationId']) == "0":
                continue # skip this product

            # Get Glam Family id.
            glam_family_id, nothing_to_do = get_value_from_database(mycursor, correlation_id=data['familyCorrelationId'], url=URL_FAMILIES, endPoint="Mercaderies ERP GF", origin="Emmegi")
            if glam_family_id is None:
                logging.error('Error sync:' + URL_PRODUCTS + ":" + str(_code).strip() + 
                              " Missing product family: " + URL_FAMILIES + ":" + data['familyCorrelationId'])
                continue # skip this product

            data["familyId"] = glam_family_id

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(mycursor, str(_code).strip(), URL_PRODUCTS, "Mercaderies ERP GF", "Emmegi")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing product ' + str(_code).strip() + ' / ' + str(_description).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized products...')
        logging.info('      Total synchronized products: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')             

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing products from original ERP (Emmegi): ' + str(e))
        send_email("ERPMercaderiesMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPMercaderiesMaintenance'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Mercaderies Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (MySQL)
    dbOrigin = None
    try:
        dbOrigin = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        mycursor = dbOrigin.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL emmegi database: ' + str(e))
        send_email("ERPMercaderiesMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)

    synchronize_families(dbOrigin, mycursor, now)
    synchronize_projects(dbOrigin, mycursor, now)
    synchronize_products(dbOrigin, mycursor, now)    

    # Send email with execution summary
    send_email("ERPMercaderiesMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Mercaderies Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing database
    mycursor.close()
    dbOrigin.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()