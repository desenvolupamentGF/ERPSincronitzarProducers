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
from utils import send_email, connectMySQL, disconnectMySQL, connectAccess, disconnectAccess
import os

# End points URLs
URL_PRODUCTIONORDERS = '/productionOrders'

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']
GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID']

GLAMSUITE_DEFAULT_PRODUCT_ID = os.environ['GLAMSUITE_DEFAULT_PRODUCT_ID']
GLAMSUITE_DEFAULT_PROCESS_SHEET_ID = os.environ['GLAMSUITE_DEFAULT_PROCESS_SHEET_ID']
GLAMSUITE_DEFAULT_ROUTING_OPERATION_ALUMINI_ID = os.environ['GLAMSUITE_DEFAULT_ROUTING_OPERATION_ALUMINI_ID']
GLAMSUITE_DEFAULT_ROUTING_OPERATION_FERRO_ID = os.environ['GLAMSUITE_DEFAULT_ROUTING_OPERATION_FERRO_ID']
GLAMSUITE_DEFAULT_WAREHOUSE_ALUMINI_ID = os.environ['GLAMSUITE_DEFAULT_WAREHOUSE_ALUMINI_ID']
GLAMSUITE_DEFAULT_WAREHOUSE_FERRO_ID = os.environ['GLAMSUITE_DEFAULT_WAREHOUSE_FERRO_ID']

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

# Database constants
ACCESS_NONO = os.environ['ACCESS_NONO']

# Other constants
YEARS_TO_RECALCULATE = 3

# TO LOG ERRORS AND WARNINGS
def save_log_database(dbOrigin, mycursor, endPoint, message, typeLog):
    sql = "INSERT INTO ERP_GF.ERPIntegrationLog (dateLog, companyId, endpoint, deploy, message, typeLog) VALUES (NOW(), %s, %s, %s, %s, %s) "
    val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), str(endPoint), str(ENVIRONMENT), str(message), str(typeLog))
    mycursor.execute(sql, val)
    dbOrigin.commit()  

# TO BE USED WHEN NEEDED
def get_value_from_database(mycursor, correlation_id: str, url, endPoint, origin):
    mycursor.execute("SELECT erpGFId, hash FROM ERP_GF.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    hash = None
    for x in myresult:
        erpGFId = str(x[0])
        hash = str(x[1])

    return erpGFId, hash

# TO BE USED ONLY WHEN COLUMN helper ON TABLE ERPIntegration IS NEEDED !!!
# Check the following query time to time. It should NOT retrieve any row:
# select * FROM ERP_GF.ERPIntegration where deploy=1 and endpoint='Treballadors ERP GF' and calltype='/workers' and helper=''
def get_value_from_database_helper(mycursor, endPoint, origin, helper):
    mycursor.execute("SELECT erpGFId, correlationId FROM ERP_GF.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND deploy = " + str(ENVIRONMENT) + " AND helper = '" + str(helper).replace("'", "''") + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    correlationId = None
    for x in myresult:
        erpGFId = str(x[0])
        correlationId = str(x[1])

    return erpGFId, correlationId

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

def synchronize_productionOrders(dbNono, myCursorNono, now, dbOrigin, myCursor):
    logging.info('   Processing production orders from origin ERP (Access-Nono)')

    # processing production orders from origin ERP (Access-Nono)
    try:
        # loop over the production orders (following WHERE conditions agreed with Nono as to get all the active OFs)
        # plus all OFs created in the last 3 years
        myCursorNono.execute("SELECT Of, FechaPrevista, Tipo, Descripcion, ROUND(IIF(ISNULL(CORTE_CargaHoras), 0, CORTE_CargaHoras*3600)+IIF(ISNULL(MECANIZADO_CargaHoras), 0, MECANIZADO_CargaHoras*3600)+IIF(ISNULL(MATRICERIA_CargaHoras), 0, MATRICERIA_CargaHoras*3600)+IIF(ISNULL(ENSAMBLADO_CargaHoras), 0, ENSAMBLADO_CargaHoras*3600)+IIF(ISNULL(VIDRIO_CargaHoras), 0, VIDRIO_CargaHoras*3600), 2) FROM [OFS Presupuestado] WHERE (Data_OK_Fabricacion IS NOT NULL AND Tipo IN ('ALU','FERRO') AND OfAcabada IS NULL) OR (FechaPrevista >= Date() - (365 * " + str(YEARS_TO_RECALCULATE) + ")) ") 

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        workerTimes = {}         
        for _of, _fechaPrevista, _tipo, _descripcion, _duration in myCursorNono.fetchall():

            name = "Fabricació alumini"
            routingOperationId = GLAMSUITE_DEFAULT_ROUTING_OPERATION_ALUMINI_ID
            warehouseId = GLAMSUITE_DEFAULT_WAREHOUSE_ALUMINI_ID
            if _tipo == "FERRO":
                name = "Fabricació ferro"
                routingOperationId = GLAMSUITE_DEFAULT_ROUTING_OPERATION_FERRO_ID
                warehouseId = GLAMSUITE_DEFAULT_WAREHOUSE_FERRO_ID

            if _of not in workerTimes:
                workerTimes[_of] = []    

            # Worker Times Tickets
            myCursorNono.execute("SELECT IdDiario, Matricula, Data, IIF(ISNULL([Taper Seg]), 0, [Taper Seg]) FROM [Diario] WHERE Matricula <> 0 AND Of = '" + str(_of) + "' ") 

            for _id, _matricula, _data, _segundos in myCursorNono.fetchall():

                if _segundos > 0:
                    total_seconds = _segundos
                    durada = datetime.timedelta(seconds=total_seconds)
                    hours = durada.days * 24 + durada.seconds // 3600
                    remaining_seconds = durada.seconds % 3600
                    minutes = remaining_seconds // 60
                    seconds = remaining_seconds % 60

                    # We need to get the worker GUID using the matricula.
                    _glam_id, _dni = get_value_from_database_helper(myCursor, 'Treballadors ERP GF', 'Sesame/Sage', str(_matricula))
                    if _glam_id is None: 
                        message = 'Matricula/code not found on the helper column of ERPIntegration. CHECK WHY: ' + str(_matricula)
                        save_log_database(dbOrigin, myCursor, "ERPProductionOrdersMaintenance", message, "ERROR")
                        logging.error(message)
                        continue # if not found, this worker is not used. Next!

                    workerTimes[_of].append(
                    {    
                        "workerId": str(_glam_id).strip(), 
                        "startDate": _data.strftime("%Y-%m-%dT%H:%M:%S"),
                        "totalTime": str(hours).zfill(2).strip() + ":" + str(minutes).zfill(2).strip() + ":" + str(seconds).zfill(2).strip(),
                        "correlationId": str(_id).strip() # row number in the access
                    })

                    total_seconds = _duration
                    durada = datetime.timedelta(seconds=total_seconds)
                    hours = durada.days * 24 + durada.seconds // 3600
                    remaining_seconds = durada.seconds % 3600
                    minutes = remaining_seconds // 60
                    seconds = remaining_seconds % 60

                data={
                    "queueType": "PRODUCTIONORDERS_PRODUCTIONORDERS_NONO",
                    "documentNumber": "OF/" + str(_of).strip(),
                    "startDate": _fechaPrevista.strftime("%Y-%m-%dT%H:%M:%S"),
                    "endDate": "2024-12-31T00:00:00", # TO_DO TODO FELIX Valor provisional darrer dia any 2024
                    "productId": GLAMSUITE_DEFAULT_PRODUCT_ID,
                    "processSheetId": GLAMSUITE_DEFAULT_PROCESS_SHEET_ID,
                    "quantity": "1",
                    "name": str(name).strip(),
                    "description": str(_descripcion).strip(),
                    "duration": str(hours).zfill(2).strip() + ":" + str(minutes).zfill(2).strip() + ":" + str(seconds).zfill(2).strip(),
                    "securityMargin": "00:10:00", # 10 minuts
                    "startTime": _fechaPrevista.strftime("%Y-%m-%dT%H:%M:%S"),
                    "endTime": "2024-12-31T00:00:00", # TO_DO TODO FELIX Valor provisional darrer dia any 2024
                    "routingOperationId": str(routingOperationId).strip(),
                    "warehouseId": str(warehouseId).strip(),
                    "workerTimes": workerTimes.get(_of, []),                
                    "correlationId": "OF/" + str(_of).strip()
                }

                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                glam_id, old_data_hash = get_value_from_database(myCursor, str(_of).strip(), URL_PRODUCTIONORDERS, "Production Orders ERP GF", "Access-Nono")

                if glam_id is None or str(old_data_hash) != str(data_hash):

                    logging.info('      Processing production order ' + str(_of).strip() + ' ...') 

                    # Sending message to queue
                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                    j += 1

                i += 1
                if i % 1000 == 0:
                    logging.info('      ' + str(i) + ' synchronized production orders...')
                    
        logging.info('      Total synchronized production orders: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')        

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        message = '   Unexpected error when processing production orders from original ERP (Access-Nono): ' + str(e)
        save_log_database(dbOrigin, myCursor, "ERPProductionOrdersMaintenance", message, "ERROR")
        logging.error(message)
        send_email("ERPProductionOrdersMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbNono)
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPProductionOrdersMaintenance'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Production Orders Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (MySQL)
    db = None
    try:
        db = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        myCursor = db.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL database: ' + str(e))
        send_email("ERPProductionOrdersMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(db)
        sys.exit(1)

    # connecting to origin database (Nono - Access)
    dbNono = None
    try:
        dbNono = connectAccess(ACCESS_NONO)
        myCursorNono = dbNono.cursor()
    except Exception as e:
        message = '   Unexpected error when connecting to Nono Access database: ' + str(e)
        save_log_database(db, myCursor, "ERPProductionOrdersMaintenance", message, "ERROR")
        logging.error(message)
        send_email("ERPProductionOrdersMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectAccess(dbNono)
        sys.exit(1)

    synchronize_productionOrders(dbNono, myCursorNono, now, db, myCursor)    

    # Send email with execution summary
    send_email("ERPProductionOrdersMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Production Orders Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing databases
    myCursorNono.close()
    dbNono.close()
    myCursor.close()
    db.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()