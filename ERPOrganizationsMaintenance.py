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
from utils import send_email, connectSQLServer, disconnectSQLServer, connectMySQL, disconnectMySQL
import os

# End points URLs
URL_PAYMENTMETHODS = '/paymentMethods'
URL_ORGANIZATIONS = '/organizations'
URL_PROJECTS = '/projects'

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']
GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID']

GLAMSUITE_DEFAULT_LANGUAGE_CATALA = os.environ['GLAMSUITE_DEFAULT_LANGUAGE_CATALA']
GLAMSUITE_DEFAULT_CURRENCY_EURO_ID = os.environ['GLAMSUITE_DEFAULT_CURRENCY_EURO_ID']
GLAMSUITE_DEFAULT_INVOICING_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_INVOICING_TYPE_ID']
GLAMSUITE_DEFAULT_WAREHOUSE_ID = os.environ['GLAMSUITE_DEFAULT_WAREHOUSE_ID']
GLAMSUITE_DEFAULT_CARRIER_ID = os.environ['GLAMSUITE_DEFAULT_CARRIER_ID']
GLAMSUITE_DEFAULT_INCOTERM_ID = os.environ['GLAMSUITE_DEFAULT_INCOTERM_ID']
GLAMSUITE_DEFAULT_RATE_ID_CLIENT = os.environ['GLAMSUITE_DEFAULT_RATE_ID_CLIENT']
GLAMSUITE_DEFAULT_RATE_ID_PROVEIDOR = os.environ['GLAMSUITE_DEFAULT_RATE_ID_PROVEIDOR']

GLAMSUITE_DEFAULT_DOCUMENT_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_DOCUMENT_TYPE_ID']
GLAMSUITE_DEFAULT_ORGANIZATION_ID = os.environ['GLAMSUITE_DEFAULT_ORGANIZATION_ID']

# Rabbit constants for messaging
RABBIT_URL = os.environ['RABBIT_URL']
RABBIT_PORT = os.environ['RABBIT_PORT']
RABBIT_QUEUE = os.environ['RABBIT_QUEUE']

# Database constants
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']

SAGE_SQLSERVER_USER = os.environ['SAGE_SQLSERVER_USER']
SAGE_SQLSERVER_PASSWORD = os.environ['SAGE_SQLSERVER_PASSWORD']
SAGE_SQLSERVER_HOST = os.environ['SAGE_SQLSERVER_HOST']
SAGE_SQLSERVER_DATABASE = os.environ['SAGE_SQLSERVER_DATABASE']

TEOWIN_SQLSERVER_USER = os.environ['TEOWIN_SQLSERVER_USER']
TEOWIN_SQLSERVER_PASSWORD = os.environ['TEOWIN_SQLSERVER_PASSWORD']
TEOWIN_SQLSERVER_HOST = os.environ['TEOWIN_SQLSERVER_HOST']
TEOWIN_SQLSERVER_DATABASE = os.environ['TEOWIN_SQLSERVER_DATABASE']

# Other constants
YEARS_TO_RECALCULATE = 3

def save_log_database(dbOrigin, mycursor, endPoint, message, typeLog):
    sql = "INSERT INTO ERP_GF.ERPIntegrationLog (dateLog, companyId, endpoint, deploy, message, typeLog) VALUES (NOW(), %s, %s, %s, %s, %s) "
    val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), str(endPoint), str(ENVIRONMENT), str(message), str(typeLog))
    mycursor.execute(sql, val)
    dbOrigin.commit()  

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

def synchronize_paymentMethods(dbSage, myCursorSage, now, dbOrigin, myCursor):
    logging.info('   Processing payment methods from origin ERP (Sage)')

    # processing payment methods from origin ERP (Sage)
    try:
        # loop over the families
        myCursorSage.execute("SELECT codigoCondiciones, condiciones FROM [GARCIAFAURA].dbo.CondicionesPlazos WHERE codigoEmpresa=1")

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _code, _description in myCursorSage.fetchall():
            data={
                "queueType": "ORGANIZATIONS_PAYMENTMETHODS",
                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                "ibanPrintMethodId": 1,
                "code": str(_code).strip(),
                "name": str(_description).strip(),
                "correlationId": str(_code).strip(),
            }

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(myCursor, str(_code).strip(), URL_PAYMENTMETHODS, "Organizations ERP GF", "Sage")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing payment method ' + str(_code).strip() + ' / ' + str(_description).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized payment methods...')
        logging.info('      Total synchronized payment methods: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')               

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        message = '   Unexpected error when processing payment methods from original ERP (Sage): ' + str(e)
        save_log_database(dbOrigin, myCursor, "ERPOrganizationsMaintenance", message, "ERROR")
        logging.error(message)
        send_email("ERPOrganizationsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbSage)
        sys.exit(1)

def synchronize_organizations(dbSage, myCursorSage, now, dbOrigin, myCursor):
    logging.info('   Processing proveïdors i clients from origin ERP (Sage)')

    # processing proveïdors i clients from origin ERP (Sage)
    try:
        # loop over the proveïdors UNION loop over the clients GROUP BY cif
        # we do this UNION and GROUP BY to avoid duplicating organizations
        myCursorSage.execute("SELECT MAX(nomProveedor) as nomProveedor, MAX(nomCliente) as nomCliente, CIF, MAX(accountProveedor) as accountProveedor, MAX(accountCliente) as accountCliente, MAX(codNacion) as codNacion, " \
                             "       MAX(nameP) as nameP, MAX(addressP) as addressP, MAX(postalCodeP) as postalCodeP, MAX(cityP) as cityP, MAX(regionP) as regionP, MAX(phoneP) as phoneP, MAX(fechaBajaP) as fechaBajaP, " \
                             "       MAX(nameC) as nameC, MAX(addressC) as addressC, MAX(postalCodeC) as postalCodeC, MAX(cityC) as cityC, MAX(regionC) as regionC, MAX(phoneC) as phoneC, MAX(fechaBajaC) as fechaBajaC, " \
                             "       MAX(codigoCondicionesP) as codigoCondicionesP, MAX(descuentoP) as descuentoP, MAX(prontoPagoP) as prontoPagoP, MAX(financiacionP) as financiacionP, " \
                             "       MAX(codigoCondicionesC) as codigoCondicionesC, MAX(descuentoC) as descuentoC, MAX(prontoPagoC) as prontoPagoC, MAX(financiacionC) as financiacionC, " \
                             "       MAX(riesgoMaximoC) as riesgoMaximoC FROM (" \
                             "  SELECT p.nombre as nomProveedor, null as nomCliente, p.cifdni as CIF, p.codigoContable as accountProveedor, null as accountCliente, n.nacionIso as codNacion, " \
                             "         p.nombre as nameP, p.domicilio as addressP, p.codigoPostal as postalCodeP, p.municipio as cityP, p.provincia as regionP, p.telefono as phoneP, p.fechabajalc as fechaBajaP, " \
                             "         null as nameC, null as addressC, null as postalCodeC, null as cityC, null as regionC, null as phoneC, null as fechaBajaC, " \
                             "         p.[codigoCondiciones] as codigoCondicionesP, p.[%descuento] as descuentoP, p.[%prontoPago] as prontoPagoP, p.[%financiacion] as financiacionP, " \
                             "         null as codigoCondicionesC, null as descuentoC, null as prontoPagoC, null as financiacionC, " \
                             "         null as riesgoMaximoC " \
                             "  FROM [GARCIAFAURA].dbo.Proveedores p, [GARCIAFAURA].dbo.Naciones n " \
                             "  WHERE p.codigoNacion = n.codigoNacion " \
                             "  AND p.codigoEmpresa = 1 " \
                             "  AND p.cifdni <> '' " \
                             "  AND (p.codigoContable LIKE '400%' OR p.codigoContable LIKE '410%') " \
                             "  AND LEN(p.codigoContable) = 10 " \
                             "  AND p.nombre <> '' " \
                             "      UNION      " \
                             "  SELECT null as nomProveedor, c.nombre as nomCliente, c.cifdni as CIF, null as accountProveedor, c.codigoContable as accountCliente, n.nacionIso as codNacion, " \
                             "         null as nameP, null as addressP, null as postalCodeP, null as cityP, null as regionP, null as phoneP, null as fechaBajaP, " \
                             "         c.nombre as nameC, c.domicilio as addressC, c.codigoPostal as postalCodeC, c.municipio as cityC, c.provincia as regionC, c.telefono as phoneC, c.fechabajalc as fechaBajaC, " \
                             "         null as codigoCondicionesP, null as descuentoP, null as prontoPagoP, null as financiacionP, " \
                             "         c.[codigoCondiciones] as codigoCondicionesC, c.[%descuento] as descuentoC, c.[%prontoPago] as prontoPagoC, c.[%financiacion] as financiacionC, " \
                             "         c.riesgoMaximo as riesgoMaximoC "
                             "  FROM [GARCIAFAURA].dbo.Clientes c, [GARCIAFAURA].dbo.Naciones n " \
                             "  WHERE c.codigoNacion = n.codigoNacion " \
                             "  AND c.codigoEmpresa = 1 " \
                             "  AND c.codigocategoriacliente_ = 'CLI' " \
                             "  AND c.cifdni <> '' " \
                             "  AND c.codigoContable LIKE '430%' " \
                             "  AND LEN(c.codigoContable) = 10 " \
                             "  AND c.nombre <> '' " \
                             ") proveedoresMASclientes GROUP BY cif " )

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _nomProveedor, _nomCliente, _CIF, _accountProveedor, _accountCliente, _codNacion, \
            _nameP, _addressP, _postalCodeP, _cityP, _regionP, _phoneP, _fechaBajaP, \
            _nameC, _addressC, _postalCodeC, _cityC, _regionC, _phoneC, _fechaBajaC, \
            _codigoCondicionesP, _descuentoP, _prontoPagoP, _financiacionP, \
            _codigoCondicionesC, _descuentoC, _prontoPagoC, _financiacionC, \
            _riesgoMaximoC in myCursorSage.fetchall():

            if _nomCliente is not None: # If organization is both proveedor and client, we pick one (client)
                nomOrganization = str(_nomCliente).strip()
            else:
                nomOrganization = str(_nomProveedor).strip()

            codigoCondicionesC = str(_codigoCondicionesC).strip()
            if codigoCondicionesC == "0": # No informat
                codigoCondicionesC = "12" # Codi que enviem per defecte (Comptat)
            codigoCondicionesP = str(_codigoCondicionesP).strip()
            if codigoCondicionesP == "0": # No informat
                codigoCondicionesP = "12" # Codi que enviem per defecte (Comptat)

            active = "YES"
            if _nomCliente is not None and _nomProveedor is not None: # If organization is both proveedor and client
                if _fechaBajaC is not None and _fechaBajaP is not None: # Only if both are populated, I will consider the organization not active
                    active = "NO"
            else: # Organization is client or proveedor but not both things
                if _nomCliente is not None and _fechaBajaC is not None:
                    active = "NO"
                if _nomProveedor is not None and _fechaBajaP is not None:
                    active = "NO"

            accountProveedor = ""
            dataProveedor = {}
            if _accountProveedor is not None:
                accountProveedor = str(_accountProveedor).strip()
                dataProveedor={
                    "name": "Domicili Fiscal",
                    "address": str(_addressP).strip(),
                    "postalCode": str(_postalCodeP).strip(),
                    "city": str(_cityP).strip(),
                    "region": str(_regionP).strip(),
                    "phone": str(_phoneP).strip(),
                    "countryId": str(_codNacion).strip(),
                    "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
                    "geolocation": "",
                    "conditionTypeId": 2,
                    "currencyId": GLAMSUITE_DEFAULT_CURRENCY_EURO_ID,
                    "invoicingTypeId": GLAMSUITE_DEFAULT_INVOICING_TYPE_ID,
                    "warehouseId": GLAMSUITE_DEFAULT_WAREHOUSE_ID,
                    "carrierId": GLAMSUITE_DEFAULT_CARRIER_ID,
                    "incotermId": GLAMSUITE_DEFAULT_INCOTERM_ID,
                    "rates": [GLAMSUITE_DEFAULT_RATE_ID_PROVEIDOR],                    
                    "specialDiscount": str(round(_descuentoP, 2)),
                    "paymentInAdvanceDiscount": str(round(_prontoPagoP, 2)),
                    "finantialCost": str(round(_financiacionP, 2)),
                    "shippingDays": 0,
                    "correlationId": str(_CIF).strip()
                }             
                # Get Glam Payment Method id.
                glam_payment_method_id, nothing_to_do = get_value_from_database(myCursor, correlation_id=codigoCondicionesP, url=URL_PAYMENTMETHODS, endPoint="Organizations ERP GF", origin="Sage")
                if glam_payment_method_id is None:
                    message = 'Payment method not found! Check why! (' + str(_CIF) + '/' + str(codigoCondicionesP) + ')'
                    save_log_database(dbOrigin, myCursor, "ERPOrganizationsMaintenance", message, "ERROR")
                    logging.error(message)
                    continue # skip to next organization (should not happen)
                else:
                    dataProveedor["paymentMethodId"] = glam_payment_method_id

            accountCliente = ""
            dataCliente = {}
            if _accountCliente is not None:
                accountCliente = str(_accountCliente).strip()
                dataCliente={
                    "name": "Domicili Fiscal",
                    "address": str(_addressC).strip(),
                    "postalCode": str(_postalCodeC).strip(),
                    "city": str(_cityC).strip(),
                    "region": str(_regionC).strip(),
                    "phone": str(_phoneC).strip(),
                    "countryId": str(_codNacion).strip(),
                    "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
                    "geolocation": "",
                    "conditionTypeId": 1,
                    "currencyId": GLAMSUITE_DEFAULT_CURRENCY_EURO_ID,
                    "invoicingTypeId": GLAMSUITE_DEFAULT_INVOICING_TYPE_ID,
                    "warehouseId": GLAMSUITE_DEFAULT_WAREHOUSE_ID,
                    "carrierId": GLAMSUITE_DEFAULT_CARRIER_ID,
                    "incotermId": GLAMSUITE_DEFAULT_INCOTERM_ID,
                    "rates": [GLAMSUITE_DEFAULT_RATE_ID_CLIENT],
                    "specialDiscount": str(round(_descuentoC, 2)),
                    "paymentInAdvanceDiscount": str(round(_prontoPagoC, 2)),
                    "finantialCost": str(round(_financiacionC, 2)),
                    "shippingDays": 0,
                    "amount": str(round(_riesgoMaximoC, 2)),
                    "insuranceCompany": "CESCE", # Sempre és aquesta pels clients segons Loli Requena
                    "correlationId": str(_CIF).strip()
                }             
                # Get Glam Payment Method id.
                glam_payment_method_id, nothing_to_do = get_value_from_database(myCursor, correlation_id=codigoCondicionesC, url=URL_PAYMENTMETHODS, endPoint="Organizations ERP GF", origin="Sage")
                if glam_payment_method_id is None:
                    message = 'Payment method not found! Check why!'
                    save_log_database(dbOrigin, myCursor, "ERPOrganizationsMaintenance", message, "ERROR")
                    logging.error(message)
                    continue # skip to next organization (should not happen)
                else:
                    dataCliente["paymentMethodId"] = glam_payment_method_id

            data={
                "queueType": "ORGANIZATIONS_ORGANIZATIONS",
                "code": str(_CIF).strip(),
                "legalName": nomOrganization,
                "tradeName": nomOrganization,
                "countryId": str(_codNacion).strip(),
                "identificationType": 
                    {
                        "typeId": 3, 
                        "number": str(_CIF).strip() 
                    }
                ,
                "accountP": accountProveedor,
                "accountC": accountCliente,
                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                "active": active,
                "correlationId": str(_CIF).strip(),
                "dataProveedor": dataProveedor,
                "dataCliente": dataCliente,
            }

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(myCursor, str(_CIF).strip(), URL_ORGANIZATIONS, "Organizations ERP GF", "Sage")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing organization CIF: ' + str(_CIF).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized organizations...')    
        logging.info('      Total synchronized organizations: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        message = '   Unexpected error when processing organizations from original ERP (Sage): ' + str(e)
        save_log_database(dbOrigin, myCursor, "ERPOrganizationsMaintenance", message, "ERROR")
        logging.error(message)
        send_email("ERPOrganizationsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbSage)
        sys.exit(1)

def synchronize_projects(dbTeowin, myCursorTeowin, now, dbOrigin, myCursor):
    logging.info('   Processing projects/obres/ot''s from origin ERP (Teowin)')

    # processing projects/obres/ot's from origin ERP (Teowin)
    try:
        myCursorTeowin.execute("SELECT OT, NomObra, FechaAdjudicacion " \
                               "FROM [GF3D].dbo.tObras " \
                               "WHERE Estado = 'A' OR FechaAdjudicacion > GETDATE() - (365 * " + str(YEARS_TO_RECALCULATE) + ") ")

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _OT, _nomObra, _fechaAdjudicacion in myCursorTeowin.fetchall():

            data={
                "queueType": "ORGANIZATIONS_PROJECTS",
                "code": "OT/" + str(_OT).strip(),
                "name": str(_nomObra).strip(),
                "organizationId": GLAMSUITE_DEFAULT_ORGANIZATION_ID,
                "documentTypeId": GLAMSUITE_DEFAULT_DOCUMENT_TYPE_ID,
                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                "date": _fechaAdjudicacion.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "correlationId": "OT/" + str(_OT).strip(),
            }

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(myCursor, str(_OT).strip(), URL_PROJECTS, "Organizations ERP GF", "Teowin")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing project/Obra/OT: ' + str(_OT).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized projects/obres/OT''s...')    
        logging.info('      Total synchronized projects/obres/OT''s: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        message = '   Unexpected error when processing projects/obres/OT''s from original ERP (Teowin): ' + str(e)
        save_log_database(dbOrigin, myCursor, "ERPOrganizationsMaintenance", message, "ERROR")
        logging.error(message)
        send_email("ERPOrganizationsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbTeowin)
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPOrganizationsMaintenance'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Organizations Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (MySQL)
    db = None
    try:
        db = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        myCursor = db.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL database: ' + str(e))
        send_email("ERPOrganizationsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(db)
        sys.exit(1)

    # connecting to Sage database (SQLServer)
    dbSage = None
    try:
        dbSage = connectSQLServer(SAGE_SQLSERVER_USER, SAGE_SQLSERVER_PASSWORD, SAGE_SQLSERVER_HOST, SAGE_SQLSERVER_DATABASE)
        myCursorSage = dbSage.cursor()
    except Exception as e:
        message = '   Unexpected error when connecting to SQLServer Sage database: ' + str(e)
        save_log_database(db, myCursor, "ERPOrganizationsMaintenance", message, "ERROR")
        logging.error(message)
        send_email("ERPOrganizationsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbSage)
        sys.exit(1)

    # connecting to Teowin database (SQLServer)
    dbTeowin = None
    try:
        dbTeowin = connectSQLServer(TEOWIN_SQLSERVER_USER, TEOWIN_SQLSERVER_PASSWORD, TEOWIN_SQLSERVER_HOST, TEOWIN_SQLSERVER_DATABASE)
        myCursorTeowin = dbTeowin.cursor()
    except Exception as e:
        message = '   Unexpected error when connecting to SQLServer Teowin database: ' + str(e)
        save_log_database(db, myCursor, "ERPOrganizationsMaintenance", message, "ERROR")
        logging.error(message)
        send_email("ERPOrganizationsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbSage)
        sys.exit(1)

    synchronize_paymentMethods(dbSage, myCursorSage, now, db, myCursor)    
    synchronize_organizations(dbSage, myCursorSage, now, db, myCursor)    
    #synchronize_projects(dbTeowin, myCursorTeowin, now, db, myCursor)

    # Send email with execution summary
    send_email("ERPOrganizationsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Organizations Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing databases
    db.close()
    myCursor.close()
    myCursorSage.close()
    dbSage.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()