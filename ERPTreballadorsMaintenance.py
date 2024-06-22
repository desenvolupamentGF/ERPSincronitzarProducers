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
URL_DEPARTMENTS = '/departments'
URL_WORKERS = '/workers'

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']

GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID']
GLAMSUITE_DEFAULT_CALENDAR_ID = os.environ['GLAMSUITE_DEFAULT_CALENDAR_ID']
GLAMSUITE_DEFAULT_ZONE_EPI_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_EPI_ID'] 
GLAMSUITE_DEFAULT_WORKFORCE_ID = os.environ['GLAMSUITE_DEFAULT_WORKFORCE_ID']

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

BIOSTAR_SQLSERVER_USER = os.environ['BIOSTAR_SQLSERVER_USER']
BIOSTAR_SQLSERVER_PASSWORD = os.environ['BIOSTAR_SQLSERVER_PASSWORD']
BIOSTAR_SQLSERVER_HOST = os.environ['BIOSTAR_SQLSERVER_HOST']
BIOSTAR_SQLSERVER_DATABASE = os.environ['BIOSTAR_SQLSERVER_DATABASE']

# Other constants
NUM_WEEKLY_WORK_HOURS = 40

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

def synchronize_departments(dbBiostar, myCursorBiostar, now, myCursor):
    logging.info('   Processing departments from origin ERP (Biostar)')

    # processing departments from origin ERP (Biostar)
    try:
        # loop over the departments
        myCursorBiostar.execute("SELECT nDepartmentIdn, sName FROM [BioStar].dbo.tb_user_dept ") 

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _code, _name in myCursorBiostar.fetchall():

            data={
                "queueType": "TREBALLADORS_DEPARTAMENTS",
                "name": str(_name).strip(),
                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                "calendarId": GLAMSUITE_DEFAULT_CALENDAR_ID,
                "correlationId": str(_code).strip()
            }

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(myCursor, str(_code).strip(), URL_DEPARTMENTS, "Treballadors ERP GF", "Biostar")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing department ' + str(_code).strip() + ' / ' + str(_name).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized departments...')
        logging.info('      Total synchronized departments: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')                       

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing departments from original ERP (Biostar): ' + str(e))
        send_email("ERPTreballadorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbBiostar)
        sys.exit(1)

def synchronize_workers(dbBiostar, dbSage, myCursorBiostar, myCursorSage, now, myCursor):
    logging.info('   Processing workers from origin ERP (Biostar)')

    # processing workers from origin ERP (Biostar)
    try:
        # loop over the workers
        # No agafem del departament 28 (no actius)
        # Ni ETTs ni Becaris
        myCursorBiostar.execute("SELECT nDepartmentIdn, sUserID, sUserName, sEmail as dni " \
                                "FROM [BioStar].dbo.tb_user " \
                                "WHERE nDepartmentIdn NOT IN (28) " \
                                "AND sUserName NOT LIKE 'ETT %' " \
                                "AND sUserName NOT LIKE 'BEC %' " \
                                "AND sUserName NOT LIKE 'New User%' " \
                                "AND sUserName NOT IN ('admin','DEV','AUXILIAR IT','AUXILIAR COMPRAS','AUX COMERCIAL') ")

        # Preparing message queue
        myRabbitPublisherService = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

        i = 0
        j = 0
        for _dept, _code, _name, _dni in myCursorBiostar.fetchall():

            myCursorSage.execute("SELECT en.idEmpleado, " \
                                 "p.DNI, " \
                                 "RTRIM(pd.CodigoSigla + ' ' + LTRIM(pd.ViaPublica + ' ') + LTRIM(pd.Numero1 + ' ') + LTRIM(pd.Numero2 + ' ') + LTRIM(pd.Escalera + ' ') + LTRIM(pd.Piso + ' ') + LTRIM(pd.Puerta + ' ') + LTRIM(pd.Letra)) AS direccion, " \
                                 "pd.CodigoPostal, " \
                                 "pd.Municipio, " \
                                 "pd.Provincia, " \
                                 "ec.ibanReceptor, " \
                                 "c.codigoContrato, " \
                                 "c.subCodigoContrato, " \
                                 "en.fechaInicioContrato, " \
                                 "en.fechaFinalContrato, " \
                                 "en.porJornada AS porcentajeJornada, " \
                                 "p.SiglaNacion, " \
                                 "p.primerApellidoEmpleado, " \
                                 "p.segundoApellidoEmpleado, "\
                                 "p.nombreEmpleado "       
                                 "FROM [GARCIAFAURA].dbo.EmpleadoNomina en " \
                                 "INNER JOIN [GARCIAFAURA].dbo.Personas p ON p.SiglaNacion = en.SiglaNacion AND p.Dni = en.Dni " \
                                 "INNER JOIN [GARCIAFAURA].dbo.PersonasDomicilios AS pd ON pd.SiglaNacion = p.SiglaNacion AND pd.Dni = p.Dni " \
                                 "INNER JOIN [GARCIAFAURA].dbo.empleadoCobro ec ON ec.codigoEmpresa = en.codigoEmpresa AND ec.idEmpleado = en.idEmpleado " \
                                 "INNER JOIN [GARCIAFAURA].dbo.contrato c ON c.codigoContrato = en.codigoContrato AND c.SubCodigoContrato = en.SubCodigoContrato " \
                                 "WHERE pd.CodigoDireccionPersona IN ('PAR','FIS') " \
                                 "AND en.FechaBaja IS NULL " \
                                 "AND p.dni = '" + str(_dni).strip() + "'"
                                 "ORDER BY ec.porcentaje DESC, pd.CodigoDireccionPersona DESC ")
            record = myCursorSage.fetchone()   

            nif = str(_code).strip()
            address = ""
            postalCode = ""
            city = ""
            region = ""
            iban = ""
            costs = {} 
            departmentId = _dept  
            contractNumber = ""
            contractTypeId = 0
            startDate = ""
            endDate = ""
            workingHours = 0
            
            if record is not None:           
                idEmpleado = str(record[0]).strip()
                nif = record[1].strip()
                address = record[2].strip()
                postalCode = record[3].strip()
                city = record[4].strip()
                region = record[5].strip()
                iban = record[6].strip()
                contractNumber = (str(record[7]) + "/" + str(record[8])).strip()
                contractTypeId = 1 # Contracte indefinit
                if contractNumber[0:1] == "4" or contractNumber[0:1] == "5":
                    contractTypeId = 2 # Contracte temporal
                if record[9] is not None:
                    startDate = record[9].strftime("%Y-%m-%dT%H:%M:%SZ")
                if record[10] is not None:
                    endDate = record[9].strftime("%Y-%m-%dT%H:%M:%SZ")
                workingHours = float(record[11] * NUM_WEEKLY_WORK_HOURS / 100)    
                country_code = record[12]

                primerApellidoEmpleado = record[13]
                segundoApellidoEmpleado = record[14]
                nombreEmpleado = record[15]
                _name = nombreEmpleado.strip()
                if primerApellidoEmpleado.strip() != "":
                    _name = _name + ' ' + primerApellidoEmpleado.strip()
                if segundoApellidoEmpleado.strip() != "":
                    _name = _name + ' ' + segundoApellidoEmpleado.strip()

                myCursorSage.execute("SELECT YEAR(fechacobro), SUM(importenom) " \
                                     "FROM [GARCIAFAURA].dbo.historico " \
                                     "WHERE idEmpleado = '" + idEmpleado + "' " \
                                     "AND codigoconceptonom NOT IN (838, 839, 840, 862, 963) " \
                                     "AND YEAR(fechaCobro) < YEAR(GETDATE()) " \
                                     "GROUP BY YEAR(fechaCobro) " \
                                     "ORDER BY YEAR(fechaCobro) ")

                for _year, _cost in myCursorSage.fetchall():
                    if _code not in costs:
                        costs[_code] = []                    
                    costs[_code].append(
                    {
                        "exercise": _year,   
                        "cost": float(_cost)
                    }
                )

            else:
                if _dept == 38: # treballadors colombians és normal no trobar-los a sage --> escric warning enlloc d'error
                    logging.warning('      Treballador departament COLOMBIA no trobat a SAGE: ' + str(_name).strip() + ' ...') 
                    country_code = 'CO'
                else:
                    logging.error('      Treballador no trobat a SAGE: ' + str(_name).strip() + ' ...') 
            
            address = address.ljust(1, ' ')
            postalCode = postalCode.ljust(1, ' ')
            city = city.ljust(1, ' ')
            region = region.ljust(1, ' ')
            iban = iban.ljust(1, ' ')

            linkedInProfile = " "

            if endDate == "":
                dataContract={
                    "contractNumber": contractNumber,
                    "contractTypeId": contractTypeId,
                    "startDate": startDate,
                    "departmentId": departmentId,
                    "workforceId": str(GLAMSUITE_DEFAULT_WORKFORCE_ID),
                    "workingHours": workingHours,
                    "correlationId": str(_code).strip()
                }     
            else:
                dataContract={
                    "contractNumber": contractNumber,
                    "contractTypeId": contractTypeId,
                    "startDate": startDate,
                    "endDate": endDate,
                    "departmentId": departmentId,
                    "workforceId": str(GLAMSUITE_DEFAULT_WORKFORCE_ID),
                    "workingHours": workingHours,
                    "correlationId": str(_code).strip()
                }     

            dataLocation={
                "correlationId": str(_code),
                "zoneId": str(GLAMSUITE_DEFAULT_ZONE_EPI_ID),
                "containerTypeId": str(GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID),
                "containerCode": "EPI" + str(_code).strip(),
                "description": str(_name).strip(),
                "position": str(_code).strip(),
                "preferential": False
            }                          

            data={
                "queueType": "TREBALLADORS_TREBALLADORS",
                "name": str(_name).strip(),
                "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                "nationality": country_code,
                "identificationTypeId": 1, # NIF = 1, NIE = 2, TIE = 3, Passport = 4, Others = 5
                "identificationNumber": str(nif).strip(),
                "address": address,
                "postalCode": postalCode,
                "city": city,
                "region": region,
                "countryId": country_code,
                "linkedInProfile": linkedInProfile,
                "iban": iban, 
                "costs": costs.get(_code, []),
                "correlationId": str(_code).strip(),
                "dataContract": dataContract,
                "dataLocation": dataLocation,
            }

            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            glam_id, old_data_hash = get_value_from_database(myCursor, str(_code).strip(), URL_WORKERS, "Treballadors ERP GF", "Biostar")

            if glam_id is None or str(old_data_hash) != str(data_hash):

                logging.info('      Processing user ' + str(_name).strip() + ' del departament ' + str(_dept).strip() + ' ...') 

                # Sending message to queue
                myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                j += 1

            i += 1
            if i % 1000 == 0:
                logging.info('      ' + str(i) + ' synchronized workers...')    
        logging.info('      Total synchronized workers: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing workers from original ERP (Biostar): ' + str(e))
        send_email("ERPTreballadorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbBiostar)
        disconnectSQLServer(dbSage)
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_ERPTreballadorsMaintenance'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Treballadors Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (MySQL)
    db = None
    try:
        db = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        myCursor = db.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL database: ' + str(e))
        send_email("ERPUsersMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(db)
        sys.exit(1)

    # connecting to Biostar database (SQLServer)
    dbBiostar = None
    try:
        dbBiostar = connectSQLServer(BIOSTAR_SQLSERVER_USER, BIOSTAR_SQLSERVER_PASSWORD, BIOSTAR_SQLSERVER_HOST, BIOSTAR_SQLSERVER_DATABASE)
        myCursorBiostar = dbBiostar.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to SQLServer Biostar database: ' + str(e))
        send_email("ERPTreballadorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbBiostar)
        sys.exit(1)

    # connecting to Sage database (SQLServer)
    dbSage = None
    try:
        dbSage = connectSQLServer(SAGE_SQLSERVER_USER, SAGE_SQLSERVER_PASSWORD, SAGE_SQLSERVER_HOST, SAGE_SQLSERVER_DATABASE)
        myCursorSage = dbSage.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to SQLServer Sage database: ' + str(e))
        send_email("ERPTreballadorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbSage)
        sys.exit(1)

    synchronize_departments(dbBiostar, myCursorBiostar, now, myCursor)    
    synchronize_workers(dbBiostar, dbSage, myCursorBiostar, myCursorSage, now, myCursor)    

    # Send email with execution summary
    send_email("ERPTreballadorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Treballadors Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing databases
    db.close()
    myCursor.close()
    myCursorBiostar.close()
    dbBiostar.close()
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