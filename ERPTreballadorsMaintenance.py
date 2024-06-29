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
from utils import send_email, connectSQLServer, disconnectSQLServer, connectMySQL, disconnectMySQL
import os

# End points URLs
URL_WORKERS = '/workers'

# FELIX-IMPORTANT - API Sesame at https://apidocs.sesametime.com/    (with region "eu2")
URL_EMPLOYEES_SESAME = "/core/v3/employees"
URL_API_SESAME = os.environ['URL_API_SESAME']
TOKEN_API_SESAME = os.environ['TOKEN_API_SESAME']

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']

GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID']
GLAMSUITE_DEFAULT_CALENDAR_ID = os.environ['GLAMSUITE_DEFAULT_CALENDAR_ID']
GLAMSUITE_DEFAULT_ZONE_EPI_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_EPI_ID'] 
GLAMSUITE_DEFAULT_WORKFORCE_ID = os.environ['GLAMSUITE_DEFAULT_WORKFORCE_ID']
GLAMSUITE_DEFAULT_TIMETABLE_ID = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID']

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

# Other constants
CONN_TIMEOUT = 50
NUM_WEEKLY_WORK_HOURS = 40
NUM_WEEKS_YEAR = 46 

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
def get_value_from_database_helper(mycursor, endPoint, origin, correlationId):
    mycursor.execute("SELECT erpGFId, helper FROM ERP_GF.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND deploy = " + str(ENVIRONMENT) + " AND correlationId = '" + str(correlationId) + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    helper = None
    for x in myresult:
        erpGFId = str(x[0])
        helper = str(x[1])

    return erpGFId, helper

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

def synchronize_workers(dbSage, myCursorSage, now, myCursor):
    logging.info('   Processing workers from origin ERP (Sesame/Sage)')

    # processing workers from origin ERP (Sesame)
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

                _workforce = data["jobChargeName"]
                if _workforce is None:
                    logging.error('Worker without jobChargeName populated: ' + data["nid"])
                    continue # if not populated, this worker is not used. Next!
                else:
                    # We need to get the department name using the workforce.
                    glam_id, _dept = get_value_from_database_helper(myCursor, 'Recursos Humans ERP GF', 'Sesame', _workforce)
                    if glam_id is None: 
                        logging.warning('Workforce not found on the correlationId column of ERPIntegration: ' + str(_workforce))
                        continue # if not found, this worker is not used. Next!

                dni = data["nid"]
                name = data["firstName"] + " " + data["lastName"]
                address = data["address"]
                postalCode = data["postalCode"]
                city = data["city"]
                region = data["province"]
                country_code = data["country"]
                iban = data["accountNumber"]
                costs = {} 
                contractNumber = ""
                contractTypeId = 0
                startDate = ""
                endDate = ""
                annualWorkingHours = 0

                myCursorSage.execute("SELECT en.idEmpleado, " \
                                     "en.codigoEmpleado, " \
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
                                     "p.segundoApellidoEmpleado, " \
                                     "p.nombreEmpleado " \
                                     "FROM [GARCIAFAURA].dbo.EmpleadoNomina en " \
                                     "INNER JOIN [GARCIAFAURA].dbo.Personas p ON p.SiglaNacion = en.SiglaNacion AND p.Dni = en.Dni " \
                                     "LEFT JOIN [GARCIAFAURA].dbo.PersonasDomicilios AS pd ON pd.SiglaNacion = p.SiglaNacion AND pd.Dni = p.Dni " \
                                     "LEFT JOIN [GARCIAFAURA].dbo.empleadoCobro ec ON ec.codigoEmpresa = en.codigoEmpresa AND ec.idEmpleado = en.idEmpleado " \
                                     "LEFT JOIN [GARCIAFAURA].dbo.contrato c ON c.codigoContrato = en.codigoContrato AND c.SubCodigoContrato = en.SubCodigoContrato " \
                                     "WHERE pd.CodigoDireccionPersona IN ('PAR','FIS') " \
                                     "AND en.FechaBaja IS NULL " \
                                     "AND p.dni = '" + str(dni).strip() + "'"
                                     "ORDER BY ec.porcentaje DESC, pd.CodigoDireccionPersona DESC ")
                record = myCursorSage.fetchone()   
            
                if record is not None:           
                    idEmpleado = str(record[0]).strip()
                    codEmpleado = str(record[1]).strip()
                    if record[2] is not None:
                        address = record[2].strip()
                    if record[3] is not None:
                        postalCode = record[3].strip()
                    if record[4] is not None:
                        city = record[4].strip()
                    if record[5] is not None:
                        region = record[5].strip()
                    if record[6] is not None:
                        iban = record[6].strip()
                    if record[7] is not None and record[8] is not None:
                        contractNumber = (str(record[7]) + "/" + str(record[8])).strip()
                        contractTypeId = 1 # Contracte indefinit
                        if contractNumber[0:1] == "4" or contractNumber[0:1] == "5":
                            contractTypeId = 2 # Contracte temporal
                    if record[9] is not None:
                        startDate = record[9].strftime("%Y-%m-%dT%H:%M:%SZ")
                    if record[10] is not None:
                        endDate = record[10].strftime("%Y-%m-%dT%H:%M:%SZ")
                    if record[11] is not None:
                        annualWorkingHours = float((record[11] * NUM_WEEKLY_WORK_HOURS / 100) * NUM_WEEKS_YEAR)
                    if record[12] is not None:                            
                        country_code = record[12]

                    primerApellidoEmpleado = record[13]
                    segundoApellidoEmpleado = record[14]
                    nombreEmpleado = record[15]
                    name = nombreEmpleado.strip()
                    if primerApellidoEmpleado.strip() != "":
                        name = name + ' ' + primerApellidoEmpleado.strip()
                    if segundoApellidoEmpleado.strip() != "":
                        name = name + ' ' + segundoApellidoEmpleado.strip()

                    myCursorSage.execute("SELECT year, SUM(anualSalary) AS annualGrossSalary, SUM(anualSocialContribution) AS annualSocialSecurityContribution, 0 AS annualOtherExpenses FROM ( " \
                                         "  SELECT YEAR(fechacobro) AS year, SUM(importenom) AS anualSalary, 0 AS anualSocialContribution " \
                                         "  FROM [GARCIAFAURA].dbo.historico " \
                                         "  WHERE idEmpleado = '" + idEmpleado + "' " \
                                         "  AND codigoconceptonom NOT IN (838, 839, 840, 862, 963) " \
                                         "  AND YEAR(fechaCobro) < YEAR(GETDATE()) " \
                                         "  GROUP BY YEAR(fechaCobro) " \
                                         "    UNION " \
                                         "  SELECT YEAR(fechacobro) AS year, 0 AS anualSalary, SUM(importenom) AS anualSocialContribution " \
                                         "  FROM [GARCIAFAURA].dbo.historico " \
                                         "  WHERE idEmpleado = '" + idEmpleado + "' " \
                                         "  AND codigoconceptonom IN (838, 839, 840, 862, 963) " \
                                         "  AND YEAR(fechaCobro) < YEAR(GETDATE()) " \
                                         "  GROUP BY YEAR(fechaCobro)) t " \
                                         " GROUP BY year " \
                                         "ORDER BY year ")

                    for _year, _annualGrossSalary, _annualSocialSecurityContribution, _annualOtherExpenses in myCursorSage.fetchall():
                        if dni not in costs:
                            costs[dni] = []    

                        _input = str(_year) + "/12/31"
                        _format = '%Y/%m/%d'    
                        _datetime = datetime.datetime.strptime(_input, _format)                                        
                        costs[dni].append(
                        {   
                            "date": _datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),   
                            "annualGrossSalary": float(_annualGrossSalary),
                            "annualSocialSecurityContribution": float(_annualSocialSecurityContribution),
                            "annualOtherExpenses": float(_annualOtherExpenses),
                            "correlationId": str(dni).strip()
                        }
                    )
                else:
                    logging.error('      Treballador no trobat a SAGE: ' + str(dni).strip() + ' ...') 
            
                if address is None:
                    address = " "
                if postalCode is None:
                    postalCode = " "
                if city is None:
                    city = " "
                if region is None:
                    region = " "
                if iban is None:
                    iban = " "
                if country_code is None:
                    logging.error('      Treballador no té país informat. Mirar per què: ' + str(dni).strip() + ' ...') 
                    continue # if not populated, this worker is not used. Next!

                linkedInProfile = " "

                if endDate == "":
                    dataContract={
                        "contractNumber": contractNumber,
                        "contractTypeId": contractTypeId,
                        "startDate": startDate,
                        "departmentId": str(_dept),
                        "workforceId": str(_workforce),
                        "calendarId": str(GLAMSUITE_DEFAULT_CALENDAR_ID),
                        "annualWorkingHours": annualWorkingHours,
                        "timetableId": GLAMSUITE_DEFAULT_TIMETABLE_ID,
                        "correlationId": str(dni).strip()
                    }     
                else:
                    dataContract={
                        "contractNumber": contractNumber,
                        "contractTypeId": contractTypeId,
                        "startDate": startDate,
                        "endDate": endDate,
                        "departmentId": str(_dept),
                        "workforceId": str(_workforce),
                        "calendarId": str(GLAMSUITE_DEFAULT_CALENDAR_ID),
                        "annualWorkingHours": annualWorkingHours,
                        "timetableId": GLAMSUITE_DEFAULT_TIMETABLE_ID,
                        "correlationId": str(dni).strip()
                    }     

                dataLocation={
                    "correlationId": str(dni),
                    "zoneId": str(GLAMSUITE_DEFAULT_ZONE_EPI_ID),
                    "containerTypeId": str(GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID),
                    "containerCode": "EPI" + str(codEmpleado).strip(),
                    "description": str(name).strip(),
                    "position": str(codEmpleado).strip(),
                    "preferential": False
                }                             

                data={
                    "queueType": "TREBALLADORS_TREBALLADORS",
                    "name": str(name).strip(),
                    "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
                    "nationality": country_code,
                    "identificationTypeId": 1, # NIF = 1, NIE = 2, TIE = 3, Passport = 4, Others = 5
                    "identificationNumber": str(dni).strip(),
                    "address": address,
                    "postalCode": postalCode,
                    "city": city,
                    "region": region,
                    "countryId": country_code,
                    "linkedInProfile": linkedInProfile,
                    "iban": iban, 
                    "costs": costs.get(dni, []),
                    "correlationId": str(dni).strip(),
                    "dataContract": dataContract,
                    "dataLocation": dataLocation,
                }

                #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                glam_id, old_data_hash = get_value_from_database(myCursor, str(dni).strip(), URL_WORKERS, "Treballadors ERP GF", "Sesame/Sage")

                if glam_id is None or str(old_data_hash) != str(data_hash):

                    logging.info('      Processing user ' + str(name).strip() + ' del departament ' + str(_dept).strip() + ' ...') 

                    # Sending message to queue
                    myRabbitPublisherService.publish_message(json.dumps(data)) # Faig un json.dumps per convertir de diccionari a String

                    j += 1

                i += 1
                if i % 1000 == 0:
                    logging.info('      ' + str(i) + ' synchronized workers...')   

            meta = response["meta"]
            if str(meta["lastPage"]) == str(page):
                endProcess = True
            else:
                page = page + 1

        logging.info('      Total synchronized workers: ' + str(i) + '. Total differences sent to rabbit: ' + str(j) + '.')           

        # Closing queue
        myRabbitPublisherService.close()

    except Exception as e:
        logging.error('   Unexpected error when processing workers from original ERP (Sesame/Sage): ' + str(e))
        send_email("ERPTreballadorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
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

    synchronize_workers(dbSage, myCursorSage, now, myCursor)    

    # Send email with execution summary
    send_email("ERPTreballadorsMaintenance", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Treballadors Maintenance - ENVIRONMENT: ' + str(ENVIRONMENT))
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