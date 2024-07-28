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
URL_ABSENCES_SESAME = "/schedule/v1/absence-day-off"
URL_API_SESAME = os.environ['URL_API_SESAME']
TOKEN_API_SESAME = os.environ['TOKEN_API_SESAME']

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']

GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID']
GLAMSUITE_DEFAULT_CALENDAR_ID = os.environ['GLAMSUITE_DEFAULT_CALENDAR_ID']
GLAMSUITE_DEFAULT_ZONE_EPI_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_EPI_ID'] 

GLAMSUITE_DEFAULT_TIMETABLE_ID_8h = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID_8h']
GLAMSUITE_DEFAULT_TIMETABLE_ID_6dot5h = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID_6dot5h']
GLAMSUITE_DEFAULT_TIMETABLE_ID_6h = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID_6h']
GLAMSUITE_DEFAULT_TIMETABLE_ID_5dot6h = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID_5dot6h']
GLAMSUITE_DEFAULT_TIMETABLE_ID_5h = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID_5h']
GLAMSUITE_DEFAULT_TIMETABLE_ID_4dot8h = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID_4dot8h']
GLAMSUITE_DEFAULT_TIMETABLE_ID_4h = os.environ['GLAMSUITE_DEFAULT_TIMETABLE_ID_4h']

GLAMSUITE_DEFAULT_SHIFT_ID_8h = os.environ['GLAMSUITE_DEFAULT_SHIFT_ID_8h']
GLAMSUITE_DEFAULT_SHIFT_ID_6dot5h = os.environ['GLAMSUITE_DEFAULT_SHIFT_ID_6dot5h']
GLAMSUITE_DEFAULT_SHIFT_ID_6h = os.environ['GLAMSUITE_DEFAULT_SHIFT_ID_6h']
GLAMSUITE_DEFAULT_SHIFT_ID_5dot6h = os.environ['GLAMSUITE_DEFAULT_SHIFT_ID_5dot6h']
GLAMSUITE_DEFAULT_SHIFT_ID_5h = os.environ['GLAMSUITE_DEFAULT_SHIFT_ID_5h']
GLAMSUITE_DEFAULT_SHIFT_ID_4dot8h = os.environ['GLAMSUITE_DEFAULT_SHIFT_ID_4dot8h']
GLAMSUITE_DEFAULT_SHIFT_ID_4h = os.environ['GLAMSUITE_DEFAULT_SHIFT_ID_4h']

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

NUM_YEARLY_WORK_HOURS_2024 = 1750
PORC_SEGURETAT_SOCIAL_2024 = 33
LIMIT_COST_SEGURETAT_SOCIAL_2024 = 18693.18 

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
# select * FROM ERP_GF.ERPIntegration  where deploy=1 and endpoint='Recursos Humans ERP GF' and calltype='/workforces' and helper=''
# File "Departaments i Workforce.xlsx" includes all the department vs workforce values.
def get_value_from_database_helper(mycursor, endPoint, origin, correlationId):
    mycursor.execute("SELECT erpGFId, helper FROM ERP_GF.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND deploy = " + str(ENVIRONMENT) + " AND correlationId = '" + str(correlationId).replace("'", "''") + "'")
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
        endProcess1 = False
        page1 = 1        
        while not endProcess1:

            headers = {
                "Authorization": "Bearer " + TOKEN_API_SESAME, 
                "Content-Type": "application/json"
            }

            get_req1 = requests.get(URL_API_SESAME + URL_EMPLOYEES_SESAME + "?page=" + str(page1), headers=headers,
                                    verify=False, timeout=CONN_TIMEOUT)
            response1 = get_req1.json()

            for data1 in response1["data"]:

                name = data1["firstName"] + " " + data1["lastName"]
                dni = data1["nid"]

                logging.info('   Worker is: ' + str(name) + ' with dni: ' + str(dni))

                _workforce = data1["jobChargeName"]
                if _workforce is None:
                    logging.error('Worker without jobChargeName populated: ' + data1["nid"])
                    continue # if not populated, this worker is not used. Next!
                else:
                    # We need to get the department name using the workforce.
                    glam_id, _dept = get_value_from_database_helper(myCursor, 'Recursos Humans ERP GF', 'Sesame', _workforce)
                    if glam_id is None: 
                        logging.warning('Workforce not found on the correlationId column of ERPIntegration: ' + str(_workforce))
                        continue # if not found, this worker is not used. Next!

                workerId = data1["id"]
                address = data1["address"]
                postalCode = data1["postalCode"]
                city = data1["city"]
                region = data1["province"]
                country_code = data1["country"]
                iban = data1["accountNumber"]
                costs = {} 
                contracts = {} 
                absences = {}

                myCursorSage.execute("SELECT en.idEmpleado, " \
                                     "en.codigoEmpleado, " \
                                     "RTRIM(pd.CodigoSigla + ' ' + LTRIM(pd.ViaPublica + ' ') + LTRIM(pd.Numero1 + ' ') + LTRIM(pd.Numero2 + ' ') + LTRIM(pd.Escalera + ' ') + LTRIM(pd.Piso + ' ') + LTRIM(pd.Puerta + ' ') + LTRIM(pd.Letra)) AS direccion, " \
                                     "pd.CodigoPostal, " \
                                     "pd.Municipio, " \
                                     "pd.Provincia, " \
                                     "ec.ibanReceptor, " \
                                     "p.SiglaNacion, " \
                                     "p.primerApellidoEmpleado, " \
                                     "p.segundoApellidoEmpleado, " \
                                     "p.nombreEmpleado " \
                                     "FROM [GARCIAFAURA].dbo.EmpleadoNomina en " \
                                     "INNER JOIN [GARCIAFAURA].dbo.Personas p ON p.SiglaNacion = en.SiglaNacion AND p.Dni = en.Dni " \
                                     "LEFT JOIN [GARCIAFAURA].dbo.PersonasDomicilios pd ON pd.SiglaNacion = p.SiglaNacion AND pd.Dni = p.Dni " \
                                     "LEFT JOIN [GARCIAFAURA].dbo.empleadoCobro ec ON ec.codigoEmpresa = en.codigoEmpresa AND ec.idEmpleado = en.idEmpleado " \
                                     "WHERE pd.CodigoDireccionPersona IN ('PAR','FIS') " \
                                     "AND en.FechaBaja IS NULL " \
                                     "AND en.codigoEmpresa = 1 " \
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
                    if record[7] is not None:                            
                        country_code = record[7]

                    primerApellidoEmpleado = record[8]
                    segundoApellidoEmpleado = record[9]
                    nombreEmpleado = record[10]
                    name = nombreEmpleado.strip()
                    if primerApellidoEmpleado.strip() != "":
                        name = name + ' ' + primerApellidoEmpleado.strip()
                    if segundoApellidoEmpleado.strip() != "":
                        name = name + ' ' + segundoApellidoEmpleado.strip()

                    # Costs per year of the employee
                    # myCursorSage.execute("SELECT year, SUM(anualSalary) AS annualGrossSalary, SUM(anualSocialContribution) AS annualSocialSecurityContribution, 0 AS annualOtherExpenses FROM ( " \
                    #                     "  SELECT año AS year, SUM(importenom) AS anualSalary, 0 AS anualSocialContribution " \
                    #                     "  FROM [GARCIAFAURA].dbo.VIS_NOM_AEM_FichaHistAnual  " \
                    #                     "  WHERE codigoEmpleado = '" + codEmpleado + "' " \
                    #                     "  AND codigoEmpresa = 1 " \
                    #                     "  AND conceptoCorto = 'Devengos' AND tipo = 'Valor' AND tipoProceso IN ('MES','P01','P02') " \
                    #                     "  AND año < YEAR(GETDATE()) " \
                    #                     "  GROUP BY año " \
                    #                     "    UNION " \
                    #                     "  SELECT año AS year, 0 AS anualSalary, SUM(importenom) AS anualSocialContribution " \
                    #                     "  FROM [GARCIAFAURA].dbo.VIS_NOM_AEM_FichaHistAnual  " \
                    #                     "  WHERE codigoEmpleado = '" + codEmpleado + "' " \
                    #                     "  AND codigoEmpresa = 1 " \
                    #                     "  AND conceptoCorto = 'Total Coste SS' AND tipo = 'Valor' AND tipoProceso IN ('MES','P01','P02') " \
                    #                     "  AND año < YEAR(GETDATE()) " \
                    #                     "  GROUP BY año) t " \
                    #                     " GROUP BY year " \
                    #                     "ORDER BY year ")
                    # Costs per year of the employee
                    myCursorSage.execute("SELECT año, ROUND(SUM(baseini) / COUNT(*), 2) AS annualGrossSalary, ROUND(SUM(baseini) * " + str(PORC_SEGURETAT_SOCIAL_2024) + " / 100 / COUNT(*), 2) AS annualSocialSecurityContribution, 0 AS annualOtherExpenses " \
                                         "FROM [GARCIAFAURA].dbo.HistoricoCalculoRentaD " \
                                         "WHERE codigoEmpleado = '" + codEmpleado + "' " \
                                         "AND codigoEmpresa = 1 " \
                                         "GROUP BY año " \
                                         "ORDER BY año ")

                    for _year, _annualGrossSalary, _annualSocialSecurityContribution, _annualOtherExpenses in myCursorSage.fetchall():
                        if dni not in costs:
                            costs[dni] = []    

                        _input = str(_year) + "/12/31"
                        _format = '%Y/%m/%d'    
                        _datetime = datetime.datetime.strptime(_input, _format)
                        if _annualSocialSecurityContribution > LIMIT_COST_SEGURETAT_SOCIAL_2024:
                            _annualSocialSecurityContribution = LIMIT_COST_SEGURETAT_SOCIAL_2024
                        costs[dni].append(
                        {   
                            "date": _datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),   
                            "annualGrossSalary": float(_annualGrossSalary),
                            "annualSocialSecurityContribution": float(_annualSocialSecurityContribution),
                            "annualOtherExpenses": float(_annualOtherExpenses),
                            "correlationId": str(dni).strip()
                        }
                    )
                        
                    # Contracts of the employee    
                    myCursorSage.execute("SELECT c.codigoContrato, " \
                                         "c.subCodigoContrato, " \
                                         "en.fechaAlta, " \
                                         "en.fechaBaja, " \
                                         "en.porJornada, " \
                                         "en.codigoDepartamento " \
                                         "FROM [GARCIAFAURA].dbo.EmpleadoNomina en " \
                                         "INNER JOIN [GARCIAFAURA].dbo.contrato c ON c.codigoContrato = en.codigoContrato AND c.SubCodigoContrato = en.SubCodigoContrato " \
                                         "AND en.codigoEmpresa = 1 " \
                                         "AND en.dni = '" + str(dni).strip() + "'")

                    for _codigoContrato, _subCodigoContrato, _fechaAlta, _fechaBaja, _porcentajeJornada, _codigoDepartamento in myCursorSage.fetchall():
                        numHorasDia = float(8 * _porcentajeJornada / 100) # Num hours a day. Example: if _porcentajeJornada is 75%, then 75% of 8 hours a day is 6 hours a day
                        horario = ""
                        shift = ""
                        if numHorasDia == float(8):
                            horario = GLAMSUITE_DEFAULT_TIMETABLE_ID_8h
                            shift = GLAMSUITE_DEFAULT_SHIFT_ID_8h
                        if numHorasDia == float(6.5):
                            horario = GLAMSUITE_DEFAULT_TIMETABLE_ID_6dot5h
                            shift = GLAMSUITE_DEFAULT_SHIFT_ID_6dot5h
                        if numHorasDia == float(6):
                            horario = GLAMSUITE_DEFAULT_TIMETABLE_ID_6h
                            shift = GLAMSUITE_DEFAULT_SHIFT_ID_6h
                        if numHorasDia == float(5.6):
                            horario = GLAMSUITE_DEFAULT_TIMETABLE_ID_5dot6h
                            shift = GLAMSUITE_DEFAULT_SHIFT_ID_5dot6h
                        if numHorasDia == float(5):
                            horario = GLAMSUITE_DEFAULT_TIMETABLE_ID_5h
                            shift = GLAMSUITE_DEFAULT_SHIFT_ID_5h
                        if numHorasDia == float(4.8):
                            horario = GLAMSUITE_DEFAULT_TIMETABLE_ID_4dot8h
                            shift = GLAMSUITE_DEFAULT_SHIFT_ID_4dot8h
                        if numHorasDia == float(4):
                            horario = GLAMSUITE_DEFAULT_TIMETABLE_ID_4h
                            shift = GLAMSUITE_DEFAULT_SHIFT_ID_4h
                        if horario == "":                        
                            logging.error('      ERROR - Hores per dia no correctes. Mirar per què: ' + str(dni).strip() + ' percentatge: ' + str(_porcentajeJornada) + ' ...') 
                            continue # this contract is not used. Next!            

                        costTypeId = _codigoDepartamento
                        if _fechaBaja is None: # For the current contract, we need values 1 (DIRECTE) or 2 (INDIRECTE) 
                            if str(costTypeId) != str(1) and str(costTypeId) != str(2):
                                logging.error('      ERROR - CostTypeId incorrecte. Mirar per què: ' + str(dni).strip() + ' costTypeId: ' + str(costTypeId) + ' ...') 

                        contractNumber = (str(_codigoContrato) + "/" + str(_subCodigoContrato)).strip()
                        contractTypeId = 1 # Contracte indefinit
                        if contractNumber[0:1] == "4" or contractNumber[0:1] == "5":
                            contractTypeId = 2 # Contracte temporal

                        if dni not in contracts:
                            contracts[dni] = []    

                        if _fechaBaja is None:
                            contracts[dni].append(
                            {    
                                "contractNumber": contractNumber,
                                "contractTypeId": contractTypeId,
                                "startDate": _fechaAlta.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "departmentId": str(_dept),
                                "workforceId": str(_workforce),
                                "calendarId": str(GLAMSUITE_DEFAULT_CALENDAR_ID),
                                "annualWorkingHours": float((_porcentajeJornada * NUM_YEARLY_WORK_HOURS_2024) / 100),
                                "timetableId": str(horario),
                                "shifts": [
                                    {
                                      "monday": str(shift),
                                      "tuesday": str(shift),
                                      "wednesday": str(shift),
                                      "thursday": str(shift),
                                      "friday": str(shift),
                                      "saturday": str(shift),
                                      "sunday": str(shift)
                                    }],
                                "costTypeId": str(costTypeId),
                                "correlationId": str(dni).strip()
                            })     
                        else:
                            contracts[dni].append(
                            {    
                                "contractNumber": contractNumber,
                                "contractTypeId": contractTypeId,
                                "startDate": _fechaAlta.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "endDate": _fechaBaja.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "departmentId": str(_dept),
                                "workforceId": str(_workforce),
                                "calendarId": str(GLAMSUITE_DEFAULT_CALENDAR_ID),
                                "annualWorkingHours": float((_porcentajeJornada * NUM_YEARLY_WORK_HOURS_2024) / 100),
                                "timetableId": str(horario),
                                "shifts": [
                                    {
                                      "monday": str(shift),
                                      "tuesday": str(shift),
                                      "wednesday": str(shift),
                                      "thursday": str(shift),
                                      "friday": str(shift),
                                      "saturday": str(shift),
                                      "sunday": str(shift)
                                    }],
                                "costTypeId": str(costTypeId),
                                "correlationId": str(dni).strip()
                            })     
                else:
                    logging.error('      Treballador no trobat a SAGE: ' + str(dni).strip() + ' ...') 
                    continue # if not found, this worker is not used. Next!
            
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
                    country_code = "ES"

                linkedInProfile = " "

                dataLocation={
                    "correlationId": str(dni),
                    "zoneId": str(GLAMSUITE_DEFAULT_ZONE_EPI_ID),
                    "containerTypeId": str(GLAMSUITE_DEFAULT_CONTAINER_EPI_TYPE_ID),
                    "containerCode": "EPI" + str(codEmpleado).strip(),
                    "description": str(name).strip(),
                    "position": str(codEmpleado).strip(),
                    "preferential": False
                }                             

                page2 = 1
                endProcess2 = False
                while not endProcess2:

                    strFrom = datetime.date.today() - datetime.timedelta(365) # Darrer any
                    get_req2 = requests.get(URL_API_SESAME + URL_ABSENCES_SESAME + "?page=" + str(page2) + "&employeeIds=" + str(workerId) + "&from=" + str(strFrom), headers=headers,
                                            verify=False, timeout=CONN_TIMEOUT)
                    response2 = get_req2.json()

                    for data2 in response2["data"]:

                        date = str(data2["date"]) + "T00:00:00"
                        nonWorkingReasonId = data2["calendar"]["absenceType"]["id"]
                        nonWorkingReasonName = data2["calendar"]["absenceType"]["name"]
                        timetableId = None
                        shiftId = None

                        strNonWorkingReasonId = ""
                        if nonWorkingReasonId == "14f7617f-5378-4b7d-97cb-a6e716c8edd0":
                            strNonWorkingReasonId = "4"
                        elif nonWorkingReasonId == "1628becd-12ec-4428-bf95-46bdbc20cdb2":
                            strNonWorkingReasonId = "11"
                        elif nonWorkingReasonId == "22d46e2c-f7d4-48d4-882f-0645e47cc9da":
                            strNonWorkingReasonId = "8"
                        elif nonWorkingReasonId == "4e3c7c27-92b0-453f-9dcd-1686e7bae5ee":
                            strNonWorkingReasonId = "10"
                        elif nonWorkingReasonId == "543fd9c7-7014-4bc6-8512-129cbddd3166":
                            strNonWorkingReasonId = "6"
                        elif nonWorkingReasonId == "7ce28898-ba8c-4823-a00b-551c421004f6":
                            strNonWorkingReasonId = "11"
                        elif nonWorkingReasonId == "8d2be1c2-108a-402e-b201-02c977462ef5":
                            strNonWorkingReasonId = "7"
                        elif nonWorkingReasonId == "91ab9ab0-9c8e-4310-887b-2a2c8574fbb6":
                            strNonWorkingReasonId = "10"
                        elif nonWorkingReasonId == "b37b82e7-c934-4d29-8433-6991a40e2e06":
                            strNonWorkingReasonId = "9"
                        elif nonWorkingReasonId == "c95f6936-e71c-45ec-8491-c911a2f8fd4b":
                            strNonWorkingReasonId = "10"
                        elif nonWorkingReasonId == "e5634585-b33b-48ee-a3c7-c1e6703f6d10":
                            strNonWorkingReasonId = "4"
                        if strNonWorkingReasonId == "":
                            logging.error('      ERROR - NotWorkingReason incorrecte. Mirar per què. Reason: ' + str(nonWorkingReasonName) + ' ...') 
                            continue # if not found, this worker is not used. Next!

                        if dni not in absences:
                            absences[dni] = []    

                        absences[dni].append(
                        {   
                            "date": str(date).strip(),
                            "nonWorkingReasonId": str(strNonWorkingReasonId).strip(),
                            "timetableId": timetableId,
                            "shiftId": shiftId,
                            "correlationId": str(dni).strip()
                        })

                    meta2 = response2["meta"]
                    if str(meta2["lastPage"]) == str(page2):
                        endProcess2 = True
                    else:
                        page2 = page2 + 1

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
                    "contracts": contracts.get(dni, []),
                    "absences": absences.get(dni, []),
                    "correlationId": str(dni).strip(),
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

            meta1 = response1["meta"]
            if str(meta1["lastPage"]) == str(page1):
                endProcess1 = True
            else:
                page1 = page1 + 1

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