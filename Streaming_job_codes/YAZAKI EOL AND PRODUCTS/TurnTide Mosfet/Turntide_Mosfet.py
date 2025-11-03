import asyncio
import time
import json
import pytz
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
import psycopg2


async def main():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="Mosfet",
        user="postgres",
        password="Postgre")

    cur = conn.cursor()
    cur1 = conn.cursor()
    connection_string = "HostName=EOLIOTHub.azure-devices.net;DeviceId=INELEdgeDevice01;SharedAccessKey=vP80bGq6aCxFxyemP+YhL5XSrlStr/l+2WhUdoZFQqE="
    
    device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    
    async def message_received_listener(device_client):
        while True:
            message = await device_client.receive_message()
            print(f"Message received: {message.data}")
    
    await device_client.connect()
    asyncio.create_task(message_received_listener(device_client))

    sample_message = "Hello from the edge device!"
    print(f"Sending message: {sample_message}")
    await device_client.send_message(sample_message)  

    messagecount = 0
    tempbcode = '000'

  

    while True:
        cur.execute('SELECT * FROM public.reportdata WHERE updateflag IS NULL')
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()

        while row is not None:
            # print(row)
            # print(row[0])
            # row = cur.fetchone()

            # if row is None:
            #     break

            orgbarcode = row[1]
            mslno = row[0]


            ProductSerialNumber = row[1]
            ProductType = row[14]
            ProductName = row[13]
            TestID = row[2]
            LSL = row[3]
            HSL = row[4]
            Value = row[5]
            Unit = row[6]
            #testres = row[9]
            #finalres = row[10]

            Testdate = row[16]

            testres = row[9]
            if testres == 1:
                teststat = "PASS"
            else:
                teststat = "FAIL"

            finalres = row[10]
            if finalres == 1:
                finalstat = "PASS"
                finalreason = 'Product cleared all tests, '
            else:
                finalstat = "FAIL"
                finalreason = 'Product failed, '


            TestStatus = teststat  # random.choice(TestStatuses) 


            Operator = row[17]
            Shift = row[15]            

            TestStatus = teststat  # random.choice(TestStatuses)   
            
            utc_timestamp = time.time()
            utc_datetime = datetime.utcfromtimestamp(utc_timestamp)

            ist_timezone = pytz.timezone('Asia/Kolkata')
            ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
            Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

            messagecount =  messagecount + 1

            ProductEOLData = {
                "SerialNumber": ProductSerialNumber,
                "ProductType": ProductType,
                "EquipmentID": 'EQ01',
                "ProductName": ProductName,
                "TestID": TestID,
                "LSL": LSL,
                "HSL": HSL,
                "Value": Value,
                "Unit": Unit,
                "TestStatus": TestStatus,
                "TestDatetime": Testdate.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ReceviedTimestamp": Currenttimestamp,
                "Operator": Operator,
                "Shift": Shift,
                "SlNo" :mslno,
                "MessageType": 'MICROLOGICEOL'
            }

            ProductEOLDatapayload = json.dumps(ProductEOLData)

            print(f" INEL EOL: {ProductEOLDatapayload}")            
            
            await device_client.send_message(json.dumps(ProductEOLData))
            print("Total messages sent:", messagecount)

            sql = """UPDATE public.reportdata SET updateflag = %s WHERE barcodedata = %s AND slno = %s AND updateflag IS NULL"""
            cur1.execute(sql, (1, orgbarcode, mslno))
            
            conn.commit()

            ProductStatus = finalstat #"PASS"
            reason = finalreason


            ProductStatusData = {
                "SerialNumber": ProductSerialNumber,
                "ProductType": ProductType,
                "EquipmentID": 'EQ01',
                "ProductName": ProductName,
                "ProductStatus": ProductStatus,
                "ProductionStartDate": Currenttimestamp,
                "ProductionEndDate": Currenttimestamp,
                "MessageType": "MICROLOGICPRODUCTS",
                "OPERATOR": Operator,
                "Shift": Shift,
                "ProductFailCategory": 'Unknown',
                "StatusReason": reason
            }

            if tempbcode != orgbarcode:


                ProductStatuspayload = json.dumps(ProductStatusData)

                print(f" ProdcutStatusData: {ProductStatuspayload}")                
                await device_client.send_message(json.dumps(ProductStatusData))
                
                print("Total messages sent:", messagecount)
                tempbcode = orgbarcode
# ## should add serial number for unique identification 
#             mosfetbarcodedata = row[1]
#             mosfetbands = row[7]
#             mosfetslno = row [0]
#             #mosfettestdatetime = row[16]
#             mosfetlasermarkingcode = row[8]
#             mosfettemprature = row[20]

#             MosfetData = {
#                 "SerialNumber": mosfetbarcodedata,

#                 "Bands": mosfetbands,
#                 #"DateTime":mosfettestdatetime,
#                 "LaserMarkingCode": mosfetlasermarkingcode,
#                 "Temperature": mosfettemprature,
#                 "SlNo": mosfetslno,
#                 "MessageType": 'Mosfetdata'
#             }


#             MosfetDatapayload = json.dumps(MosfetData)

#             print(f" MosfetStatusData: {MosfetDatapayload}")                 
            
#             await device_client.send_message(json.dumps(MosfetData))
#             print(f"Mosfet Data Sent: {MosfetData}")
            
#             row = cur.fetchone()
#             time.sleep(.1)

            time.sleep(.1)  # Sending telemetry every 5 seconds

        cur.close

    # Wait for the user to terminate the program
    await asyncio.Event().wait()

    # Disconnect the device client
    await device_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
