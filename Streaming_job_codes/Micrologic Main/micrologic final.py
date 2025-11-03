# Install the required packages using pip:
# pip install azure-iot-device azure-iot-device-edge

import asyncio
import time
import random
import json
import pytz
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
import psycopg2



async def main():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="Snakesensors",
        user="postgres",
        password="Postgre")


    cur = conn.cursor()
    cur1 = conn.cursor()
    connection_string = "HostName=EOLIOTHub.azure-devices.net;DeviceId=INELEdgeDevice01;SharedAccessKey=vP80bGq6aCxFxyemP+YhL5XSrlStr/l+2WhUdoZFQqE="

    # Create an IoT Hub device client
    device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)

    # Define a callback function to handle the message received from the IoT Hub
    async def message_received_listener(device_client):
        while True:
            message = await device_client.receive_message()
            print(f"Message received: {message.data}")
            # You can process the message data here

    # Connect the device client to the IoT Hub
    await device_client.connect()

    # Start the message received listener
    asyncio.create_task(message_received_listener(device_client))

    # Send a sample message to the IoT Hub
    sample_message = "Hello from the edge device!"
    print(f"Sending message: {sample_message}")
    await device_client.send_message(sample_message)

    # You can add more code here to send telemetry, handle desired properties, etc.

    messagecount = 0
    tempbcode = '000'

    while True:
        # Send a sample telemetry message
        cur.execute('SELECT * FROM public.reportdata WHERE updateflag IS NULL')
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()

        while row is not None:
            print(row)
            print(row[0])
            row = cur.fetchone()

            if row is None:
                break

            orgbarcode = row[1]
            mslno = row[0]

        #iIndex = 0
        #while iIndex < 34:
            # Preparing sample values for the eol data
            #random_numberProduct = "WCM"  # str(random.randint(1, 10000))
            #random_numberEquip = "E0"  # str(random.randint(1, 48))
            ProductSerialNumber = row[1]
           # EquipmentID_eol = row_eol[4]
            ProductType = row[12]
            ProductName= row[11]
            TestID = row[2]
            LSL = row[3]
            HSL= row[4]
            Value = row[5]
            Unit = row[6]
            
            testres = row[7]
            if testres == 1:
                teststat = "pass"
            else:
                teststat = "FAIL"

            finalres = row[8]
            if finalres == 1:
                finalstat = "PASS"
                finalreason = 'Product cleared all tests, '
            else:
                finalstat = "FAIL"
                finalreason = 'Product failed, '


            TestStatus = teststat  # random.choice(TestStatuses)            


            Operator = row[15]
            Shift = row[13]


            TestStatus = teststat  # random.choice(TestStatuses)

            # Get the current UTC timestamp
            utc_timestamp = time.time()
            # Convert UTC timestamp to a datetime object
            utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
            # Create a timezone object for IST
            ist_timezone = pytz.timezone('Asia/Kolkata')  # 'Asia/Kolkata' is the time zone for IST
            # Convert the UTC datetime to IST datetime
            ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
            # Format the IST datetime as a string
            Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

            messagecount = messagecount + 1

            ProductEOLData = {

                "SerialNumber": ProductSerialNumber,
                "ProductType": ProductType,
                "EquipmentID": 'EQ02',
                "ProductName": ProductName,
                "TestID": TestID,
                "LSL": LSL,
                "HSL": HSL,
                "Value": Value,
                "Unit": Unit,
                "TestStatus": TestStatus,
                "TestDatetime": Currenttimestamp,
                "ReceviedTimestamp": Currenttimestamp,
                "Operator": Operator,
                "Shift": Shift,
                "MessageType": 'MICROLOGICEOL'
            }

            ProductEOLDatapayload = json.dumps(ProductEOLData)

            print(f" INEL EOL: {ProductEOLDatapayload}")

            await device_client.send_message(ProductEOLDatapayload)

            print("Total messages sent:", messagecount)

            sql =sql = """UPDATE public.reportdata SET updateflag = %s WHERE barcodedata = %s AND slno = %s AND updateflag IS NULL"""
            cur1.execute(sql, (1,orgbarcode,mslno))
            updated_rows = cur1.rowcount
            conn.commit()


            ProductStatus = finalstat #"PASS"
            reason = finalreason
            # Product Status
            ProductStatusData = {

                "SerialNumber": ProductSerialNumber,
                "ProductType": ProductType,
                "EquipmentID": 'EQ02',
                "ProductName": ProductName,
                "ProductStatus": ProductStatus,
                "ProductionStartDate": Currenttimestamp,
                "ProductionEndDate": Currenttimestamp,
                "MessageType ": "MICROLOGICPRODUCTS",
                "OPERATOR": Operator,
                "Shift" : 'Shift',               
                "StatusReason": reason
            }

            if tempbcode != orgbarcode:


                ProductStatuspayload = json.dumps(ProductStatusData)

                print(f" ProdcutStatusData: {ProductStatuspayload}")

                await device_client.send_message(ProductStatuspayload)

                print("Total messages sent:", messagecount)

                tempbcode = orgbarcode




            time.sleep(.1)  # Sending telemetry every 5 seconds

        cur.close

    # Wait for the user to terminate the program
    await asyncio.Event().wait()

    # Disconnect the device client
    await device_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
