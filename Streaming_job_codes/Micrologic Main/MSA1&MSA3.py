# pip install azure-iot-device psycopg2 pytz

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
        database="fastest",
        user="postgres",
        password="Postgre"
    )

    cur = conn.cursor()
    cur1 = conn.cursor()
    cur_msa1 = conn.cursor()
    cur_msa1_update = conn.cursor()
    cur_msa3 = conn.cursor()
    cur_msa3_update = conn.cursor()

    connection_string = "HostName=EOLIOTHub.azure-devices.net;DeviceId=INELEdgeDevice01;SharedAccessKey=vP80bGq6aCxFxyemP+YhL5XSrlStr/l+2WhUdoZFQqE="
    device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)

    async def message_received_listener(device_client):
        while True:
            message = await device_client.receive_message()
            print(f"Message received: {message.data}")

    await device_client.connect()
    asyncio.create_task(message_received_listener(device_client))

    print("Sending startup message...")
    await device_client.send_message("Hello from the edge device!")

    messagecount = 0
    tempbcode = '000'

    while True:
        ist_datetime = datetime.now(pytz.utc).astimezone(pytz.timezone('Asia/Kolkata'))
        Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

        # ========= EOL REPORTDATA ==========
        cur.execute('SELECT * FROM public.reportdata WHERE updateflag IS NULL')
        row = cur.fetchone()
        while row:
            orgbarcode, mslno = row[1], row[0]
            ProductSerialNumber, ProductType, ProductName = row[1], row[12], row[11]
            TestID, LSL, HSL, Value, Unit = row[2], row[3], row[4], row[5], row[6]
            testres, finalres = row[7], row[8]
            Operator, Shift = row[15], row[13]
            slno = row[18]

            Productiondate = row[14].strftime("%Y-%m-%dT%H:%M:%SZ")

            teststat = "PASS" if testres == 1 else "FAIL"
            finalstat = "PASS" if finalres == 1 else "FAIL"
            finalreason = "Product cleared all tests" if finalres == 1 else "Product failed"

            messagecount += 1

            ProductEOLData = {
                "SerialNumber": ProductSerialNumber,
                "ProductType": ProductType,
                "EquipmentID": "EQ03",
                "ProductName": ProductName,
                "TestID": TestID,
                "LSL": LSL,
                "HSL": HSL,
                "Value": Value,
                "Unit": Unit,
                "TestStatus": teststat,
                "TestDatetime": Productiondate,
                "ReceviedTimestamp": Currenttimestamp,
                "Operator": Operator,
                "Shift": Shift,
                "SlNo" : slno,
                "MessageType": "MICROLOGICEOL"
            }

            await device_client.send_message(json.dumps(ProductEOLData))
            print(f"[EOL] Sent: {ProductEOLData}")
            print("Total messages sent:", messagecount)

            cur1.execute("""UPDATE public.reportdata SET updateflag = %s 
                            WHERE barcodedata = %s AND slno = %s AND updateflag IS NULL""",
                         (1, orgbarcode, mslno))
            conn.commit()

            if tempbcode != orgbarcode:
                ProductStatusData = {
                    "SerialNumber": ProductSerialNumber,
                    "ProductType": ProductType,
                    "EquipmentID": "EQ03",
                    "ProductName": ProductName,
                    "ProductStatus": finalstat,
                    "ProductionStartDate": Productiondate,
                    "ProductionEndDate": Productiondate,
                    "MessageType": "MICROLOGICPRODUCTS",
                    "OPERATOR": Operator,
                    "Shift": Shift,
                    "StatusReason": finalreason
                }

                await device_client.send_message(json.dumps(ProductStatusData))
                print(f"[EOL-Status] Sent: {ProductStatusData}")
                tempbcode = orgbarcode

            time.sleep(0.1)
            row = cur.fetchone()

        # ========= MSA1 REPORTDATA ==========
        cur_msa1.execute('SELECT * FROM public.msa1reportdata WHERE updateflag IS NULL')
        row_msa1 = cur_msa1.fetchone()
        while row_msa1:
            orgbarcode = row_msa1[1]
            Productiondate = row_msa1[14].strftime("%Y-%m-%dT%H:%M:%SZ")
            test_status = "PASS" if row_msa1[7] == 1 else "FAIL"
            
            ProductMSA1Data = {
                "SerialNumber": row_msa1[1],
                "ProductType": row_msa1[12],
                "EquipmentID": "EQ03",
                "ProductName": row_msa1[11],
                "TestID": row_msa1[2],
                "LSL": row_msa1[3],
                "HSL": row_msa1[4],
                "Value": row_msa1[5],
                "Unit": row_msa1[6],
                "TestStatus": test_status,
                "TestDatetime": Productiondate,
                "ReceviedTimestamp": Currenttimestamp,
                "Operator": row_msa1[15],
                "Shift": row_msa1[13],
                "TestCycleNumber":row_msa1[18],
                "SlNo": row_msa1[19],
                "BatchNo": row_msa1[19],
                "MessageType": "MSA1"
            }

            await device_client.send_message(json.dumps(ProductMSA1Data))
            print(f"[MSA1] Sent: {ProductMSA1Data}")
            messagecount += 1
            cur_msa1_update.execute("""UPDATE public.msa1reportdata SET updateflag = %s 
                                       WHERE barcodedata = %s AND updateflag IS NULL""",
                                    (1, orgbarcode))
            conn.commit()
            time.sleep(0.1)
            row_msa1 = cur_msa1.fetchone()

        # ========= MSA3 REPORTDATA ==========
        cur_msa3.execute('SELECT * FROM public.msa3reportdata WHERE updateflag IS NULL')
        row_msa3 = cur_msa3.fetchone()
        while row_msa3:
            orgbarcode = row_msa3[1]
            Productiondate = row_msa3[14].strftime("%Y-%m-%dT%H:%M:%SZ")
            test_status = "PASS" if row_msa3[7] == 1 else "FAIL"
            
            ProductMSA3Data = {
                "SerialNumber": row_msa3[1],
                "ProductType": row_msa3[12],
                "EquipmentID": "EQ03",
                "ProductName": row_msa3[11],
                "TestID": row_msa3[2],
                "LSL": row_msa3[3],
                "HSL": row_msa3[4],
                "Value": row_msa3[5],
                "Unit": row_msa3[6],
                "TestStatus": test_status,
                "TestDatetime": Productiondate,
                "ReceviedTimestamp": Currenttimestamp,
                "Operator": row_msa3[15],
                "Shift": row_msa3[13],
                "TestCycleNumber":1 if row_msa3[18] and int(row_msa3[18]) % 2 != 0 else 2,
                "SlNo": row_msa3[19],
                "BatchNo": row_msa3[19],
                "MessageType": "MSA3"
            }

            await device_client.send_message(json.dumps(ProductMSA3Data))
            print(f"[MSA3] Sent: {ProductMSA3Data}")
            messagecount += 1
            cur_msa3_update.execute("""UPDATE public.msa3reportdata SET updateflag = %s 
                                       WHERE barcodedata = %s AND updateflag IS NULL""",
                                    (1, orgbarcode))
            conn.commit()
            time.sleep(0.1)
            row_msa3 = cur_msa3.fetchone()

    # Never reached because of infinite loop, but added for structure
    await device_client.disconnect()
    cur.close()
    cur1.close()
    cur_msa1.close()
    cur_msa1_update.close()
    cur_msa3.close()
    cur_msa3_update.close()
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())