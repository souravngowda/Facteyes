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
        database="Traceserver",
        user="postgres",
        password="Postgre")

    cur_eol = conn.cursor()
    cur_prod = conn.cursor()
    cur_order = conn.cursor()
    cur_rework = conn.cursor()
    cur_eol_update = conn.cursor()
    cur_prod_update = conn.cursor()
    cur_order_update = conn.cursor()
    cur_rework_update = conn.cursor()

    connection_string = "HostName=EOLIOTHub.azure-devices.net;DeviceId=TraceWare01;SharedAccessKey=pfnHpbWH+cqsBFlKRW2P5GHoAhmr8HaGe+IRH30Tkao="
    device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)

    async def message_received_listener(device_client):
        while True:
            message = await device_client.receive_message()
            print(f"Message received: {message.data}")
            # Process the message data here

    await device_client.connect()
    asyncio.create_task(message_received_listener(device_client))

    messagecount = 0

    while True:
        # Send EOL telemetry
        cur_eol.execute('SELECT * FROM public.producteolresults WHERE updateflag IS NULL')
        print("The number of parts: ", cur_eol.rowcount)
        row_eol = cur_eol.fetchone()

        while row_eol is not None:
            orgbarcode_eol = row_eol[1]

            ProductSerialNumber_eol = row_eol[1]
            EquipmentID_eol = row_eol[4]
            ProductType_eol = row_eol[3]
            ProductName_eol = row_eol[2]
            TestID_eol = row_eol[7]
            LSL_eol = row_eol[8]
            HSL_eol = row_eol[9]
            Value_eol = row_eol[10]
            Unit_eol = row_eol[11]
            TestStatus_eol = row_eol[13]
            Operator_eol = row_eol[5]
            Shift_eol = row_eol[6]

            utc_timestamp = time.time()
            utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
            ist_timezone = pytz.timezone('Asia/Kolkata')
            ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
            Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            Productiondate_eol = row_eol[12].strftime("%Y-%m-%dT%H:%M:%SZ")

            messagecount += 1

            ProductEOLData = {
                "SerialNumber": ProductSerialNumber_eol,
                "ProductType": ProductType_eol,
                "EquipmentID": EquipmentID_eol,
                "ProductName": ProductName_eol,
                "TestID": TestID_eol,
                "LSL": LSL_eol,
                "HSL": HSL_eol,
                "Value": Value_eol,
                "Unit": Unit_eol,
                "TestStatus": TestStatus_eol,
                "TestDatetime": Productiondate_eol,
                "ReceviedTimestamp": Currenttimestamp,
                "Operator": Operator_eol,
                "Shift": Shift_eol,
                "MessageType": 'YAZAKIEOL'
            }

            ProductEOLDatapayload = json.dumps(ProductEOLData)

            print(f" INEL EOL: {ProductEOLDatapayload}")

            await device_client.send_message(ProductEOLDatapayload)

            print("Total messages sent:", messagecount)

            sql = """UPDATE public.producteolresults SET updateflag = %s WHERE uid = %s AND updateflag IS NULL"""
            cur_eol_update.execute(sql, (1,orgbarcode_eol,))
            conn.commit()

            row_eol = cur_eol.fetchone()
            time.sleep(0.1)

        # Send Product Status telemetry
        cur_prod.execute('SELECT * FROM public.productionlifecycle WHERE updateflag IS NULL ')
        print("The number of parts: ", cur_prod.rowcount)
        row_prod = cur_prod.fetchone()

        while row_prod is not None:
            orgbarcode_prod = row_prod[1]

            ProductionLineID_prod = row_prod[0]
            ProductSerialNumber_prod = row_prod[1]
            EquipmentID_prod = row_prod[2]
            ProductType_prod = row_prod[18]
            ProductName_prod = row_prod[13]
            ProductStatus_prod = row_prod[10]
            StatusReason_prod = row_prod[12]
            Operator_prod = row_prod[3]
            Shift_prod = row_prod[4]

            utc_timestamp = time.time()
            utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
            ist_timezone = pytz.timezone('Asia/Kolkata')
            ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
            Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

            ProductionStartDate = row_prod[5].strftime("%Y-%m-%dT%H:%M:%SZ")
            ProductionEndDate = row_prod[6].strftime("%Y-%m-%dT%H:%M:%SZ")

            messagecount += 1

            ProductStatusData = {
                "SerialNumber": ProductSerialNumber_prod,
                "ProductType": ProductType_prod,
                "EquipmentID": EquipmentID_prod,
                "ProductName": ProductName_prod,
                "ProductStatus": ProductStatus_prod,
                "ProductionStartDate": ProductionStartDate,
                "ProductionEndDate": ProductionEndDate,
                "MessageType": 'YAZAKIProducts',
                "StatusReason": StatusReason_prod,
                "ProductFailCategory": 'Unknown' if ProductStatus_prod == 'FAIL' else None,                
                "OPERATOR": Operator_prod,
                "Shift" : Shift_prod,
                "ProductionLineID" : ProductionLineID_prod
            }

            ProductStatuspayload = json.dumps(ProductStatusData)

            print(f" INEL Product Status: {ProductStatuspayload}")

            await device_client.send_message(ProductStatuspayload)

            print("Total messages sent:", messagecount)

            sql = """UPDATE public.productionlifecycle  SET updateflag = %s WHERE uid = %s AND updateflag IS NULL"""
            cur_prod_update.execute(sql, (1,orgbarcode_prod,))
            conn.commit()

            row_prod = cur_prod.fetchone()
            time.sleep(0.1)







        time.sleep(5)

    cur_eol.close()
    cur_prod.close()
    cur_eol_update.close()
    cur_prod_update.close()


if __name__ == "__main__":
    asyncio.run(main())
