import asyncio
import time
import json
import pytz
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
import psycopg2
from azure.identity import DefaultAzureCredential
from azure.mgmt.streamanalytics import StreamAnalyticsManagementClient
from azure.mgmt.streamanalytics.models import StreamingJob

# Azure Stream Analytics configuration
SUBSCRIPTION_ID = "f12eff60-5d63-48a2-a19e-96f7de3e35bb"  # Replace with your Azure subscription ID
RESOURCE_GROUP_NAME = "INEL"  # Replace with your resource group name
STREAM_ANALYTICS_JOB_NAME = "INELProductstoSQL"  # Replace with your ASA job name

# Database configuration
DATABASE_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "Traceserver-YAZAKI",
    "user": "postgres",
    "password": "Postgre"
}

# IoT Hub connection string
IOT_HUB_CONNECTION_STRING = "HostName=EOLIOTHub.azure-devices.net;DeviceId=TraceWare01;SharedAccessKey=pfnHpbWH+cqsBFlKRW2P5GHoAhmr8HaGe+IRH30Tkao="

async def check_stream_analytics_job_status():
    """Check if the Azure Stream Analytics job is running."""
    try:
        # Authenticate with Azure
        credential = DefaultAzureCredential()
        stream_analytics_client = StreamAnalyticsManagementClient(credential, SUBSCRIPTION_ID)

        # Get the Stream Analytics job
        job = stream_analytics_client.streaming_jobs.get(RESOURCE_GROUP_NAME, STREAM_ANALYTICS_JOB_NAME)

        # Check the job status
        if job.job_state == "Running":
            print("Stream Analytics job is running.")
            return True
        else:
            print(f"Stream Analytics job is not running. Current status: {job.job_state}")
            return False
    except Exception as e:
        print(f"Failed to check Stream Analytics job status: {e}")
        return False  # Assume the job is not running if there's an error

async def send_telemetry(device_client, message):
    """Send a telemetry message to Azure IoT Hub."""
    try:
        await device_client.send_message(message)
        print(f"Message sent: {message}")
    except Exception as e:
        print(f"Failed to send message: {e}")
        raise  # Re-raise the exception to handle it in the calling function

async def process_eol_row(row, device_client, cur_update, conn):
    """Process a single row from the EOL table and send EOL data."""
    try:
        orgbarcode = row[1]

        ProductSerialNumber = row[1]
        EquipmentID = row[4]
        ProductType = row[3]
        ProductName = row[2]
        TestID = row[7]
        LSL = row[8]
        HSL = row[9]
        Value = row[10]
        Unit = row[11]
        TestStatus = row[13]
        Operator = row[5]
        Shift = row[6]
        Productiondate = row[12].strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get the current timestamp in IST
        utc_timestamp = time.time()
        utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
        ist_timezone = pytz.timezone('Asia/Kolkata')
        ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
        Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Prepare EOL data
        ProductEOLData = {
            "SerialNumber": ProductSerialNumber,
            "ProductType": ProductType,
            "EquipmentID": EquipmentID,
            "ProductName": ProductName,
            "TestID": TestID,
            "LSL": LSL,
            "HSL": HSL,
            "Value": Value,
            "Unit": Unit,
            "TestStatus": TestStatus,
            "TestDatetime": Productiondate,
            "ReceviedTimestamp": Currenttimestamp,
            "Operator": Operator,
            "Shift": Shift,
            "MessageType": 'YAZAKIEOL'
        }

        # Send EOL data
        ProductEOLDatapayload = json.dumps(ProductEOLData)
        await send_telemetry(device_client, ProductEOLDatapayload)

        # Update the database to mark the row as processed
        sql = """UPDATE public.producteolresults SET updateflag = %s WHERE uid = %s AND updateflag IS NULL"""
        cur_update.execute(sql, (1, orgbarcode))
        conn.commit()

    except Exception as e:
        print(f"Failed to process EOL row: {e}")
        raise  # Re-raise the exception to stop further processing

async def process_product_row(row, device_client, cur_update, conn):
    """Process a single row from the production lifecycle table and send product status data."""
    try:
        orgbarcode = row[1]

        ProductionLineID = row[0]
        ProductSerialNumber = row[1]
        EquipmentID = row[2]
        ProductType = row[18]
        ProductName = row[13]
        ProductStatus = row[10]
        StatusReason = row[12]
        Operator = row[3]
        Shift = row[4]
        ProductionStartDate = row[5].strftime("%Y-%m-%dT%H:%M:%SZ")
        ProductionEndDate = row[6].strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get the current timestamp in IST
        utc_timestamp = time.time()
        utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
        ist_timezone = pytz.timezone('Asia/Kolkata')
        ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
        Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Prepare product status data
        ProductStatusData = {
            "SerialNumber": ProductSerialNumber,
            "ProductType": ProductType,
            "EquipmentID": EquipmentID,
            "ProductName": ProductName,
            "ProductStatus": ProductStatus,
            "ProductionStartDate": ProductionStartDate,
            "ProductionEndDate": ProductionEndDate,
            "MessageType": 'YAZAKIProducts',
            "StatusReason": StatusReason,
            "ProductFailCategory": 'Unknown' if ProductStatus == 'FAIL' else None,
            "OPERATOR": Operator,
            "Shift": Shift,
            "ProductionLineID": ProductionLineID
        }

        # Send product status data
        ProductStatuspayload = json.dumps(ProductStatusData)
        await send_telemetry(device_client, ProductStatuspayload)

        # Update the database to mark the row as processed
        sql = """UPDATE public.productionlifecycle SET updateflag = %s WHERE uid = %s AND updateflag IS NULL"""
        cur_update.execute(sql, (1, orgbarcode))
        conn.commit()

    except Exception as e:
        print(f"Failed to process product row: {e}")
        raise  # Re-raise the exception to stop further processing

async def main():
    """Main function to fetch data from the database and send it to Azure IoT Hub."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cur_eol = conn.cursor()
        cur_prod = conn.cursor()
        cur_eol_update = conn.cursor()
        cur_prod_update = conn.cursor()

        # Connect to Azure IoT Hub
        device_client = IoTHubDeviceClient.create_from_connection_string(IOT_HUB_CONNECTION_STRING)
        await device_client.connect()

        messagecount = 0

        while True:
            # Check if the Stream Analytics job is running
            if not await check_stream_analytics_job_status():
                print("Stream Analytics job has stopped. Waiting for connection...")
                while not await check_stream_analytics_job_status():
                    await asyncio.sleep(10)  # Wait for the job to resume
                print("Stream Analytics job has resumed. Continuing data processing...")

            # Process EOL data
            cur_eol.execute('SELECT * FROM public.producteolresults WHERE updateflag IS NULL')
            rows_eol = cur_eol.fetchall()

            for row in rows_eol:
                if not await check_stream_analytics_job_status():  # Check before processing each row
                    print("Stream Analytics job stopped. Pausing data transmission...")
                    break  # Exit the loop and wait for the job to restart

                await process_eol_row(row, device_client, cur_eol_update, conn)
                messagecount += 1
                print(f"Total messages sent: {messagecount}")
                await asyncio.sleep(0.1)

            # Process product status data
            cur_prod.execute('SELECT * FROM public.productionlifecycle WHERE updateflag IS NULL')
            rows_prod = cur_prod.fetchall()

            for row in rows_prod:
                if not await check_stream_analytics_job_status():  # Check before processing each row
                    print("Stream Analytics job stopped. Pausing data transmission...")
                    break  # Exit the loop and wait for the job to restart

                await process_product_row(row, device_client, cur_prod_update, conn)
                messagecount += 1
                print(f"Total messages sent: {messagecount}")
                await asyncio.sleep(0.1)

            await asyncio.sleep(5)


    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Clean up resources
        cur_eol.close()
        cur_prod.close()
        cur_eol_update.close()
        cur_prod_update.close()
        conn.close()
        await device_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())