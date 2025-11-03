import asyncio
import time
import json
import pytz
import logging
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
import psycopg2
from azure.identity import DefaultAzureCredential
from azure.mgmt.streamanalytics import StreamAnalyticsManagementClient
from azure.mgmt.streamanalytics.models import StreamingJob

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "Mosfet",
    "user": "postgres",
    "password": "Postgre"
}

# Azure IoT Hub connection string
IOT_HUB_CONNECTION_STRING = "HostName=EOLIOTHub.azure-devices.net;DeviceId=INELEdgeDevice01;SharedAccessKey=vP80bGq6aCxFxyemP+YhL5XSrlStr/l+2WhUdoZFQqE="

# Azure Stream Analytics configuration
SUBSCRIPTION_ID = "f12eff60-5d63-48a2-a19e-96f7de3e35bb"  # Replace with your Azure subscription ID
RESOURCE_GROUP_NAME = "INEL"  # Replace with your resource group name
STREAM_ANALYTICS_JOB_NAME = "INELEOLResultstoSQL"  # Replace with your ASA job name

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
            logger.info("Stream Analytics job is running.")
            return True
        else:
            logger.warning(f"Stream Analytics job is not running. Current status: {job.job_state}")
            return False
    except Exception as e:
        logger.error(f"Failed to check Stream Analytics job status: {e}")
        return False

async def send_telemetry(device_client, message):
    """Send a telemetry message to Azure IoT Hub."""
    try:
        await device_client.send_message(message)
        logger.info(f"Message sent: {message}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        raise  # Re-raise the exception to handle it in the calling function

async def process_row(row, device_client, cur1, conn, processed_barcodes):
    """Process a single row from the database and send EOL data."""
    try:
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
        Testdate = row[16]
        testres = row[9]
        finalres = row[10]
        Operator = row[17]
        Shift = row[15]

        # Determine test and final status
        teststat = "PASS" if testres == 1 else "FAIL"
        finalstat = "PASS" if finalres == 1 else "FAIL"
        finalreason = 'Product cleared all tests, ' if finalres == 1 else 'Product failed, '

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
            "EquipmentID": 'EQ01',
            "ProductName": ProductName,
            "TestID": TestID,
            "LSL": LSL,
            "HSL": HSL,
            "Value": Value,
            "Unit": Unit,
            "TestStatus": teststat,
            "TestDatetime": Testdate.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ReceviedTimestamp": Currenttimestamp,
            "Operator": Operator,
            "Shift": Shift,
            "SlNo": mslno,
            "MessageType": 'MICROLOGICEOL'
        }

        # Send EOL data
        ProductEOLDatapayload = json.dumps(ProductEOLData)
        await send_telemetry(device_client, ProductEOLDatapayload)

        # Prepare MosfetData
        mosfetbarcodedata = row[1]
        mosfetbands = row[7]
        mosfetslno = row[0]
        mosfetlasermarkingcode = row[8]
        mosfettemprature = row[20]

        MosfetData = {
            "SerialNumber": mosfetbarcodedata,
            "Bands": mosfetbands,
            "LaserMarkingCode": mosfetlasermarkingcode,
            "Temperature": mosfettemprature,
            "SlNo": mosfetslno,
            "MessageType": 'Mosfetdata'
        }

        # Send MosfetData
        MosfetDatapayload = json.dumps(MosfetData)
        await send_telemetry(device_client, MosfetDatapayload)
        logger.info(f"Mosfet Data Sent: {MosfetDatapayload}")

        # Update the database to mark the row as processed
        sql = """UPDATE public.reportdata SET updateflag = %s WHERE barcodedata = %s AND slno = %s AND updateflag IS NULL"""
        cur1.execute(sql, (1, orgbarcode, mslno))
        conn.commit()

        # Add the barcode to the set of processed barcodes
        processed_barcodes.add(orgbarcode)
    except Exception as e:
        logger.error(f"Failed to process row: {e}")
        raise  # Re-raise the exception to stop further processing

async def send_product_status(device_client, processed_barcodes, rows):
    """Send ProductStatusData for all unique barcodes in the current batch."""
    try:
        for row in rows:
            orgbarcode = row[1]
            if orgbarcode in processed_barcodes:
                # Prepare ProductStatusData only once per barcode
                ProductSerialNumber = row[1]
                ProductType = row[14]
                ProductName = row[13]
                finalres = row[10]
                finalstat = "PASS" if finalres == 1 else "FAIL"
                finalreason = 'Product cleared all tests, ' if finalres == 1 else 'Product failed, '
                Operator = row[17]
                Shift = row[15]

                # Get the current timestamp in IST
                utc_timestamp = time.time()
                utc_datetime = datetime.utcfromtimestamp(utc_timestamp)
                ist_timezone = pytz.timezone('Asia/Kolkata')
                ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
                Currenttimestamp = ist_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

                # Prepare ProductStatusData
                ProductStatusData = {
                    "SerialNumber": ProductSerialNumber,
                    "ProductType": ProductType,
                    "EquipmentID": 'EQ01',
                    "ProductName": ProductName,
                    "ProductStatus": finalstat,
                    "ProductionStartDate": Currenttimestamp,
                    "ProductionEndDate": Currenttimestamp,
                    "MessageType": "MICROLOGICPRODUCTS",
                    "OPERATOR": Operator,
                    "Shift": Shift,
                    "ProductFailCategory": 'Unknown' if finalstat == 'FAIL' else None,
                    "StatusReason": finalreason
                }

                # Send ProductStatusData
                ProductStatuspayload = json.dumps(ProductStatusData)
                await send_telemetry(device_client, ProductStatuspayload)

                # Remove the barcode from the set to avoid duplicates in future batches
                processed_barcodes.remove(orgbarcode)
    except Exception as e:
        logger.error(f"Failed to send product status: {e}")
        raise  # Re-raise the exception to handle it in the calling function

async def main():
    """Main function to fetch data from the database and send it to Azure IoT Hub."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cur = conn.cursor()
        cur1 = conn.cursor()

        # Connect to Azure IoT Hub
        device_client = IoTHubDeviceClient.create_from_connection_string(IOT_HUB_CONNECTION_STRING)
        await device_client.connect()

        messagecount = 0
        processed_barcodes = set()  # Track processed barcodes

        while True:
            try:
                # Check if the Stream Analytics job is running
                if not await check_stream_analytics_job_status():
                    logger.info("Stream Analytics job is not running. Waiting for streaming connection...")
                    await asyncio.sleep(10)  # Wait for 10 seconds before checking again
                    continue

                # Fetch unprocessed rows from the database
                cur.execute('SELECT * FROM public.reportdata WHERE updateflag IS NULL')
                rows = cur.fetchall()

                if not rows:
                    logger.info("No new rows to process.")
                    await asyncio.sleep(5)  # Wait for new data
                    continue

                # Process each row
                for row in rows:
                    await process_row(row, device_client, cur1, conn, processed_barcodes)
                    messagecount += 1
                    logger.info(f"Total messages sent: {messagecount}")
                    await asyncio.sleep(0.1)

                # Send ProductStatusData for all unique barcodes in this batch
                await send_product_status(device_client, processed_barcodes, rows)
            except Exception as e:
                logger.error(f"An error occurred in the main loop: {e}")
                await asyncio.sleep(10)  # Wait before retrying
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Clean up resources
        cur.close()
        cur1.close()
        conn.close()
        await device_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())