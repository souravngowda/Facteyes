import asyncio
import time
import json
import pytz
import logging
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration (consider moving to a config file or environment variables)
DATABASE_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "fastest",
    "user": "postgres",
    "password": "Postgre"
}

IOT_HUB_CONNECTION_STRING = "HostName=EOLIOTHub.azure-devices.net;DeviceId=INELEdgeDevice01;SharedAccessKey=vP80bGq6aCxFxyemP+YhL5XSrlStr/l+2WhUdoZFQqE="

async def send_telemetry(device_client, message):
    """Send a telemetry message to Azure IoT Hub."""
    try:
        await device_client.send_message(message)
        logger.info(f"Message sent: {message}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

async def process_row(row, device_client, cur1, conn, processed_barcodes):
    """Process a single row from the database and send EOL data."""
    orgbarcode = row[1]
    mslno = row[0]

    ProductSerialNumber = row[1]
    ProductType = row[12]
    ProductName = row[11]
    TestID = row[2]
    LSL = row[3]
    HSL = row[4]
    Value = row[5]
    Unit = row[6]
    testres = row[7]
    teststat = "pass" if testres == 1 else "FAIL"
    finalres = row[8]
    finalstat = "PASS" if finalres == 1 else "FAIL"
    finalreason = 'Product cleared all tests, ' if finalres == 1 else 'Product failed, '
    Operator = row[15]
    Shift = row[13]

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
        "EquipmentID": 'EQ02',
        "ProductName": ProductName,
        "TestID": TestID,
        "LSL": LSL,
        "HSL": HSL,
        "Value": Value,
        "Unit": Unit,
        "TestStatus": teststat,
        "TestDatetime": Currenttimestamp,
        "ReceviedTimestamp": Currenttimestamp,
        "Operator": Operator,
        "Shift": Shift,
        "MessageType": 'MICROLOGICEOL'
    }

    # Send EOL data
    ProductEOLDatapayload = json.dumps(ProductEOLData)
    await send_telemetry(device_client, ProductEOLDatapayload)

    # Update the database to mark the row as processed
    sql = """UPDATE public.reportdata SET updateflag = %s WHERE barcodedata = %s AND slno = %s AND updateflag IS NULL"""
    cur1.execute(sql, (1, orgbarcode, mslno))
    conn.commit()

    # Add the barcode to the set of processed barcodes
    processed_barcodes.add(orgbarcode)

async def send_product_status(device_client, processed_barcodes, rows):
    """Send ProductStatusData for all unique barcodes in the current batch."""
    for row in rows:
        orgbarcode = row[1]
        if orgbarcode in processed_barcodes:
            # Prepare ProductStatusData only once per barcode
            ProductSerialNumber = row[1]
            ProductType = row[12]
            ProductName = row[11]
            finalres = row[8]
            finalstat = "PASS" if finalres == 1 else "FAIL"
            finalreason = 'Product cleared all tests, ' if finalres == 1 else 'Product failed, '
            Operator = row[15]
            Shift = row[13]

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
                "EquipmentID": 'EQ02',
                "ProductName": ProductName,
                "ProductStatus": finalstat,
                "ProductionStartDate": Currenttimestamp,
                "ProductionEndDate": Currenttimestamp,
                "MessageType": "MICROLOGICPRODUCTS",
                "OPERATOR": Operator,
                "Shift": Shift,
                "StatusReason": finalreason
            }

            # Send ProductStatusData
            ProductStatuspayload = json.dumps(ProductStatusData)
            await send_telemetry(device_client, ProductStatuspayload)

            # Remove the barcode from the set to avoid duplicates in future batches
            processed_barcodes.remove(orgbarcode)

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
            # Fetch unprocessed rows from the database
            cur.execute('SELECT * FROM public.reportdata WHERE updateflag IS NULL')
            rows = cur.fetchall()

            if not rows:
                logger.info("No new rows to process.")
                time.sleep(5)  # Wait for new data
                continue

            # Process each row
            for row in rows:
                await process_row(row, device_client, cur1, conn, processed_barcodes)
                messagecount += 1
                logger.info(f"Total messages sent: {messagecount}")
                time.sleep(0.1)

            # Send ProductStatusData for all unique barcodes in this batch
            await send_product_status(device_client, processed_barcodes, rows)

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