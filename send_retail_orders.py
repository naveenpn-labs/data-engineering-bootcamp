
import csv
import json
import os

from azure.eventhub import EventHubProducerClient, EventData

CONNECTION_STRING = "Endpoint=sb://esehpn5rfaxti1pgawts9e.servicebus.windows.net/;SharedAccessKeyName=key_9ce4f107-4f6f-40d8-8f27-e6e6f902e940;SharedAccessKey=ZoAOViK7+JHJZLpC1YbmoChyXA6yD3B4U+AEhJESoOE=;EntityPath=esehpn5rfaxti1pgawts9e_eh"

# Create Event Hubs producer
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING
)

total_records = 0
total_batches = 0
CSV_FILE = "retail_orders.csv"
try:

    # Read CSV file
    with open(CSV_FILE, "r", encoding="utf-8", newline="") as file:

        reader = csv.DictReader(file)

        event_batch = producer.create_batch()

        for row in reader:
            # Convert CSV values to appropriate data types
            order = {
                "order_id": int(row["order_id"]),
                "customer_id": int(row["customer_id"]),
                "product": row["product"],
                "quantity": int(row["quantity"]),
                "amount": int(row["amount"])
            }

            # Convert dictionary to JSON
            json_data = json.dumps(order)

            # Create EventData
            event = EventData(json_data)

            # Add event to batch
            try:
                event_batch.add(event)

            except ValueError:

                # Send current batch when full
                producer.send_batch(event_batch)

                total_batches += 1
                print(f"Batch {total_batches} sent")

                # Create new batch
                event_batch = producer.create_batch()

                # Add current event
                event_batch.add(event)

            total_records += 1

            print("Sending:", json_data)

        # Send remaining records
        if len(event_batch) > 0:

            producer.send_batch(event_batch)

            total_batches += 1
            print(f"Batch {total_batches} sent")

    print("\nIngestion completed successfully")
    print("Total records:", total_records)
    print("Total batches:", total_batches)

finally:
    producer.close()