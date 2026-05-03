import json
import time
import random
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer


def next_sync_time():
    global last_event_time

    step_seconds = random.randint(1, 8)
    last_event_time = last_event_time + timedelta(seconds=step_seconds)

    return last_event_time


def format_good_time(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def random_bad_time(dt):
    bad_formats = [
        dt.strftime("%Y%m%d%H%M%S"),              # 20260424211025
        dt.strftime("%Y/%m/%d %H:%M:%S"),         # 2026/04/24 21:10:25
        dt.strftime("%d-%m-%Y %H:%M:%S"),         # 24-04-2026 21:10:25
        dt.strftime("%m/%d/%Y %I:%M:%S %p"),      # 04/24/2026 09:10:25 PM
        dt.strftime("%Y-%m-%d %H:%M:%S"),         # 2026-04-24 21:10:25
        dt.strftime("%Y-%m-%d"),                  # 2026-04-24
        str(int(dt.timestamp())),                 # unix seconds
        str(int(dt.timestamp() * 1000)),          # unix milliseconds
        "2026-99-99T99:99:99Z",
        "24 April 2026 9 PM",
        "",
        "null",
        "invalid-date"
    ]

    return random.choice(bad_formats)


def generate_good_event():
    global event_counter

    event_counter += 1
    event_time = next_sync_time()

    return {
        "event_id": f"evt_{event_counter}",
        "customer_id": random.randint(1, 1000),
        "amount": round(random.uniform(10, 2000), 2),
        "payment_status": random.choice(PAYMENT_STATUS),
        "event_time": format_good_time(event_time)
    }


def make_bad_event(event):
    bad_event = event.copy()

    error_type = random.choice([
        "missing_event_id",
        "empty_event_id",
        "wrong_event_id_format",
        "missing_customer_id",
        "empty_customer_id",
        "wrong_customer_type",
        "negative_amount",
        "amount_as_string",
        "amount_with_currency",
        "amount_comma_format",
        "invalid_amount",
        "missing_amount",
        "invalid_payment_status",
        "empty_payment_status",
        "bad_timestamp_format",
        "future_timestamp",
        "missing_event_time",
        "duplicate_event_id",
        "extra_field_schema_drift",
        "renamed_field_schema_drift"
    ])

    current_dt = next_sync_time()

    if error_type == "missing_event_id":
        bad_event.pop("event_id", None)

    elif error_type == "empty_event_id":
        bad_event["event_id"] = random.choice(["", " ", None])

    elif error_type == "wrong_event_id_format":
        bad_event["event_id"] = random.choice([
            str(random.randint(10000, 99999)),
            f"event-{random.randint(10000, 99999)}",
            f"EVT{random.randint(10000, 99999)}"
        ])

    elif error_type == "missing_customer_id":
        bad_event.pop("customer_id", None)

    elif error_type == "empty_customer_id":
        bad_event["customer_id"] = random.choice(["", " ", None])

    elif error_type == "wrong_customer_type":
        bad_event["customer_id"] = random.choice([
            f"cus_{random.randint(1, 1000)}",
            "unknown",
            "guest",
            "N/A"
        ])

    elif error_type == "negative_amount":
        bad_event["amount"] = -round(random.uniform(10, 2000), 2)

    elif error_type == "amount_as_string":
        bad_event["amount"] = str(round(random.uniform(10, 2000), 2))

    elif error_type == "amount_with_currency":
        bad_event["amount"] = random.choice([
            f"${round(random.uniform(10, 2000), 2)}",
            f"CAD {round(random.uniform(10, 2000), 2)}",
            f"{round(random.uniform(10, 2000), 2)} CAD"
        ])

    elif error_type == "amount_comma_format":
        bad_event["amount"] = f"{random.randint(1, 9)},{random.randint(100, 999)}.{random.randint(10, 99)}"

    elif error_type == "invalid_amount":
        bad_event["amount"] = random.choice([
            "free",
            "error",
            "NaN",
            "",
            None
        ])

    elif error_type == "missing_amount":
        bad_event.pop("amount", None)

    elif error_type == "invalid_payment_status":
        bad_event["payment_status"] = random.choice([
            "done",
            "paid",
            "complete",
            "cancelled",
            "error",
            "SUCCESS",
            "FAILED"
        ])

    elif error_type == "empty_payment_status":
        bad_event["payment_status"] = random.choice(["", " ", None])

    elif error_type == "bad_timestamp_format":
        bad_event["event_time"] = random_bad_time(current_dt)

    elif error_type == "future_timestamp":
        future_dt = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 60))
        bad_event["event_time"] = random.choice([
            format_good_time(future_dt),
            future_dt.strftime("%Y%m%d%H%M%S"),
            future_dt.strftime("%Y/%m/%d %H:%M:%S")
        ])

    elif error_type == "missing_event_time":
        bad_event.pop("event_time", None)

    elif error_type == "duplicate_event_id":
        bad_event["event_id"] = f"evt_{random.randint(10001, max(10001, event_counter - 1))}"

    elif error_type == "extra_field_schema_drift":
        bad_event["source_system"] = random.choice(["mobile_app", "web_checkout", "pos_terminal"])
        bad_event["device_id"] = f"dev_{random.randint(100, 999)}"

    elif error_type == "renamed_field_schema_drift":
        bad_event["status"] = bad_event.pop("payment_status")

    return bad_event


def generate_event():
    event = generate_good_event()

    if random.random() > GOOD_RATE:
        event = make_bad_event(event)

    return event


if __name__ == '__main__':
    
    config = json.load(open("config.json"))
    
    GOOD_RATE = 0.60
    SLEEP_SECONDS = 1

    event_counter = 10000
    last_event_time = datetime.now(timezone.utc)

    PAYMENT_STATUS = ["success", "failed", "pending"]
    
    #=========== KAFKA CONFIGURATION ===========#
    BOOTSTRAP_SERVER = config["BOOTSTRAP_SERVER"]
    API_KEY = config["API_KEY"]
    API_SECRET = config["API_SECRET"]
    TOPIC = config["TOPIC"]

    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": API_KEY,
        "sasl.password": API_SECRET,
    }

    producer = Producer(conf)

    while True:
        event = generate_event()
        print(json.dumps(event))

        producer.produce(
            TOPIC,
            key=str(event.get("event_id", "missing_key")),
            value=json.dumps(event)
        )

        producer.flush()
        print("sent:", event)
        time.sleep(0.1)
