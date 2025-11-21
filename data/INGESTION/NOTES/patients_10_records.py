import csv
import random
import string
from datetime import datetime, timedelta

output_file = "patients_10_records.csv"
record_count = 10


first_names = ["Rick", "John", "Robert", "David", "Michael", "James", "Daniel", "Chris",
               "Sarah", "Emily", "Jessica", "Ashley", "Sophia", "Emma", "Olivia"]

last_names = ["Russo", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
              "Davis", "Wilson", "Taylor", "Miller"]

middle_names = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

street_names = [
    "Main St", "High St", "Broadway", "Oak St", "Maple Ave",
    "Pine St", "Cedar St", "Elm St", "Washington Ave"
]

cities = [
    "DPO AE 82777", "DPO AA 12345", "APO AE 44556",
    "FPO AP 88765", "Unit 2089 Box 9981", "Unit 0915 Box 7064"
]


def random_patient_id(n):
    return f"HOSP1-{str(n).zfill(6)}"


def random_ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"


def random_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}x{random.randint(1000,9999)}"


def random_gender():
    return random.choice(["Male", "Female"])


def random_dob():
    start = datetime(1930, 1, 1)
    end = datetime(2020, 1, 1)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def random_address():
    return f'Unit {random.randint(1000,9999)} Box {random.randint(1000,9999)}, {random.choice(cities)}'


def random_modified_date():
    today = datetime.now()
    days_back = random.randint(1, 2000)
    return (today - timedelta(days=days_back)).strftime("%Y- %m-%d")


# ----------- WRITE 10 RECORDS -----------

with open(output_file, "w", newline="") as f:
    writer = csv.writer(f)

    for i in range(1, record_count + 1):
        writer.writerow([
            random_patient_id(i),
            random.choice(first_names),
            random.choice(last_names),
            random.choice(middle_names),
            random_ssn(),
            random_phone(),
            random_gender(),
            random_dob(),
            random_address(),
            random_modified_date()
        ])

print("✔ 10 patient records created in correct format!")
