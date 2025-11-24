import numpy as np
import pandas as pd
from datetime import date, timedelta
import random
import pycountry
import string
from tqdm import tqdm
from faker import Faker
import os

fake = Faker()

def creation_date():

    start_date = date(2024, 1, 1)

    days_offset = np.random.randint(0, 365)

    createdAt = start_date + timedelta(days = days_offset)

    return createdAt

def generate_birthdate():

    """
    - This function generate a list of normally distributes values centered around a mean of 28 with a deviation of 15.
    - ages are then clipped to range between 16 and 68. Then a sampling with replacement performed to select a sample of 6000.
    - 
    """

    birthdate_list =[]

    np.random.seed(42)
    ages = np.random.normal(loc=28, scale=15, size=10000)
    valid_ages = ages[(ages>=16) & (ages<=68)]
    sampling = random.choices(valid_ages, k=6000)
    today = date.today()
    for age in sampling:
        birth_year = today.year -int(age)
        days_offset = np.random.randint(0, 365)
        birth_date = date(birth_year, 1, 1) + timedelta(days=days_offset)
        birthdate_list.append(birth_date)
    
    return birthdate_list

def generate_emails():

    valid_chars = string.ascii_lowercase + string.digits
    prefix = "".join(random.choice(valid_chars) for _ in range(10))
    domains = ["gmail.com", "outlook.com", "yahoo.com"]
    domain = random.choice(domains)

    return f"{prefix}@{domain}"

def generate_qualification():

    degree = [
        "High School or Baccalaureate",
        "Some College (1-3 years)",
        "Bachelor's degree",
        "Master's degree",
        "Doctorate (e.g. PhD)",
    ]

    qualification = random.choice(degree)

    return qualification

def generate_student_data(num_of_obs:int):

    data = {}
    name = []
    DOB =[]
    gender =[]
    email = []
    admissionsQuiz = []
    countryISO2 =[]
    highestDegreeEarned = []
    createdAt = []

    countries=[country.alpha_2 for country in pycountry.countries]

    for i in tqdm(range(0, num_of_obs)):
        name.append(fake.name())
        gender.append(random.choice(["male","female"]))
        email.append(generate_emails())
        admissionsQuiz.append(np.random.choice(["complete","incomplete"]))
        countryISO2.append(random.choices(countries)[0])
        highestDegreeEarned.append(generate_qualification())
        createdAt.append(creation_date())

    data["name"] =name
    data["DOB"] = generate_birthdate()
    data["gender"] = gender
    data["email"] = email
    data["admissionsQuiz"] = admissionsQuiz
    data["countryISO2"] = countryISO2
    data["highestDegreeEarned"] = highestDegreeEarned
    data["createdAt"] = createdAt

    df = pd.DataFrame(data)
    
    if not os.path.exists("data/students_data.csv"):
          os.makedirs("data", exist_ok=True)
    df.to_csv("data/students_data.csv", index=False)

if __name__ =="__main__":
    generate_student_data(6000)