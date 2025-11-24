import os
import random
import pandas as pd
from datetime import timedelta
from typing import List
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv(".venv")

class MongoRepository:

    def __init__(self, db_username = os.environ.get("db_username"),
                       db_password = os.environ.get("db_password"), 
                       database = "data_science", 
                       collection ="wqu_ab_testing",
                       cluster_url = "cluster0.xtxfmd5.mongodb.net/sff.mongodb.net/"
                ):

            """
            parameters
            -----------
            db_username & db_password: your mongodb credentials where your data is.   
            database, collection, and cluster_url: Must given as arguments.
                       
            """
            
            if not db_username or not db_password: 
                raise RuntimeError("There's a problem with your logging credentials. Please double check if they're correct.")   
            
            self.client = MongoClient(f"mongodb+srv://{db_username}:{db_password}@{cluster_url}")
            self.collection = self.client[database][collection]

    def find_by_date(self, date_string:str)->List:

        """
        Extract documents created 60 days before the given date that hasn't completed the quiz.
        
        parameters
        ----------
        collection : list. collection of a database.
        date_string: "00-00-00"

        Returns
        --------
        observations: list
        list of students who haven't completed the quiz.
        """

        start_date = pd.to_datetime(date_string)

        end_date = start_date + timedelta(days=60)

        query = {"createdAt":{"$gte":start_date, "$lt":end_date}, "admissionsQuiz": "incomplete"}

        result = self.collection.find(query)

        observations = list(result)

        return observations

    def update_applicants(self, observations_assigned:list)->dict:

        """
        Parameters
        ----------
        collection : collection.
        observations_assigned: List of students who have been assigned to the experiments.

        Returns
        -------
        transaction_list: dict. It has a number of modified documents.

        """    

        # initializing variables
        n = 0
        n_modified = 0

        for obs in observations_assigned:
            result = self.collection.update_many(
                filter = {"_id": obs["_id"]},
                update = {"$set": obs}
            )
            
            n += result.matched_count
            n_modified += result.modified_count

        transaction_result = {"n":n, "n_modified":n_modified}

        return transaction_result

    def export_email(self, observations_assigned, directory="./data"):

        """
        This function takes a list of students who has been placed in the experiement. 
        Export emails of the students that are in the trearment group and send it to stakeholder to give it to the correct team to send these students a remainder email to complete the quiz.
        
        parameters:
        ----------
        observations_assigned : list

        Returns
        --------
        None
        """

        df = pd.DataFrame(observations_assigned)

        df =df[df["group"] == "email (treatment)"]

        df["tag"] ="ab-test"

        date_string = pd.Timestamp.now().strftime(format = "%Y-%m-%d")

        filename = directory + "/" + date_string + "_ab-test.csv"
        
        df[["email", "tag"]].to_csv(filename, index = False)

    def assign_to_group(self, date_string:str):

        """
        Parameters
        ----------
        observations: list of students.


        "This function takes a list of students who haven't completed the quiz,
        assign them to two different groups, control and treatment.
        
        """
        observations = self.find_by_date(date_string)

        random.seed(42)
        random.shuffle(observations)

        idx = len(observations)//2

        for doc in observations[:idx]:
            doc["inExperiement"] = True
            doc["group"] = "no email (control)"

        for doc in observations[idx:]:
            doc["inExperiement"] = True
            doc["group"] = "email (treatment)" 

        # Export list of students that will be sent emails.    
        self.export_email(observations, directory="./data")

        transaction_result = self.update_applicants(observations) 

        return transaction_result

if __name__=="__main__":
    repo = MongoRepository()
    transaction_result = repo.assign_to_group("2024-08-07")
    print(transaction_result)


 

