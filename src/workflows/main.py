# %%
import os
import dotenv
import requests
import json

dotenv.load_dotenv(".env")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def list_job_names(directory):
    return [i.replace(".json", "") for i in os.listdir(f"./{directory}") if i.endswith(".json")]


def load_settings(job_name, directory):
    with open(f"./{directory}/{job_name}.json", "r") as open_file:
        settings = json.load(open_file)
    return settings


def reset_job(settings):
    url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/reset"
    header = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.post(url=url, headers=header, json=settings)
    return resp

def create_job(settings):
    url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/create"
    header = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.post(url=url, headers=header, json=settings)
    return resp

def main():
    for i in list_job_names(directory="reset"):
        settings = load_settings(job_name=i, directory="reset")
        resp = reset_job(settings=settings)
        if resp.status_code == 200:
            print(f"Job '{i}' processado com sucesso!")
        else:
            print(f"Error: {resp.text}")

if __name__ == "__main__":
    main()