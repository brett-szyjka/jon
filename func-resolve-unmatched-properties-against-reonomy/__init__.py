import logging
import requests
import json
import time
import csv
import io
import os

import azure.functions as func
from azure.storage.queue import QueueClient

# app settings: reonomy file match url
reonomy_match_url = "https://api.reonomy.com/v2/jobs/file"
reonomy_auth_header = os.environ['reonomy_authentication_header_raw']
storage_account_url_prefix = "https://stobsvutil.blob.core.windows.net"
headers = {"Authorization": reonomy_auth_header}
pending_check_delay_sec = 5

queue = QueueClient.from_connection_string(os.environ['storage_account_connection_string'], os.environ['resolved_matches_queue_name'])

def submit_match_job_to_reonomy(download_url) -> str:
  resulting_id = ""
  file_content = requests.get(download_url).content
  file_bytes = io.BytesIO(file_content)
  file_name = download_url.split("/")[-1]
  files = { "file": (file_name, file_bytes) }
  data = {
    "job_type": "match"
    , "metadata": json.dumps({
      "headers": [
      "address1"
        , "city"
        , "state"
        , "zip"
        , "salesforce_id"
      ],
      "mapping": {
        "address_line_1": "address1"
          , "address_city": "city"
          , "address_state": "state"
          , "address_postal": "zip"
        }
      })
  }
  try:
    response = requests.post(reonomy_match_url, data=data, files=files, headers=headers).json()
    resulting_id = response["id"]
  except KeyError:
    logging.error(f"{response['message']}")
  finally:
    return resulting_id

def poll_for_match_job_completion(match_job_id) -> (str, str):
  response = requests.get(f"{reonomy_match_url}/{match_job_id}", headers=headers).json()
  check_status = response['status']
  if (check_status == "SUCCESS"):
    return (check_status, response['result_url'])
  else:
    return (check_status, "")

def enqueue_match_result(salesforce_id, reonomy_id):
  queue.send_message(json.dumps({"salesforce_id": salesforce_id, "reonomy_id": reonomy_id}))
  return

def process_match_results(result_url) -> int:
  counter = 0
  response = requests.get(result_url).content.decode('utf-8')
  for row in list(csv.reader(response.splitlines()[1:], delimiter=',')):
    if (row[1] != 'MISS'):
      enqueue_match_result(row[7], row[0])
      counter += 1
  return counter

def main(msg: func.QueueMessage) -> None:
  obj = json.loads(msg.get_body().decode('utf-8'))
  file_path = obj['filepath']
  logic_app_workflow_id = (file_path.split("/")[-1]).split(".",1)[0]
  logging.info(f"started processing logic app workflow id: {logic_app_workflow_id}")
  download_url = f"{storage_account_url_prefix}{file_path}"

  job_id = submit_match_job_to_reonomy(download_url)
  if (len(job_id) == 0):
    logging.error(f"exiting execution triggered by logic app workflow id: {logic_app_workflow_id}")
    return
  logging.info(f"started reonomy match job id: {job_id} ")
  
  while (True):
    time.sleep(pending_check_delay_sec)
    (status, result_url) = poll_for_match_job_completion(job_id)
    if (status == 'SUCCESS'):
      logging.info(f"completed reonomy match job id: {job_id}")
      match_count = process_match_results(result_url)
      logging.info(f"enqueued {match_count} reonomy id matches")
      break

  logging.info(f"completed processing logic app workflow id: {logic_app_workflow_id}")
