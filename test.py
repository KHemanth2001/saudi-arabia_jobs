import os
import csv
import time
import datetime
import requests
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}
DIR_PATH = os.path.abspath(os.path.dirname(__file__))
folder_name = "_Output"
file_name = 'all_job_data.csv'


# Function: goto_next_page
def goto_next_page(url, page, sort_option=None):
    try:
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,  # Increase the delay to 1 second
            status_forcelist=[429, 500, 502, 503, 504],  # Include 429 error in retry list
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        if sort_option:
            url += f'?sort={sort_option}'

        response = session.get(url + f'&page={page}', headers=headers)

        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        return response.content

    except requests.RequestException as e:
        print(f"Error occurred while fetching data from {url} (Page: {page}). {str(e)}")
        raise e

    except Exception as e:
        print(f"Unexpected error occurred while fetching data from {url} (Page: {page}). {str(e)}")
        raise e


# Function: fetch_job_ids (Modified for asynchronous requests)
async def fetch_job_ids_async(url, page, sort_option=None):
    response = goto_next_page(url, page, sort_option)
    if response is not None:
        soup = BeautifulSoup(response, 'html.parser')
        job_elements = soup.find_all('li', class_='has-pointer-d')
        job_ids = [job_element.get("data-job-id") for job_element in job_elements if job_element.get("data-job-id")]
        job_dates = [job_element.find('time', class_='has-no-wrap').get('datetime') for job_element in job_elements]
        job_dates = [datetime.datetime.strptime(date_str, "%Y-%m-%d") for date_str in job_dates]

        # Get the current date
        current_date = datetime.datetime.now()

        # Fetch only the job IDs that were posted within the past 7 days
        recent_job_ids = [job_id for job_id, job_date in zip(job_ids, job_dates) if (current_date - job_date).days <= 7]

        print(f"Job IDs fetched from Page {page}")
        return recent_job_ids
    else:
        print(f"Failed to fetch data from the next page (Page: {page}).")


async def fetch_all_job_ids(url, sort_option=None):
    page = 1
    all_job_ids = []
    while True:
        job_ids = await fetch_job_ids_async(url, page, sort_option)
        if not job_ids:
            break
        all_job_ids.extend(job_ids)
        page += 1
        await asyncio.sleep(3)  # Add a delay of 3 seconds between page requests
    return all_job_ids


# Function: fetch_data_for_job_id
async def fetch_data_for_job_id(job_id, field_names, session):
    try:
        url = f'https://www.bayt.com/en/job/{job_id}/'
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                soup = BeautifulSoup(await response.content.read(), 'html.parser')
                details_desc_mapping = {'Job ID': job_id}

                job_elements = soup.find_all('dl', class_='dlist is-spaced is-fitted t-small')

                for job_element in job_elements:
                    job_attributes = job_element.find_all('dt')
                    job_desc = job_element.find_all('dd')

                    for title, data in zip(job_attributes, job_desc):
                        title_name = title.text.strip()
                        data_text = data.text.strip()
                        details_desc_mapping[title_name] = data_text
                        field_names.add(title_name)  # Add new field names to the set

                return details_desc_mapping

            else:
                print(f"Failed to retrieve data for Job ID: {job_id}. Status code: {response.status}")
                return {}
    except Exception as e:
        print(f"Error occurred while fetching data for Job ID: {job_id}. {str(e)}")
        return {}


# Function: save_to_csv
def save_to_csv(all_data, csv_filename):
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = list(all_data[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_data)
        print(f"Data has been successfully saved to '{csv_filename}'.")

    except Exception as e:
        print(f"Error occurred while saving to CSV: {str(e)}")


# Main function
async def main():
    url = 'https://www.bayt.com/en/saudi-arabia/jobs/'
    sort_option = 'date'  # Replace 'date' with the desired sorting criteria, e.g., 'relevance', 'salary', etc.
    job_ids = await fetch_all_job_ids(url, sort_option)

    if job_ids:
        all_data = []
        field_names = set()  # Set to store all unique field names

        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [fetch_data_for_job_id(job_id, field_names, session) for job_id in job_ids]
            details_desc_mappings = await asyncio.gather(*tasks)

        for details_desc_mapping in details_desc_mappings:
            if details_desc_mapping:
                all_data.append(details_desc_mapping)

        field_names.update(field for details_desc_mapping in all_data for field in details_desc_mapping.keys())

        for details_desc_mapping in all_data:
            for field_name in field_names:
                if field_name not in details_desc_mapping:
                    details_desc_mapping[field_name] = ''

        folder_name = "_Output"
        path = os.path.join(DIR_PATH, folder_name)
        try:
            os.mkdir(path)
        except OSError as error:
            print(error)
        csv_filename = "all_job_data.csv"
        save_to_csv(all_data, os.path.join(path, csv_filename))
    else:
        print("No job IDs found.")


if __name__ == '__main__':
    asyncio.run(main())
