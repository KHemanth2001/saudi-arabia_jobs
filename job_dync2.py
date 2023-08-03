import os.path
import requests
import csv
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import asyncio
from random import uniform
from fake_useragent import UserAgent

ua = UserAgent()

headers = {
    'User-Agent': ua.random
}

DIR_PATH = os.path.abspath(os.path.dirname(__file__))
folder_name = "_Output"
file_name = 'all_job_data.csv'


# ... (Previous code remains the same)

# ... (Previous code remains the same)

def fetch_job_ids(url):
    try:
        all_job_ids = set()
        page = 1
        prev_job_ids = set()

        while True:
            response = goto_next_page(url, page)

            if response is not None:
                soup = BeautifulSoup(response, 'html.parser')
                job_elements = soup.find_all('li', class_='has-pointer-d')
                if not job_elements:
                    print("No job IDs found.")
                    break  # No more job IDs to fetch

                job_ids = {job_element.get("data-job-id") for job_element in job_elements}
                if job_ids == prev_job_ids:
                    print(f"Reached the last page. Stopping data fetching.")
                    break  # Break the loop if the current page's job IDs are the same as the previous page's

                prev_job_ids = job_ids
                all_job_ids.update(job_ids)

                print(f"Data extracted from page {page}")  # Statement to be executed after extracting data
                page += 1
                time.sleep(uniform(1, 2))  # Add a random delay (1-2 seconds) to avoid overloading the website
            else:
                print(f"Failed to fetch data from page {page}. Exiting the loop.")
                break

        return list(all_job_ids)

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return []

# ... (Rest of the code remains the same)


# ... (Rest of the code remains the same)


def goto_next_page(url, page, retries=3, backoff_factor=2, last_page_content=None):
    try:
        headers['User-Agent'] = ua.random  # Rotate user-agent
        response = requests.get(url + f'?page={page}', headers=headers, timeout=30)  # Increase timeout to 30 seconds

        if response.status_code == 200:
            content = response.content
            if content == last_page_content:
                print(f"Reached the last page. Stopping data fetching.")
                return None  # Return None to indicate the last page
            return content
        elif response.status_code == 429 and retries > 0:
            retry_after = int(response.headers.get('Retry-After', 5))
            print(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return goto_next_page(url, page, retries - 1, backoff_factor * 2, last_page_content)
        elif response.status_code == 404:
            print(f"Reached the last page. Stopping data fetching.")
            return None  # Return None to indicate the last page

        print(f"Failed to retrieve data from {url} (Page: {page}). Status code: {response.status_code}")
        return None

    except requests.Timeout:
        print(f"Request timed out while fetching data from {url} (Page: {page}).")
        return None

    except Exception as e:
        print(f"Error occurred while fetching data from {url} (Page: {page}). {str(e)}")
        return None



def fetch_data_for_job_id(job_id, retries=3, backoff_factor=2):
    try:
        headers['User-Agent'] = ua.random  # Rotate user-agent
        url = f'https://www.bayt.com/en/job/{job_id}/'
        with requests.Session() as session:
            response = session.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
            details_desc_mapping = {}

            # Extract the job name and add it to the dictionary
            job_name_element = soup.find('h1', class_='h3')
            job_name = job_name_element.text.strip() if job_name_element else ''
            details_desc_mapping['Job ID'] = job_id
            details_desc_mapping['Job Name'] = job_name

            job_elements = soup.find_all('dl', class_='dlist is-spaced is-fitted t-small')

            for job_element in job_elements:
                job_attributes = job_element.find_all('dt')
                job_desc = job_element.find_all('dd')

                for title, data in zip(job_attributes, job_desc):
                    title_name = title.text.strip()
                    data_text = data.text.strip()
                    details_desc_mapping[title_name] = data_text

            return details_desc_mapping
        elif response.status_code == 429 and retries > 0:
            retry_after = int(response.headers.get('Retry-After', 5))
            print(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return fetch_data_for_job_id(job_id, retries - 1, backoff_factor * 2)

        print(f"Failed to retrieve data for Job ID: {job_id}. Status code: {response.status_code}")
        return {}

    except Exception as e:
        print(f"Error occurred while fetching data for Job ID: {job_id}. {str(e)}")
        return {}


async def main():
    url = 'https://www.bayt.com/en/saudi-arabia/jobs/'
    job_ids = fetch_job_ids(url)

    if job_ids:
        all_data = []
        field_names = set()  # Set to store all unique field names

        with ThreadPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            futures = [loop.run_in_executor(executor, fetch_data_for_job_id, job_id) for job_id in job_ids]

            for result in await asyncio.gather(*futures):
                all_data.append(result)
                field_names.update(result.keys())  # Update field names set with each job data's keys

        # Add missing fields with empty values to all_data
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
        csv_path = os.path.join(path, csv_filename)

        # Save to CSV
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                field_names = ['Job ID', 'Job Name'] + sorted(field_names - {'Job ID', 'Job Name'})
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(all_data)
            print(f"Data has been successfully saved to '{csv_path}'.")
        except Exception as e:
            print(f"Error occurred while saving to CSV: {str(e)}")

    else:
        print("No job IDs found.")


if __name__ == '__main__':
    asyncio.run(main())
