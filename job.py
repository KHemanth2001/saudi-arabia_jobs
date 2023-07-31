import requests
import csv
import time
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

def fetch_job_ids(url, max_pages=2):
    try:
        all_job_ids = []
        page = 1

        while page <= max_pages:
            response = goto_next_page(url, page)

            if response is not None:
                soup = BeautifulSoup(response, 'html.parser')
                job_elements = soup.find_all('li', class_='has-pointer-d')
                if not job_elements:
                    print("No job IDs found.")
                    break  # No more job IDs to fetch
                for job_element in job_elements:
                    job_id = job_element.get("data-job-id")
                    if job_id:
                        all_job_ids.append(job_id)

                page += 1
                time.sleep(1)  # Add a delay to avoid overloading the website
            else:
                print(f"Failed to fetch data from the next page. Exiting the loop.")
                break

        return all_job_ids

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return []

def goto_next_page(url, page):
    try:
        response = requests.get(url + f'?page={page}', headers=headers, timeout=30)  # Increase timeout to 30 seconds

        if response.status_code == 200:
            return response.content

        print(f"Failed to retrieve data from {url} (Page: {page}). Status code: {response.status_code}")
        return None

    except requests.Timeout:
        print(f"Request timed out while fetching data from {url} (Page: {page}).")
        return None

    except Exception as e:
        print(f"Error occurred while fetching data from {url} (Page: {page}). {str(e)}")
        return None

def fetch_data_for_job_id(job_id, field_names):
    try:
        url = f'https://www.bayt.com/en/job/{job_id}/'
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            dt_dd_mapping = {'Job ID': job_id}

            job_elements = soup.find_all('dl', class_='dlist is-spaced is-fitted t-small')

            for job_element in job_elements:
                dt_elements = job_element.find_all('dt')
                dd_elements = job_element.find_all('dd')

                for dt, dd in zip(dt_elements, dd_elements):
                    dt_name = dt.text.strip()
                    dd_text = dd.text.strip()
                    dt_dd_mapping[dt_name] = dd_text
                    field_names.add(dt_name)  # Add new field names to the set

            return dt_dd_mapping

        else:
            print(f"Failed to retrieve data for Job ID: {job_id}. Status code: {response.status_code}")
            return {}

    except Exception as e:
        print(f"Error occurred while fetching data for Job ID: {job_id}. {str(e)}")
        return {}

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

if __name__ == '__main__':
    url = 'https://www.bayt.com/en/saudi-arabia/jobs/'
    job_ids = fetch_job_ids(url)

    if job_ids:
        all_data = []
        field_names = set()  # Set to store all unique field names

        for job_id in job_ids:
            dt_dd_mapping = fetch_data_for_job_id(job_id, field_names)
            all_data.append(dt_dd_mapping)

        # Update field names set based on all_data
        field_names.update(field for dt_dd_mapping in all_data for field in dt_dd_mapping.keys())

        # Add missing fields with empty values to all_data
        for dt_dd_mapping in all_data:
            for field_name in field_names:
                if field_name not in dt_dd_mapping:
                    dt_dd_mapping[field_name] = ''

        csv_filename = "all_job_data.csv"
        save_to_csv(all_data, csv_filename)
    else:
        print("No job IDs found.")
