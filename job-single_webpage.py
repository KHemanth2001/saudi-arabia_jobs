import requests
import csv
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

def fetch_job_ids(url):
    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            job_ids = []

            job_elements = soup.find_all('li', class_='has-pointer-d')
            for job_element in job_elements:
                job_id = job_element.get("data-job-id")
                if job_id:
                    job_ids.append(job_id)

            return job_ids

        else:
            print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
            return []

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return []

def fetch_data_for_job_id(job_id):
    try:
        url = f'https://www.bayt.com/en/job/{job_id}/'
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            dt_dd_mapping = {'Job ID': [job_id]}

            job_elements = soup.find_all('dl', class_='dlist is-spaced is-fitted t-small')

            for job_element in job_elements:
                dt_elements = job_element.find_all('dt')
                dd_elements = job_element.find_all('dd')

                for dt, dd in zip(dt_elements, dd_elements):
                    dt_name = dt.text.strip()
                    dd_text = dd.text.strip()

                    if dt_name in dt_dd_mapping:
                        dt_dd_mapping[dt_name].append(dd_text)
                    else:
                        dt_dd_mapping[dt_name] = [dd_text]

            return dt_dd_mapping

        else:
            print(f"Failed to retrieve data for Job ID: {job_id}. Status code: {response.status_code}")
            return {}

    except Exception as e:
        print(f"Error occurred while fetching data for Job ID: {job_id}. {str(e)}")
        return {}

def save_to_csv(data, csv_filename):
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = list(data.keys())
            max_length = max(len(data[fieldname]) for fieldname in fieldnames)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for i in range(max_length):
                row = {fieldname: data[fieldname][i] if i < len(data[fieldname]) else '' for fieldname in fieldnames}
                writer.writerow(row)
        print(f"Data has been successfully saved to '{csv_filename}'.")

    except Exception as e:
        print(f"Error occurred while saving to CSV: {str(e)}")

if __name__ == '__main__':
    url = 'https://www.bayt.com/en/saudi-arabia/jobs/'
    job_ids = fetch_job_ids(url)

    if job_ids:
        all_data = {}
        for job_id in job_ids:
            dt_dd_mapping = fetch_data_for_job_id(job_id)
            for dt_name, dd_list in dt_dd_mapping.items():
                if dt_name in all_data:
                    all_data[dt_name].extend(dd_list)
                else:
                    all_data[dt_name] = dd_list

        csv_filename = "all_job_data.csv"
        save_to_csv(all_data, csv_filename)
    else:
        print("No job IDs found.")
