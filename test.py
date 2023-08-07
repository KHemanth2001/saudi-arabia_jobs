import os.path
import requests
import csv
import time
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

DIR_PATH = os.path.abspath(os.path.dirname(__file__))
folder_name = "_Output"
file_name = 'all_job_data.csv'

def goto_next_page(url, page):
    try:
        response = requests.get(url + f'?page={page}', headers=headers, timeout=30)
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

def fetch_job_ids(url):
    try:
        all_job_ids = []
        page = 1
        prev_page_content = None

        while True:
            response = goto_next_page(url, page)

            if response is not None:
                soup = BeautifulSoup(response, 'html.parser')
                job_elements = soup.find_all('li', class_='has-pointer-d')
                if not job_elements:
                    print("No job IDs found.")
                    break  # No more job IDs to fetch

                # Check if the current page content is the same as the previous page
                current_page_content = str(response)
                if current_page_content == prev_page_content:
                    print("Reached the last page of job listings.")
                    break

                for job_element in job_elements:
                    job_id = job_element.get("data-job-id")
                    if job_id:
                        all_job_ids.append(job_id)

                print(f"Fetched job IDs from Page {page}")
                page += 1
                prev_page_content = current_page_content
                time.sleep(1)  # Add a delay to avoid overloading the website
            else:
                print(f"Failed to fetch data from the next page. Exiting the loop.")
                break

        return all_job_ids
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return []

def save_job_ids_to_csv(job_ids):
    try:
        output_folder_path = os.path.join(DIR_PATH, folder_name)
        os.makedirs(output_folder_path, exist_ok=True)

        output_file_path = os.path.join(output_folder_path, file_name)

        with open(output_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Job ID'])
            for job_id in job_ids:
                writer.writerow([job_id])

        print(f"Job IDs saved to: {output_file_path}")
    except Exception as e:
        print(f"Error occurred while saving to CSV: {str(e)}")

if __name__ == "__main__":
    url = 'https://www.example.com/joblistings'  # Replace this with the actual URL of the job listing website

    job_ids = fetch_job_ids(url)
    if job_ids:
        save_job_ids_to_csv(job_ids)
