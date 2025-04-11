# import libraries
from prefect import flow, task
import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime
from datetime import timedelta

# declare tasks
@task
def scrape_frontier_data():
    # url
    url = 'http://frontierclimate.com/portfolio'

    # get html
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    infor = soup.find_all('div', class_='mb-4')

    # data lists
    company, description, tracks, contracted_tons, locations, pathways = [], [], [], [], [], []

    for info in infor:
        # get data
        name = info.find('h3', class_='mb-3').get_text(strip=True)
        desc = info.find('p').get_text(" ", strip=True)
        track = info.find('dd').get_text(strip=True)
        ddtext = info.find_all('dd', class_='text-[14px]')
        contracted_ton = ddtext[0].get_text(strip=True)
        location = ddtext[1].get_text(strip=True)
        pathway = info.find('div', class_='text-black').get_text(strip=True) if info.find('div', class_='text-black') else None

        # append data to list
        company.append(name)
        description.append(desc)
        tracks.append(track)
        contracted_tons.append(contracted_ton)
        locations.append(location)
        pathways.append(pathway)

    # put data in dataframe
    df = pd.DataFrame({
        'Company': company,
        'Description': description,
        'Track': tracks,
        'Contracted_tons': contracted_tons,
        'Location': locations,
        'Pathways': pathways
    })

    # save to csv
    df.to_csv('Frontier_data.csv', index=False)
    return df

@task
def clean_data(df):
    # convert to numeric
    df['Contracted_tons'] = df['Contracted_tons'].str.replace(',', '')
    df['Contracted_tons'] = df['Contracted_tons'].str.replace('-', '0')
    df['Contracted_tons'] = df['Contracted_tons'].astype('int32')
    df.to_csv('cleaned_frontier.csv', index=False)
    return df

# declare flow
@flow(name="frontierpipeline")
def frontier_pipeline():
    print(f" Flow started at {datetime.datetime.now()}")
    raw_df = scrape_frontier_data()
    cleaned_df = clean_data(raw_df)
    print(" Flow finished successfully")

#call flow
if __name__ == "__main__":
    frontier_pipeline()

#Check out the dashboard at http://127.0.0.1:4200
'''
prefect server start


prefect work-pool create frontier-pool --type process

prefect deploy frontier_pipeline.py:frontierpipeline --name daily-frontier --pool frontier-pool --cron "0 0 * * *"

prefect worker start --pool frontier-pool

prefect deployment run frontierpipeline/daily-frontier

prefect flow-run ls
'''