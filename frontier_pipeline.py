from prefect import flow, task
import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime
#from datetime import timedelta

@task
def scrape_frontier_data():
    url = 'http://frontierclimate.com/portfolio'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    infor = soup.find_all('div', class_='mb-4')

    company, description, tracks, contracted_tons, locations, pathways, links = [], [], [], [], [], [], []

    for info in infor:
        name = info.find('h3', class_='mb-3').get_text(strip=True)
        desc = info.find('p').get_text(" ", strip=True)
        track = info.find('dd').get_text(strip=True)
        ddtext = info.find_all('dd', class_='text-[14px]')
        contracted_ton = ddtext[0].get_text(strip=True)
        location = ddtext[1].get_text(strip=True)
        pathway = info.find('div', class_='text-black').get_text(strip=True) if info.find('div', class_='text-black') else None

        a_tag = info.find('a', class_='rounded')
        if a_tag:
            tag = str(a_tag.get('href'))
            links.append('https://frontierclimate.com' + tag)
        else:
            links.append('empty')

        company.append(name)
        description.append(desc)
        tracks.append(track)
        contracted_tons.append(contracted_ton)
        locations.append(location)
        pathways.append(pathway)

    soups = []

    for link in links:
        if link != 'empty':
            response = requests.get(link)
            soup = BeautifulSoup(response.text, 'html.parser')
            soups.append(soup)

        else: 
            soups.append('empty')
            
    tons, contract_value, deliverytimeline = [], [], []

    for soup in soups:
        if soup != 'empty':
            infor = soup.find_all('div', class_ = 'grid')
            for info in infor:
                contract = info.find_all('dd', class_ = 'text-[14px]')
                ton = contract[0].get_text(strip = True)
                value = contract[1].get_text(strip = True)
                timeline = contract[3].get_text(strip = True)

                
                tons.append(ton)
                contract_value.append(value)
                deliverytimeline.append(timeline)
        else: 
            contract_value.append(0)
            tons.append('empty')
            deliverytimeline.append(None)


    df = pd.DataFrame({
        'Company': company,
        'Description': description,
        'Track': tracks,
        'Contracted_tons': contracted_tons,
        'Location': locations,
        'Pathways': pathways,
        'Tons': tons,
        'Contracted value': contract_value,
        'Delivery timeline': deliverytimeline
    })


    df.to_csv('Frontier_data.csv', index=False)
    return df

#data = pd.read_csv('Frontier_data.csv')

@task
def clean_data(df):
    
    df['Contracted_tons'] = df['Contracted_tons'].str.replace(',', '')
    df['Contracted_tons'] = df['Contracted_tons'].str.replace('-', '0')
    df['Tons'] = df['Tons'].apply(lambda x: '0' if x == 'empty' else x.replace(',', '')) 
    df['Tons'] = df['Tons'].apply(lambda x: '0' if x == 'empty' else x.replace('-', '0')) 
    df['Tons'] = df['Tons'].astype('int32')
    df['Contracted_tons'] = df['Contracted_tons'].astype('int32')

    df.fillna('not specified', axis=1, inplace=True)
    df.to_csv('cleaned_frontier.csv', index=False)
    return df

@flow(name="frontierpipeline")
def frontier_pipeline():
    print(f" Flow started at {datetime.datetime.now()}")
    raw_df = scrape_frontier_data()
    cleaned_df = clean_data(raw_df)
    print(" Flow finished successfully")

if __name__ == "__main__":
    frontier_pipeline.serve(name = 'frontier_deployment', 
                            interval  = 3600)

#Check out the dashboard at http://127.0.0.1:4200
'''
prefect server start

run the script and create a work pool

'''
