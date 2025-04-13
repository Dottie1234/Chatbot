# Chatbot


# Frontier Climate Scraper + CSV Chatbot

This project scrapes climate tech project data from [Frontier Climate's Portfolio page](http://frontierclimate.com/portfolio), cleans and stores it into a CSV, then allows users to query the data using an LLM-powered chatbot (via LangChain and OpenAI).

---

## Website Scraped

- **URL:** https://frontierclimate.com/portfolio
- Scrapes company/project data from the main portfolio page and each individual project’s subpage.

---

## Columns Extracted

From the **main page**:
- `Company`: Name of the organization
- `Description`: Short description of the project
- `Track`: Procurement track (e.g., Advance Market Commitment)
- `Contracted_tons`: Number of contracted carbon removal tons
- `Location`: Country or region of the project
- `Pathways`: Type of carbon removal pathway (e.g., Direct Air Capture)

From **individual project pages**:
- `Tons`: Total tons purchased in the contract
- `Contracted value`: Financial value of the contract
- `Delivery timeline`: Delivery period for the carbon removal

PS: some of the projects do not have a link to an individual page.
---

## Data Cleaning & Storage
- filled the empty individual pages with NaN and 'empty'
- Commas removed from numeric fields
- Missing values filled with `'not specified'`
- All ton values converted to integers
- Final cleaned data saved as: `cleaned_frontier.csv`

---

## Automation with Prefect

The scraping and cleaning are orchestrated using [**Prefect**](https://www.prefect.io/):

- Tasks: `scrape_frontier_data()`, `clean_data()`
- Flow: `frontier_pipeline()`

```python
frontier_pipeline.serve(name='frontier_deployment', interval=3600)


