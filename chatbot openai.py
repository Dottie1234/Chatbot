
import pandas as pd
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.llms import OpenAI
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get OpenAI API
openai_api_key = os.getenv("OPENAI_API_KEY")

if openai_api_key is None:
    print("Error: OPENAI_API_KEY not found in environment.")
    exit()

# Load your CSV file
csv_path = "cleaned frontier.csv" 
df = pd.read_csv(csv_path)

# Create the LangChain agent
agent = create_pandas_dataframe_agent(OpenAI(temperature=0, openai_api_key=openai_api_key), 
                                      df, 
                                      verbose=True,
                                      allow_dangerous_code=True)

# Chat
print("CSV Chatbot is ready. Ask questions about your dataset! (type 'exit' to quit)\n")

while True:
    query = input("You: ")
    if query.lower() in ["exit", "quit"]:
        print("Goodbye!")
        break
    try:
        response = agent.run(query)
        print("Bot:", response)
    except Exception as e:
        print("Error:", e)
