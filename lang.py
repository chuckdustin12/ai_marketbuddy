from langchain_experimental.agents.agent_toolkits.python.base import create_python_agent
from langchain_experimental.tools.python.tool import PythonREPLTool
from langchain_experimental.utilities.python import PythonREPL
from langchain.llms.openai import OpenAI
from langchain.agents.agent_types import AgentType
from langchain.chat_models import ChatOpenAI
import os
from helpers import convert_unix_to_eastern
import requests
from webull_endpoints import Webull
import asyncio
api_key = os.environ.get('YOUR_OPENAI_KEY')


agent_executor = create_python_agent(
    llm=OpenAI(temperature=0, max_tokens=1000, openai_api_key=api_key),
    tool=PythonREPLTool(),
    verbose=True,
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION
)
wb = Webull()
async def main():
    data = await wb.get_bars('AAPL', timeframe='m60')
    response = agent_executor.run(f'What was the trend today? {data}')

    print(response)


asyncio.run(main())


