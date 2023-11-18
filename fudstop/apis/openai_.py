import os
from dotenv import load_dotenv
import requests
import base64
load_dotenv()

from openai import OpenAI


class OpenAISDK:
    def __init__(self):
        self.key = os.environ.get('YOUR_OPENAI_KEY')
        self.client = OpenAI(api_key=self.key)

    def analyze_stock(self, url):
        

        response = self.client.chat.completions.create(
        model="gpt-4-vision-preview",
        messages=[
            {
            "role": "user",
            "content": [
                {"type": "text", "text": "Analyze the image. If it's of a stock chart - analyze in an attempt to find any meaningful patterns."},
                {
                "type": "image_url",
                "image_url": {
                    "url": f"{url}",
                    "detail": "high"
                },
                },
            ],
            }
        ],
        max_tokens=300,
        )

        print(response.choices[0].message.content)
        return response.choices[0].message.content

