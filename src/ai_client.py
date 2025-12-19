import os
from google import genai
from dotenv import load_dotenv
from prompts import TRANSACTION_ANALYSIS_PROMPT

load_dotenv()

class GeminiClient:
    def __init__(self):
       
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in .env file")
        
    
        self.client = genai.Client(
            api_key=api_key,
            http_options={'api_version': 'v1beta'}
        )
        
        self.model_id = "gemini-2.0-flash-lite"

    def analyze_data(self, combined_text: str):
      
        full_prompt = TRANSACTION_ANALYSIS_PROMPT.format(data=combined_text)
        
        response = self.client.models.generate_content(
            model=self.model_id,
            contents=full_prompt
        )
        return response.text
    
    
