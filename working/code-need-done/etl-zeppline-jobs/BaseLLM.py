import os
from openai import OpenAI
import re
import json


def safe_llm_result(text):
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    return text

def safe_json_extract(text):
    if not text:
        return None
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    text = re.sub(r"```json|```", "", text)

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None

    json_str = match.group(0)

    json_str = re.sub(r",\s*}", "}", json_str)
    json_str = re.sub(r",\s*]", "]", json_str)

    try:
        return json.loads(json_str)
    except Exception as e:
        print("❌ Still invalid JSON:", e)
        print("RAW:", json_str)
        return None
    
    
class BaseLLM:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("LLM_GENERATOR_API_KEY"),
            base_url=os.getenv("LLM_GENERATOR_URL")  # Groq endpoint
        )
        self.model = os.getenv("LLM_GENERATOR_MODEL")

    def chat(self, system_prompt: str, user_prompt: str, temperature=0) -> str:
        messages=[
                {"role": "user", "content": user_prompt}
        ]
        
        if system_prompt :
            messages += [
                {"role": "system", "content": system_prompt},
            ]
            
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature
        )
        return resp.choices[0].message.content
    
    
    def chat_json(self, user_prompt: str, system_prompt: str = None, temperature=0):
        text = None
        try:
            messages=[
                {"role": "user", "content": user_prompt}
            ]
            if system_prompt :
                messages += [
                {"role": "system", "content": system_prompt},
                ]
            response = self.client.chat.completions.create(
                model=self.model,  
                temperature=temperature,
                messages=messages
            )

            content = response.choices[0].message.content
            text = content
            return safe_json_extract(content)
        except Exception as e:
            print(f"❌ {self.model} via OpenAI SDK error:", str(e), text)
            raise e
        
    def next(self):
        pass