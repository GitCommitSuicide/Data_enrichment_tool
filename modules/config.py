import os
import sys
from dotenv import load_dotenv
from tavily import TavilyClient

load_dotenv()

TAVILY_API_KEY  = os.getenv("TAVILY_API_KEY")
OLLAMA_URL      = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "gpt-oss:120b-cloud")
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", 6))
MAX_CONTEXT     = int(os.getenv("MAX_CONTEXT", 100000))   # chars sent to model
OLLAMA_TIMEOUT  = int(os.getenv("OLLAMA_TIMEOUT", 120)) 
MAX_RESULTS_PER_QUERY = 12

if not TAVILY_API_KEY:
    sys.exit("[fatal] TAVILY_API_KEY is not set in .env")

tavily = TavilyClient(api_key=TAVILY_API_KEY)
