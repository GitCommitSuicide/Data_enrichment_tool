import os
import sys
from dotenv import load_dotenv
from tavily import TavilyClient

load_dotenv()
# kimi-k2:1t:cloud   - not working
# deepseek-v3.1:671b-cloud -paid
#   kimi-k2-thinking:cloud -paid
#  mistral-large-3:675b-cloud - paid


# qwen3-coder:480b-cloud
# gpt-oss:120b-cloud
# nemotron-3-super:cloud


OLLAMA_URL     = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL", "ministral-3:14b-cloud")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", 600))

TAVILY_API_KEY        = os.getenv("TAVILY_API_KEY")
MAX_RESULTS_PER_QUERY = int(os.getenv("MAX_RESULTS_PER_QUERY", 10))
MAX_CONTEXT           = int(os.getenv("MAX_CONTEXT", 135_000))

if not TAVILY_API_KEY:
    sys.exit("[fatal] TAVILY_API_KEY is not set in .env")

tavily = TavilyClient(api_key=TAVILY_API_KEY)