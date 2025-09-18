from tiingo import TiingoClient
from .config import settings


tiingo_client = TiingoClient({"api_key": settings.TIINGO_API_KEY})