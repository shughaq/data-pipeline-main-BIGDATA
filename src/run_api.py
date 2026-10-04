import uvicorn
from config.settings import API_HOST, API_PORT

if __name__ == "__main__":
    uvicorn.run("src.final_api:app", host=API_HOST, port=API_PORT, reload=False)
