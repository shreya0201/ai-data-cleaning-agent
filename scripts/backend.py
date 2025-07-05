import sys
import os
import pandas as pd
import io
import aiohttp
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from sqlalchemy import create_engine
from pydantic import BaseModel
import requests

# Ensure the scripts folder is in Python's path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from scripts.ai_agent import AIAgent
from scripts.data_cleaning import DataCleaning

app = FastAPI()

ai_agent = AIAgent()
cleaner = DataCleaning()

@app.post("/clean-data")
async def clean_data(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        file_extension = file.filename.split(".")[-1]

        print(f"✅ File received: {file.filename}")
        print(f"✅ File extension: {file_extension}")

        if file_extension == "csv":
            df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
        elif file_extension == "xlsx":
            df = pd.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Use CSV or Excel.")

        print(f"✅ Original DF shape: {df.shape}")

        df_cleaned = cleaner.clean_data(df)
        print(f"✅ After rule-based cleaning: {df_cleaned.shape}")

        df_ai_cleaned = ai_agent.process_data(df_cleaned)

        from io import StringIO
        if isinstance(df_ai_cleaned, str):
            df_ai_cleaned = pd.read_csv(StringIO(df_ai_cleaned))

        return {"cleaned_data": df_ai_cleaned.to_dict(orient="records")}

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

class DBQuery(BaseModel):
    db_url: str
    query: str

@app.post("/clean-db")
async def clean_db(query: DBQuery):
    try:
        engine = create_engine(query.db_url)
        df = pd.read_sql(query.query, engine)
        df_cleaned = cleaner.clean_data(df)
        df_ai_cleaned = ai_agent.process_data(df_cleaned)

        from io import StringIO
        if isinstance(df_ai_cleaned, str):
            df_ai_cleaned = pd.read_csv(StringIO(df_ai_cleaned))

        return {"cleaned_data": df_ai_cleaned.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from database: {str(e)}")

class APIRequest(BaseModel):
    api_url: str

@app.post("/clean-api")
async def clean_api(api_request: APIRequest):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_request.api_url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=400, detail="Failed to fetch data from API.")
                
                data = await response.json()
                df = pd.DataFrame(data)
                df_cleaned = cleaner.clean_data(df)
                df_ai_cleaned = ai_agent.process_data(df_cleaned)

                from io import StringIO
                if isinstance(df_ai_cleaned, str):
                    df_ai_cleaned = pd.read_csv(StringIO(df_ai_cleaned))

                return {"cleaned_data": df_ai_cleaned.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing API data: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)



