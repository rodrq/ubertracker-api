import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
import boto3
from boto3.dynamodb.conditions import Key

app = FastAPI()

load_dotenv('.env')

REGION_NAME = os.getenv('REGION_NAME')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
TABLE_NAME = os.getenv('TABLE_NAME')

def get_table():
    session = boto3.Session(aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY, region_name = REGION_NAME)
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)
    return table

@app.get("/")
async def root():
    return {"message": "Hello world"}

@app.get("/get-fare/{day}") #Hour optional
async def get_fare(day: str, hour: str = None):
    table = get_table()
    if hour:
        response = table.query(
            KeyConditionExpression=Key('date').eq(day) & Key('time').eq(hour)
        )
    else:
        response = table.query(
            KeyConditionExpression=Key('date').eq(day)
        )
    items = response.get("Items")
    if not items:
        raise HTTPException(status_code=404, detail=f"Fare not found for {day} at {hour}")
    return items