from fastapi import APIRouter, Request
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from datetime import datetime
from log_file import *
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.adminmodel import get_item_dtl_from_oracle,get_item_dtl_from_oracle_manual, OracleDataProcessor
from src.models.mysql.master_plant_model import plant_Lists
from typing import AsyncGenerator
import time


router = APIRouter()


@router.get("/api/get_item_and_rm_dtl_from_oracle_month/")
async def get_item_and_rm_dtl_from_oracle(cnx:AsyncGenerator=Depends(get_db)):
    try:
        start_time = time.time()
        createFolder("oracle_data/MONTH/",f"Month API call ")
        Hour_now = datetime.now().strftime("%H")
        P_TYPE = 'MONTH'
        if Hour_now == "01":
            createFolder("oracle_data/MONTH/",f"if Condition Executed")
            # res = await get_item_dtl_from_oracle(cnx,P_TYPE)
            process = OracleDataProcessor()
            await process.process_data(cnx, P_TYPE)
        end_time = time.time()
        createFolder("oracle_data/MONTH/",f"Process completed --> Duration: {end_time- start_time} Seconds")
    
    except Exception as e:
        return get_exception_response(e)
    
@router.get("/api/get_item_and_rm_dtl_from_oracle_day/")
async def get_item_and_rm_dtl_from_oracle(cnx:AsyncGenerator=Depends(get_db)):
    try:
        start_time = time.time()
        createFolder("oracle_data/DAY/",f"Day API call")
        Hour_now = datetime.now().strftime("%H:%M:%S")
        P_TYPE = 'DAY'
        if Hour_now >= "09:00:00" and Hour_now <= "09:05:00":
            createFolder("oracle_data/DAY/",f"if Condition Executed")
            # res = await get_item_dtl_from_oracle(cnx,P_TYPE)
            process = OracleDataProcessor()
            await process.process_data(cnx, P_TYPE)
        end_time = time.time()
        createFolder("oracle_data/DAY/",f"Process completed --> Duration: {end_time- start_time} Seconds")
    
    except Exception as e:
        return get_exception_response(e)

    
@router.get("/api/get_item_dtl_from_oracle_manual")
async def get_item_dtl_from_oracle_manual_api(P_TYPE:str = (''),cnx:AsyncGenerator=Depends(get_db)):
    try:
        start_time = time.time()
        createFolder(f"oracle_data/{P_TYPE}/",f"Manual API call")
        # res = await get_item_dtl_from_oracle(cnx,P_TYPE)
        process = OracleDataProcessor()
        await process.process_data(cnx, P_TYPE)
        end_time = time.time()
        createFolder(f"oracle_data/{P_TYPE}/",f"Process completed --> Duration: {end_time- start_time} Seconds")
    except Exception as e:
        return get_exception_response(e)
    
@router.get("/api/get_item_dtl_from_oracle_manual_new")
async def get_item_dtl_from_oracle_manual_api_new(P_TYPE:str = (''),cnx:AsyncGenerator=Depends(get_db)):
    try:
        start_time = time.time()
        createFolder(f"oracle_data/{P_TYPE}/",f"Manual API call")

        process = OracleDataProcessor()
        await process.process_data(cnx, P_TYPE)
        end_time = time.time()
        createFolder(f"oracle_data/{P_TYPE}/",f"Process completed --> Duration: {end_time- start_time} Seconds")
    except Exception as e:
        return get_exception_response(e)

# @router.get("/api/get_item_dtl_from_oracle_manual_insert")
# async def get_item_dtl_from_oracle_manual_insert(P_TYPE:str = (''),cnx:AsyncGenerator=Depends(get_db)):
#     try:
#         createFolder("oracle_data/",f"Manual API call")
#         res = await get_item_dtl_from_oracle_manual(cnx,P_TYPE)
       
#     except Exception as e:
#         return get_exception_response(e)




# tj
# def post_ems_jv(data: dict):
#     # Connect to Oracle
#     connection = connect_to_oracle()
    
#     # Prepare the JSON data
#     p_obj_json = json.dumps({"ems": data})
    
#     # Call the Oracle procedure
#     cursor = connection.cursor()
#     cursor.callproc("APPS.TI_IIOT_PKG.POST_EMS_JV", [p_obj_json, cursor])
    
#     # Commit the transaction
#     connection.commit()
    
#     # Fetch the output record set
#     jv_rset = cursor.fetchall()
    
#     connection.close()
    
#     # Process the output record set
#     output = {}
#     for row in jv_rset:
#         error_code, error_message, remarks = row
#         if error_code == 0:
#             output = {
#                 "Error Code": error_code,
#                 "Error Message": error_message,
#                 "Remarks": remarks,
#                 "Status": "Success"
#             }
#         else:
#             output = {
#                 "Error Code": error_code,
#                 "Error Message": error_message,
#                 "Remarks": remarks,
#                 "Status": "Failed"
#             }
    
#     return output




# import requests

# def call_post_ems_jv_api(data):
#     url = "http://localhost:8000/post_ems_jv"  # Update the URL with your FastAPI server address

#     # Make a POST request to the FastAPI endpoint
#     response = requests.post(url, json=data)

#     if response.status_code == 200:
#         return response.json()
#     else:
#         return {"error": "Failed to call the API"}

# # Example data to post
# data = {
#     "ac": "401.499.194.440001.211199.999.999",
#     "desc": "Variable power cost Provision Provided (Tube) - JUN-2024",
#     "dr": 28731418,
#     "cr": 0
# }

# # Call the API function
# result = call_post_ems_jv_api(data)
# print(result)