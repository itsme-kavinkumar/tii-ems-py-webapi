from fastapi import APIRouter
from fastapi import Form, Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import (
 
    get_exception_response
)
from pathlib import Path
from fastapi.requests import Request
from fastapi import FastAPI, Depends, Form, File, UploadFile, Body
import os
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from src.models.parse_date import parse_date
from datetime import datetime, timedelta
import requests
import httpx
import asyncio
from src.endpoints.report import get_availability_report
import json
file_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "..", "database.txt"
    )
)


# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace

    if content == "MySQL":
        from mysql_connection import get_db
        from src.models.mysql.gl_post_detail_model import (
            gl_post_detail_list_data
        )

    elif content == "MSSQL":
        from mssql_connection import get_db

    else:
        raise Exception(
            "Database is not configured or 'database.txt' contains an unexpected value."
        )
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"


@router.post("/get_gl_post_detail_list/", tags=["GL Post Detail"])
async def gl_post_detail_list(
    id: int = Form(""),
    campus_id: int = Form(""),
    post_month_year: str = Form(""),
    status_code : int = Form(""),
    cnx: AsyncSession = Depends(get_db)
):
    try:
        result = await gl_post_detail_list_data(cnx, id, campus_id, post_month_year, status_code)

        createFolder(
            "Log/", "Query executed successfully for get_gl_post_detail_list"
        )

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response

    except Exception as e:
        return get_exception_response(e)
    
@router.get("/autoglpost/")
async def AutoGlPost(cnx: AsyncSession = Depends(get_db)):
    try:
        createFolder("Log/auto_gl", "<<---autoglpost started---->>")
        now = datetime.today()
        if now.day == 1 and now.strftime("%H:%M") == "18:00":
            createFolder("Log/auto_gl", "<<---if condition Executed---->>")
            month = ((datetime.today().replace(day=1)) - timedelta(days=1)).replace(day=1).strftime("%d-%m-%Y")  # To get the previous month date
            query = 'SELECT campus_id FROM master_campus'
            response = await cnx.execute(query)
            campus_ids = response.scalars().all()
             
            
            for campus_id in campus_ids:
                request = Request({
                    "type": "http",
                    "headers": [],
                })

                response = await get_availability_report(
                    request,
                    campus_id=campus_id,
                    month=month,
                    report_for='excel',
                    report_type='with_rate',
                    report_method='provision',
                    save_account_dtl='yes',
                    user_login_id = 1,
                    is_auto_gl = 'yes',
                    cnx = cnx
                )
                response_body = response.body.decode('utf-8')
                response_content = json.loads(response_body)
                createFolder("Log/auto_gl", f"{campus_id} -->> {response_content}")
                print( f"{campus_id} -->> {response_content}")

    except Exception as e:
        return get_exception_response(e)

