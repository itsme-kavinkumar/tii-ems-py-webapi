from fastapi import APIRouter
from fastapi import Form, Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import (
    _getReturnResponseJson,
    _getSuccessResponseJson,
    _getErrorResponseJson,
    get_exception_response,
)
from pathlib import Path
from fastapi.requests import Request
from fastapi import FastAPI, Depends, Form, File, UploadFile, Body
import os
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from src.models.parse_date import parse_date
from datetime import datetime, timedelta

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
        from src.models.mysql.master_account_code_entry_model import (
            account_code_entry_list,
            save_account_code_entry,
            update_account_code_entry,
            update_sourceentryStatus,
            get_reporting_department_data,
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


@router.post("/get_account_code_entry_list/", tags=["Master Account Code Entry"])
async def account_code_entrylist(
    account_code_entry_id: int = Form(""),
    campus_id: int = Form(""),
    cnx: AsyncSession = Depends(get_db),
):
    try:
        result = await account_code_entry_list(cnx, account_code_entry_id, campus_id)

        createFolder(
            "Log/", "Query executed successfully for get_account_code_entry_list"
        )

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result,
        }

        return response

    except Exception as e:
        return get_exception_response(e)


@router.post("/save_account_code_entry/", tags=["Master Account Code Entry"])
async def savesourceentry(
    account_code_entry_id: int = Form(""),
    campus_id: int = Form(""),
    account_code: str = Form(""),
    description: str = Form(""),
    reporting_department: str = Form(""),
    account_type: str = Form("", description="debit, credit"),
    unit_type: str = Form("", description="variable, fixed, generation"),
    rate_type: str = Form(
        "", description="variable, fixed, generation, fixed_percentage"
    ),
    fixed_percentage_value: str = Form(""),
    rate_per_unit: str = Form(""),
    user_login_id: str = Form(""),
    cnx: AsyncSession = Depends(get_db),
):
    try:
        if campus_id == "":
            return _getErrorResponseJson(" Campus ID is required")

        if account_code == "":
            return _getErrorResponseJson(" Account Code is required")

        if description == "":
            return _getErrorResponseJson(" description is required")

        
        if rate_type == "fixed_percentage":
            if fixed_percentage_value == "":
                return _getErrorResponseJson("fixed_percentage_value is required")
            
        if rate_type == "rate_per_unit":
            if rate_per_unit == "":
                return _getErrorResponseJson("rate_per_unit is required")

        if account_code_entry_id == "":
            await save_account_code_entry(
                cnx,
                campus_id,
                account_code,
                description,
                reporting_department,
                account_type,
                unit_type,
                rate_type,
                fixed_percentage_value,
                rate_per_unit,
                user_login_id,
            )
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_account_code_entry(
                cnx,
                account_code_entry_id,
                campus_id,
                account_code,
                description,
                reporting_department,
                account_type,
                unit_type,
                rate_type,
                fixed_percentage_value,
                rate_per_unit,
                user_login_id,
            )
            createFolder(
                "Log/", "Query executed successfully for update update_account_code_entry"
            )
            return _getSuccessResponseJson("Updated Successfully...")

    except Exception as e:
        return get_exception_response(e)


@router.post("/remove_account_code_entry/", tags=["Master Account Code Entry"])
async def remove_sourceentry(
    account_code_entry_id: str = Form(""),
    status: str = Form(""),
    cnx: AsyncSession = Depends(get_db),
):
    if account_code_entry_id == "":
        return _getErrorResponseJson("account_code_entry_id is required")

    try:
        await update_sourceentryStatus(cnx, account_code_entry_id, status)
        if status != "":
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)


@router.post("/get_reporting_department/", tags=["Master Account Code Entry"])
async def get_reporting_department(
    campus_id: int = Form(""), cnx: AsyncSession = Depends(get_db)
):
    try:
        result = await get_reporting_department_data(cnx, campus_id)

        createFolder("Log/", "Query executed successfully for get_reporting_department")

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result,
        }

        return response

    except Exception as e:
        return get_exception_response(e)
