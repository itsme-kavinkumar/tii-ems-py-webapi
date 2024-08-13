from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from sqlalchemy.ext.asyncio import AsyncSession
import os

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.operation_model import operation_dtl
    elif content == 'MSSQL':
        from mssql_connection import get_db
        from src.models.mssql.operation_model import operation_dtl
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")


router = APIRouter()

@router.post("/operations/")
async def operations(employee_id:str=Form(''),
                     menu_id:str=Form(''),
                     cnx: AsyncSession = Depends(get_db)):

    try: 
        if employee_id == "" :
            return _getErrorResponseJson("employee_id is required...")
        
        result = await operation_dtl(cnx,employee_id,menu_id)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    