from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.image import parse_date
from datetime import date
def operation_dtl(cnx,employee_id,menu_id):
    try:
        where=""
        if menu_id !='':
            where+=f''' and menu_id='{menu_id}' '''

        query=text(f'''SELECT 
                        u.*,
                        e.employee_type
                  FROM 
                        [ems_v1].[dbo].[user_rights] u
                        inner join [ems_v1].[dbo].[master_employee] e on e.employee_id=u.userid
                  WHERE
                        u.userid={employee_id} {where}
                        
        ''')
        data = cnx.execute(query).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)