from datetime import datetime,date
import socket
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from sqlalchemy.sql import text
from log_file import createFolder

def loginformmodel(cnx, logemployee_name, logpassword):
    try:
        where =""
        where += f''' and me.employee_code='{logemployee_name}' and me.password_login=MD5('{logpassword}') '''
        
        query=text(f'''
                    SELECT 
                    me.*,
                    mc.company_name as company_name,
                    mc.company_code as company_code,
                    mb.branch_name as branch_name,
                    mb.branch_code as branch_code,
                    md.department_name as department_name,
                    md.department_code as department_code,
                    ms.shed_name as shed_name,
                    ms.shed_code as shed_code,
                    mmt.machinetype_name as machinetype_name,
                    mmt.machinetype_code as machinetype_code
                    FROM 
                    [ems_v1].[dbo].[master_employee] me
                    LEFT JOIN [ems_v1].[dbo].[master_company] mc on me.company_id=mc.company_id
                    LEFT JOIN [ems_v1].[dbo].[master_branch] mb on me.branch_id=mb.branch_id
                    LEFT JOIN [ems_v1].[dbo].[master_department] md on me.department_id=md.department_id
                    LEFT JOIN [ems_v1].[dbo].[master_shed] ms on me.shed_id=ms.shed_id
                    LEFT JOIN [ems_v1].[dbo].[master_machinetype] mmt on me.machinetype_id=mmt.machinetype_id
                    WHERE 
                        me.employee_code='{logemployee_name}' AND 
                        me.password_login=HASHBYTES('MD5', '{logpassword}') AND me.is_login='yes' AND me.status = 'active' ''')
        data = cnx.execute(query).mappings().all()        
        
        return data
    except Exception as e:
        return get_exception_response(e)
    
def changepassword(cnx,employee_id,old_password,new_password,retype_password):
    try:   
        sql = text(f'''select * from [ems_v1].[dbo].[master_employee] where employee_id = {employee_id} and password_login = HASHBYTES('MD5', '{old_password}') ''')
        data = cnx.execute(sql).mappings().all()          

        if len(data) == 0:            
           return _getErrorResponseJson("incorrect user id or password")

        else:
            if new_password != retype_password:
                return _getErrorResponseJson("retype password is incorrect")

            query=text(f'''update [ems_v1].[dbo].[master_employee] set password_login = HASHBYTES('MD5', '{new_password}') where employee_id ='{employee_id}' ''')
            cnx.execute(query)
            cnx.commit()

    except Exception as e:
        return get_exception_response(e)
    
