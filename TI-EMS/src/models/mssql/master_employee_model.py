from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image
from log_file import createFolder

def employee_Lists(cnx,employee_id):
    try:
        where = ""
        if employee_id != "":
            where += f"and me.employee_id = '{employee_id}' "
    
        query=text(f'''
                    SELECT
                        me.*,
                        mc.company_name AS company_name,
                        mb.branch_name AS branch_name,
                        ISNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	                    ISNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
                        md.department_name as department_name,
                        md.department_code as department_code,
                        ms.shed_name as shed_name,
                        ms.shed_code as shed_code,
                        mmt.machinetype_name as machinetype_name,
                        mmt.machinetype_code as machinetype_code
                    FROM 
                        [ems_v1].[dbo].[master_employee] me
                        left join [ems_v1].[dbo].[master_employee] cu on cu.employee_id=me.created_by
	                    left join [ems_v1].[dbo].[master_employee] mu on mu.employee_id=me.modified_by
                        left JOIN [ems_v1].[dbo].[master_company] mc ON mc.company_id=me.company_id
                        left JOIN [ems_v1].[dbo].[master_branch] mb ON mb.branch_id=me.branch_id                        
                        left JOIN [ems_v1].[dbo].[master_department] md ON md.department_id=me.department_id                        
                        left JOIN [ems_v1].[dbo].[master_shed] ms ON ms.shed_id=me.shed_id                        
                        left JOIN [ems_v1].[dbo].[master_machinetype] mmt ON mmt.machinetype_id=me.machinetype_id                        
                        WHERE me.status!='delete'and me.employee_type != 'admin' {where} ''')      
        
        data = cnx.execute(query).mappings().all()
        createFolder("Log/","Query executed successfully for  employee list")

        return data
    except Exception as e:
        return get_exception_response(e)
    
def getemployeedtl(cnx, employee_id, employee_code):
    where=""

    if employee_id != "":
        where += f"and employee_id <> '{employee_id}' "
      
    query=f'''select * from ems_v1.dbo.master_employee where 1=1 and status<>'delete' and employee_code= '{employee_code}' {where}'''

    result = cnx.execute(text(query)).mappings().all()
    
    return result

def save_employee(cnx,company_id,branch_id,department_id,shed_id,machinetype_id,employee_code,employee_name,employee_type,mobileno,email,password_login,is_login,login_id):

    try: 
        query= text(f'''insert into [ems_v1].[dbo].[master_employee] (company_id,branch_id,employee_name,
                       employee_code,employee_type,mobileno,email,password_login,is_login,created_on,created_by,department_id, shed_id, machinetype_id )
                       values('{company_id}','{branch_id}' ,'{employee_name}','{employee_code}','{employee_type}',
                       '{mobileno}','{email}', HASHBYTES('MD5', '{password_login}'),'{is_login}',GETDATE(),'{login_id}','{department_id}','{shed_id}','{machinetype_id}') ''')
     
        cnx.execute(query)
        insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
        cnx.commit()

        return insert_id
    except Exception as e:
        return get_exception_response(e)
    
def update_employee(cnx,employee_id,company_id,branch_id,department_id,shed_id,machinetype_id,employee_code,employee_name,employee_type,mobileno,email,password_login,is_login,login_id):
    try:
        query =text(f'''update  [ems_v1].[dbo].[master_employee] set company_id = '{company_id}',branch_id = '{branch_id}',  
                       employee_name = '{employee_name}',employee_code = '{employee_code}',employee_type = '{employee_type}',
                       mobileno = '{mobileno}',email = '{email}',password_login= HASHBYTES('MD5', '{password_login}'),is_login='{is_login}',
                       department_id = '{department_id}',shed_id = '{shed_id}', machinetype_id = '{machinetype_id}',
                       modified_on = GETDATE(),modified_by='{login_id}' where employee_id = '{employee_id}'
                       ''')
        
        cnx.execute(query)
        cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    
def update_employeeStatus(cnx, employee_id, status):
    if status != '':
        query=f''' Update ems_v1.dbo.master_employee Set status = '{status}' Where employee_id='{employee_id}' '''
    else: 
        query=f''' Update ems_v1.dbo.master_employee Set status = 'delete' Where employee_id='{employee_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    
    

