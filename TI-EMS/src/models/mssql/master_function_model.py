from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image
from log_file import createFolder
def function_Lists(cnx, function_type,function_id,base_path):
    try:
        where = ""
        if function_id != "":
            where += f"and mf.function_id = '{function_id}' "
    
        if function_type != "":
            where = f" and mf.function_type = {function_type}"

        query =text( f''' 
                    SELECT 
                    	mf.* , 
                    	CONCAT('{base_path}/attachment/images/',mf.image) AS image,
                        ISNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
                    	ISNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                    FROM
                    	[ems_v1].[dbo].[master_function] as mf 
                    	left join [ems_v1].[dbo].[master_employee] cu on cu.employee_id=mf.created_by
                    	left join [ems_v1].[dbo].[master_employee] mu on mu.employee_id=mf.modified_by
                    WHERE
                    	mf.status !='delete' {where} 
                    ''')    
        data = cnx.execute(query).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)
    
def getfunctiondtl(cnx, function_id, function_code):
    where=""

    if function_id != "":
        where += f"and function_id <> '{function_id}' "
      
    query=f'''select * from ems_v1.dbo.master_function where 1=1 and status<>'delete' and function_code= '{function_code}' {where}'''

    result = cnx.execute(text(query)).mappings().all()
    
    return result
def save_function(cnx, function_code,function_name,function_type,image,old_image,user_login_id,static_dir):

    try: 
        if old_image == "":
            filename = save_image(image, f"{static_dir}/images")
        else:
            filename = image
        query = text(f"""
                INSERT INTO [ems_v1].[dbo].[master_function] (function_name, function_code, image, created_on, created_by, function_type)
                VALUES ('{function_name}', '{function_code}', '{filename}', GETDATE(), '{user_login_id}','{function_type}')
            """)
        createFolder("Log/","Query executed successfully for save plant function"+str(query))
        cnx.execute(query)
        cnx.commit()

    except Exception as e:
        return get_exception_response(e)
    
def update_function(cnx,function_id, function_code,function_name,function_type,image,old_image,user_login_id,static_dir):
    try:
        if old_image == "":
            filename = save_image(image, f"{static_dir}/images")
        else:
            filename = image

        if function_id != '':              
                query = text(f"""
                UPDATE [ems_v1].[dbo].[master_function] SET function_name = '{function_name}', 
                function_code = '{function_code}',
                image = '{filename}',
                modified_on = GETDATE(), 
                modified_by = '{user_login_id}', function_type = '{function_type}' WHERE function_id = {function_id}
            """)
        
        cnx.execute(query)
        cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    
def update_functionStatus(cnx, function_id, status):
    if status != '':
        query=f''' Update ems_v1.dbo.master_function Set status = '{status}' Where function_id='{function_id}' '''
    else: 
        query=f''' Update ems_v1.dbo.master_function Set status = 'delete' Where function_id='{function_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    
    

