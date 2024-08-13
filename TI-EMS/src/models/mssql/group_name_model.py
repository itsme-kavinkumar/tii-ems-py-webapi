from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

def group_namelist(cnx):
    try:
        
        query = text(f'''select * from [ems_v1].[dbo].[master_group_name] where status!='delete' ''')
        data = cnx.execute(query).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)
    
def get_groupname_dtl(cnx, group_name):
    
    select_query = text(f'''select * from [ems_v1].[dbo].[master_group_name]  where group_name = '{group_name}' and status!='delete' ''')
    data = cnx.execute(select_query).mappings().all()
    
    return data

def save_groupname(cnx,group_name,user_login_id):
    try: 
        query = text(f'''INSERT INTO [ems_v1].[dbo].[master_group_name] 
                         (group_name,created_on,created_by)
                         values('{group_name}',getdate(),'{user_login_id}')''')
        cnx.execute(query)
        insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
        cnx.commit()

        return insert_id
    except Exception as e:
        return get_exception_response(e)
    
def update_groupname(cnx, id,group_name,user_login_id):
    try:
        query = text(f'''UPDATE [ems_v1].[dbo].[master_group_name]
                        SET group_name ='{group_name}',modified_on= getdate(),modified_by = '{user_login_id}' 
                        where id = {id} ''')
        
        cnx.execute(query)
        cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    
def update_groupname_status(cnx, id, status):
    
    if status !='':
        query = text(f" UPDATE [ems_v1].[dbo].[master_group_name]  SET status = '{status}' WHERE id = '{id}' ")
        cnx.execute(query)
    else:
        query = text(f" UPDATE [ems_v1].[dbo].[master_group_name]  SET status = 'delete' WHERE id = '{id}' ")                
        cnx.execute(query)
    cnx.commit()

