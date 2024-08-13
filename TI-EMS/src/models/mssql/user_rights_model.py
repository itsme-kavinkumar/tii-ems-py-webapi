from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
def employeelistuser(cnx,employee_id,is_login):
    try:
        where = ""
        if employee_id !='':
            where = text(f"and employee_id = '{employee_id}' ")

        query=text(f'''SELECT * FROM  [ems_v1].[dbo].[master_employee] WHERE status='active' and employee_type <> 'admin' {where} ''')
        data = cnx.execute(query).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)
    
def menu_list(cnx,employee_id):
    try:
        query1=text(f''' 
                    SELECT 
                        ms.*,
                        ISNULL (u.id,0) AS u_r_id,
                        ISNULL (u.add_op,'')AS add_opp,
                        ISNULL (u.edit_op,'')AS edit_opp,
                        ISNULL (u.delete_op,'')AS delete_opp
                    FROM
                        [ems_v1].[dbo].[menu_mas] ms
                        LEFT JOIN 
                        (select * from [ems_v1].[dbo].[user_rights] where userid={employee_id}) As u
                        ON u.menu_id=ms.menu_id
                        WHERE ms.status='active' 
                        ORDER BY ms.slno
			  ''')
        print(query1)  
        data = cnx.execute(query1).mappings().all()

        return data
    except Exception as e:
        return get_exception_response(e)

def save_userrights(cnx,employee_id, menu):
    try: 
        del_query=text(f'''DELETE FROM [ems_v1].[dbo].[user_rights] WHERE userid='{employee_id}' ''')
        cnx.execute(del_query)
        cnx.commit()
        user_dict = json.loads(menu)
        for i in user_dict:
              menu_id=i['menu_id']
              add_op = i['add_op']
              edit_op = i['edit_op']
              delete_op=i['delete_op']
              query=text(f'''insert into [ems_v1].[dbo].[user_rights](menu_id,add_op,edit_op,delete_op,userid)
                      values('{menu_id}','{add_op}','{edit_op}','{delete_op}','{employee_id}') ''')        
              cnx.execute(query)

              cnx.commit() 

        return employee_id
    except Exception as e:
        return get_exception_response(e)
    
def save_menumas(cnx,menu):
    user_dict = json.loads(menu)
    for i in user_dict:
        menu_id=i['menu_id']
        menu_name_display = i['menu_name_display']
        query = text(f'''update ems_v1.menu_mas set menu_name_display= '{menu_name_display}' where menu_id = {menu_id}''')       
        cnx.execute(query)
        cnx.commit()