from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.image import parse_date
from datetime import date
def sidebarlist(cnx,employee_id):
    try:
        query=text(f''' select * from [ems_v1].[dbo].[master_employee] where employee_id={employee_id}''')
        data=cnx.execute(query).mappings().all()
        print(data)
        
        if len(data) > 0 :
            for record in data:
                employee_type=record['employee_type']

        if employee_type == 'admin':
            query1=text(f'''
                        select * 
                        from [ems_v1].[dbo].[menu_mas]  
                        where status='active' 
                        order by slno
                        ''')
        else:
            query1=text(f''' SELECT 
                            ms.*,
                            ISNULL(u.id, 0) AS u_r_id,
                            ISNULL(u.add_op, '') AS add_opp,
                            ISNULL(u.edit_op, '') AS edit_opp,
                            ISNULL(u.delete_op, '') AS delete_opp
                        FROM
                            [ems_v1].[dbo].[menu_mas] ms,
                            [ems_v1].[dbo].[user_rights] u
                        WHERE
                            ms.status = 'active'
                            AND ms.menu_id = u.menu_id
                            AND u.userid = {employee_id}
                        ORDER BY ms.slno
                            
			  ''')
        data = cnx.execute(query1).mappings().all()
        return data
    except Exception as e:
        return get_exception_response(e)