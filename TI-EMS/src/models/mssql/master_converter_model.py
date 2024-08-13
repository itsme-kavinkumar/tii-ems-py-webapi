from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image

def converter_Lists(cnx, converter_id):
    try:
        where = ""
        if converter_id != "":
            where += f"and mcd.converter_id = '{converter_id}' "

        query =text( f''' 
                    SELECT 
                    	mcd.*,
                    	ISNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
                    	ISNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user

                    FROM  
                    	[ems_v1].[dbo].[master_converter_detail] mcd
                    	left join ems_v1.dbo.master_employee cu on cu.employee_id=mcd.created_by
                    	left join ems_v1.dbo.master_employee mu on mu.employee_id=mcd.modified_by
                    WHERE 
                    	mcd.status !='delete'
                    	{where} 
                    ''')    
        data = cnx.execute(query).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)
    
def getconverterdtl(cnx, converter_id, converter_name):
    where=""

    if converter_id != "":
        where += f"and converter_id <> '{converter_id}' "
      
    query=f'''select * from ems_v1.dbo.master_converter_detail where 1=1 and status<>'delete' and converter_name= '{converter_name}' {where}'''

    result = cnx.execute(text(query)).mappings().all()
    
    return result
def save_converter(cnx,converter_name,ip_address,port_no,user_login_id):

    try: 
        query = text(f"""
                INSERT INTO [ems_v1].[dbo].[master_converter_detail] (
                     converter_name, ip_address, port_no, created_on, created_by
                )
                VALUES (
                    '{converter_name}', '{ip_address}', {port_no},  GETDATE(), '{user_login_id}'
                )
            """) 
        cnx.execute(query)
        insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
        cnx.commit()

        return insert_id
    except Exception as e:
        return get_exception_response(e)
    
def update_converter(cnx,converter_id,converter_name,ip_address,port_no,user_login_id):
    try:
        query =text(f"""
                UPDATE [ems_v1].[dbo].[master_converter_detail]
                SET converter_name = '{converter_name}', ip_address = '{ip_address}',
                port_no = {port_no},  modified_on = GETDATE(),
                modified_by = '{user_login_id}'
                WHERE converter_id = {converter_id}
            """)
        
        cnx.execute(query)
        cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    
def update_converterStatus(cnx, converter_id, status):
    if status != '':
        query=f''' Update ems_v1.dbo.master_converter_detail Set status = '{status}' Where converter_id='{converter_id}' '''
    else: 
        query=f''' Update ems_v1.dbo.master_converter_detail Set status = 'delete' Where converter_id='{converter_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    
    

