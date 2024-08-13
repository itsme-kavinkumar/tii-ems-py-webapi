from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

async def mail_list(cnx,mail_id):
    where =''
    if mail_id != '':
        where += f" and  mail_id = '{mail_id}'"
    query = text(f'''select mm.*,c.campus_name from ems_v1.master_mail mm
                    inner join master_campus c on c.campus_id = mm.campus_id
                    where mm.status != 'delete' {where}''')
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    

async def save_mail(cnx,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_date,user_login_id,campus_id,mail_for):
    if send_at == '':
        send_at = '10:00'

    query = text(f'''insert into ems_v1.master_mail (from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_date,created_on , created_by,campus_id,mail_for)
                     values('{from_mail}','{to_mail}','{pwd}','{cc_mail}','{bcc_mail}','{subject}','{compose_textarea}','{report}','{mail_type}','{send_at}','{send_date}', now(), {user_login_id},'{campus_id}','{mail_for}')''')

    await cnx.execute(query)
    await cnx.commit()
    
async def update_mail(cnx,mail_id,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_date,user_login_id,campus_id,mail_for):
    query = text(f'''update ems_v1.master_mail 
                        set from_mail = '{from_mail}', to_mail = '{to_mail}', pwd = '{pwd}',
                        cc_mail = '{cc_mail}', bcc_mail = '{bcc_mail}', subject = '{subject}', 
                        compose_textarea = '{compose_textarea}', report = '{report}', 
                        mail_type = '{mail_type}', send_at = '{send_at}', send_date = '{send_date}', modified_on = now(),campus_id = '{campus_id}',mail_for = '{mail_for}',
                        modified_by = {user_login_id} where mail_id = {mail_id} ''')
    await cnx.execute(query)
    await cnx.commit()

async def update_mailstatus(cnx, mail_id, status):

    if status != '':
        query = text(f"update master_mail set status = '{status}'  where mail_id = '{mail_id}' ")
    else:
        query = text(f"update master_mail set status = 'delete'  where mail_id = '{mail_id}' ")
    await cnx.execute(query)
    await cnx.commit()