from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

def mail_list(cnx,mail_id):
    try:
        where =''
        if mail_id != '':
            where += f"where mail_id = '{mail_id}'"
        query = text(f'''select * from ems_v1.master_mail {where}''')
        data = cnx.execute(query).fetchall()
        cnx.commit()
        
        return data
    except Exception as e:
        return get_exception_response(e)
    

def save_mail(cnx,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report_csv,report_pdf,report_excel,user_login_id):
    try: 
        query = text(f'''insert into ems_v1.master_mail (from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report_csv,report_pdf,report_excel,created_on , created_by)
                         values('{from_mail}','{to_mail}','{pwd}','{cc_mail}','{bcc_mail}','{subject}','{compose_textarea}','{report_csv}','{report_pdf}','{report_excel}', now(), {user_login_id})''')
   
        cnx.execute(query)
        insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
        cnx.commit()

        return insert_id
    except Exception as e:
        return get_exception_response(e)
    
def update_mail(cnx,mail_id,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report_csv,report_pdf,report_excel,user_login_id):
    query = text(f'''update ems_v1.master_mail 
                        set from_mail = '{from_mail}', to_mail = '{to_mail}', pwd = '{pwd}',
                        cc_mail = '{cc_mail}', bcc_mail = '{bcc_mail}', subject = '{subject}', 
                        compose_textarea = '{compose_textarea}', report_csv = '{report_csv}', 
                        report_pdf = '{report_pdf}', report_excel = '{report_excel}', modified_on = now(),
                        modified_by = {user_login_id} where mail_id = {mail_id} ''')
    cnx.execute(query)
    cnx.commit()

