from sqlalchemy import text


async def gl_post_detail_list_data(cnx, id, campus_id, post_month_year, status_code):
    where = ''
    if status_code == 0:
        where += f' AND s.error_code = {status_code}'
  
    query = text(f"""
        SELECT                
            s.*,
            c.campus_name,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user
        FROM 
            ems_v1.gl_post_detail s
            left join ems_v1.master_employee cu on cu.employee_id=s.created_by              
	        left join ems_v1.master_campus c on c.campus_id=s.campus_id                
        WHERE 
            s.campus_id = '{campus_id}' AND  s.post_month_year = '{post_month_year}' {where}
    """)

    data = await cnx.execute(query)
    data = data.fetchall()
    return data



async def save_gl_post_detail(
                cnx,
                campus_id,
                post_month_year,
                account_lst,
                status,
                error_Code,                
                user_login_id
            ):


    query = text(f"""
                    INSERT INTO ems_v1.gl_post_detail (campus_id, post_month_year, request_data, response_data, error_Code, created_by, created_on
                    )
                    VALUES (
                        '{campus_id}','{post_month_year}','{account_lst}', "{status}", '{error_Code}', '{user_login_id}', now()
                    )
                """)
                
    await cnx.execute(query)
    await cnx.commit()
    