from sqlalchemy import text


async def account_code_entry_list(cnx, account_code_entry_id, campus_id):
    where = ""

    if account_code_entry_id != "":
        where += f" AND s.account_code_entry_id = '{account_code_entry_id}' "
    if campus_id != "" and campus_id != 0:
        where += f" and s.campus_id = '{campus_id}' "

    query = text(f"""
        SELECT                
            s.*,
            c.campus_name,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        FROM 
            ems_v1.master_account_code_entry s
            left join ems_v1.master_employee cu on cu.employee_id=s.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=s.modified_by                
	        left join ems_v1.master_campus c on c.campus_id=s.campus_id                
        WHERE 
            s.status != 'delete'{where}
    """)

    data = await cnx.execute(query)
    data = data.fetchall()
    return data


async def save_account_code_entry(
                cnx,
                campus_id,
                account_code,
                description,
                reporting_department,
                account_type,
                unit_type,
                rate_type,
                fixed_percentage_value,
                rate_per_unit,
                user_login_id,
            ):
    query = text(f"""
            INSERT INTO ems_v1.master_account_code_entry (campus_id, account_code,
            description, reporting_department, account_type, created_on, created_by, unit_type, rate_type, fixed_percentage_value, rate_per_unit
            )
            VALUES (
                '{campus_id}','{account_code}','{description}', '{reporting_department}', '{account_type}', now(), '{user_login_id}', '{unit_type}', '{rate_type}', '{fixed_percentage_value}', '{rate_per_unit}'
            )
        """)
    await cnx.execute(query)
    await cnx.commit()


async def update_account_code_entry(
    cnx,
    account_code_entry_id,
    campus_id,
     account_code,
                description,
                reporting_department,
                account_type,
                unit_type,
                rate_type,
                fixed_percentage_value,
                rate_per_unit,
                user_login_id,
):
    query = text(f"""
        UPDATE 
            ems_v1.master_account_code_entry
        SET 
            campus_id = '{campus_id}',
            account_code = '{account_code}',
            description = '{description}', 
            reporting_department = '{reporting_department}', 
            account_type = '{account_type}', 
            modified_on = NOW(),
            modified_by = '{user_login_id}',            
            unit_type = '{unit_type}',            
            rate_type = '{rate_type}',            
            fixed_percentage_value = '{fixed_percentage_value}',           
            rate_per_unit = '{rate_per_unit}'            
        WHERE
            account_code_entry_id = '{account_code_entry_id}'
    """)

    await cnx.execute(query)
    await cnx.commit()


async def update_sourceentryStatus(cnx, account_code_entry_id, status):
    if status != "":
        query = f""" Update ems_v1.master_account_code_entry Set status = '{status}' Where account_code_entry_id='{account_code_entry_id}' """
    else:
        query = f""" Update ems_v1.master_account_code_entry Set status = 'delete' Where account_code_entry_id='{account_code_entry_id}' """

    await cnx.execute(text(query))
    await cnx.commit()


async def get_reporting_department_data(cnx, campus_id):
    where = ""

    if campus_id != "":
        where += f" AND mbe.campus_id = '{campus_id}' "

    query = text(f"""
        SELECT 
            mbe.campus_id, mbe.reporting_department
        FROM 
            ems_v1.master_budget_entry mbe 
        WHERE mbe.status <> 'delete'{where}
    """)

    data = await cnx.execute(query)
    data = data.fetchall()
    return data