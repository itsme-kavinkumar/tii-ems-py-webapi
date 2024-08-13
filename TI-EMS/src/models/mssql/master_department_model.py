from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text
def department_Lists(cnx, plant_department_id,plant_id, bu_id, company_id):
    where = ""
    orderby = ""
    
    if plant_department_id != "":
        where += f"and mt.plant_department_id = '{plant_department_id}' "
    if bu_id != "" and bu_id != 'all':
        where += f''' and mt.bu_id = '{bu_id}' '''
    if company_id != "" and company_id != 'all':
        where += f''' and mt.company_id = '{company_id}' '''
    if plant_id != "" and plant_id != 'all':
        where += f''' and mt.plant_id = '{plant_id}' '''
    
    orderby += "mt.plant_department_id"

    query=f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
            ifnull(concat(mp.plant_code,'-',mp.plant_name),'') as plant_name
		FROM
            master_plant_wise_department mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER JOIN master_company mc on mc.company_id = mt.company_id
			INNER JOIN master_business_unit mb on mb.bu_id = mt.bu_id
            INNER JOIN master_plant mp on mp.plant_id = mt.plant_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    
    result = cnx.execute(text(query)).fetchall()
    
    return result

def getdepartmentdtl(cnx, plant_department_id, department_code, department_name):
    where=""

    if plant_department_id != "":
        where += f"and plant_department_id <> '{plant_department_id}' "
      
    query=f'''select * from master_plant_wise_department where 1=1 and status<>'delete' and plant_department_code='{department_code}' {where}'''

    result = cnx.execute(text(query)).fetchall()
    
    return result

def save_department(cnx, department_code, department_name, company_name, bu_name, plant_name, user_login_id):
      query= f'''insert into master_plant_wise_department(plant_department_code,plant_department_name,company_id,bu_id,plant_id,created_on,created_by)
               values('{department_code}','{department_name}','{company_name}','{bu_name}','{plant_name}',now(),'{user_login_id}')
      '''
      cnx.execute(text(query))
      insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
      cnx.commit()
      
      update_plant_wise_sync(cnx, 'master_plant_wise_department')
      return insert_id

def update_department(cnx, department_id, department_code, department_name, user_login_id):
    query=f''' update 
                    master_plant_wise_department
                set 
                    plant_department_code = '{department_code}',
                    plant_department_name = '{department_name}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    plant_department_id = '{department_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    update_plant_wise_sync(cnx, 'master_plant_wise_department')

def update_departmentStatus(cnx, plant_department_id, status='delete'):

      query=f''' Update master_plant_wise_department Set sync_status = 'update',status = '{status}' Where plant_department_id='{plant_department_id}' '''
      
      cnx.execute(text(query))
      cnx.commit()
      update_plant_wise_sync(cnx, 'master_plant_wise_department')

def changestatus_department(cnx, plant_department_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_plant_wise_department Set sync_status = 'update',status = '{status}' Where plant_department_id='{plant_department_id}' '''
    cnx.execute(text(query))
    cnx.commit()
    update_plant_wise_sync(cnx, 'master_plant_wise_department')

def get_department_name(cnx, plant_id, bu_id, company_id):
    where = ""
    
    if bu_id != "" and bu_id != 'all':
        where += f''' and bu_id = '{bu_id}' '''
    if company_id != "" and company_id != 'all':
        where += f''' and company_id = '{company_id}' '''
    if plant_id != "" and plant_id != 'all':
        where += f''' and plant_id = '{plant_id}' '''
    
        
    query = f''' select * from master_plant_wise_department where 1=1 and status = 'active' {where} '''
    
    result = cnx.execute(text(query)).fetchall()

    return result