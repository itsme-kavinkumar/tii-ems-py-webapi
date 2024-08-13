from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text


def plant_Lists(cnx, plant_id, bu_id, company_id):
    where = ""
    orderby = ""
    
    if plant_id != "":
        where += f"and mt.plant_id = '{plant_id}' "
    if bu_id != "" and bu_id != 'all':
        where += f''' and mt.bu_id = '{bu_id}' '''
    if company_id != "" and company_id != 'all':
        where += f''' and mt.company_id = '{company_id}' '''
    
    orderby += "mt.plant_id"

    query=f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name
		FROM
			master_plant mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER join master_company mc on mc.company_id = mt.company_id
            INNER join master_business_unit mb on mb.bu_id = mt.bu_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    print(query)
    result = cnx.execute(text(query)).fetchall()
    
    return result

def getplantdtl(cnx, plant_id, plant_code, plant_name):
    where=""

    if plant_id != "":
        where += f"and plant_id <> '{plant_id}' "
      
    query=f'''select * from master_plant where 1=1 and status<>'delete' and plant_code = '{plant_code}' {where}'''

    result = cnx.execute(text(query)).fetchall()
    
    return result

def save_plant(cnx, plant_code, plant_name, company_name, bu_name, oracle_id, ramco_id, plant_address, plant_pincode, plant_state, plant_country, host_ip, is_subcategory, user_login_id):
      query= f'''insert into master_plant(plant_code,plant_name,company_id,bu_id,oracle_id,ramco_id,plant_address,plant_pincode,plant_state,plant_country,host_ip, is_subcategory, created_on,created_by)
               values('{plant_code}','{plant_name}','{company_name}','{bu_name}','{oracle_id}','{ramco_id}','{plant_address}','{plant_pincode}','{plant_state}','{plant_country}','{host_ip}','{is_subcategory}',now(),'{user_login_id}')
      '''
      print(query)
      cnx.execute(text(query))
      insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
      cnx.commit()
      
      update_plant_wise_sync(cnx, 'master_plant')
      return insert_id

def update_plant(cnx, plant_id, plant_code, plant_name, company_name, bu_name, oracle_id, ramco_id, plant_address, plant_pincode, plant_state, plant_country, host_ip, is_subcategory, user_login_id):
    query=f''' update 
                    master_plant
                set 
                    plant_code = '{plant_code}',
                    plant_name = '{plant_name}',
                    company_id = '{company_name}',
                    bu_id = '{bu_name}',
                    oracle_id = '{oracle_id}',
                    ramco_id = '{ramco_id}',
                    plant_address = '{plant_address}',
                    plant_pincode = '{plant_pincode}',
                    plant_state = '{plant_state}',
                    plant_country = '{plant_country}',
                    host_ip = '{host_ip}',
                    sync_status = 'update',
                    is_subcategory = '{is_subcategory}',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    plant_id = '{plant_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    update_plant_wise_sync(cnx, 'master_plant')

def update_plantStatus(cnx, plant_id,status='delete'):
      query=f''' Update master_plant Set sync_status = 'update', status = '{status}' where plant_id = '{plant_id}'
      '''
      cnx.execute(text(query))
      cnx.commit()
      update_plant_wise_sync(cnx, 'master_plant')

def changestatus_plant(cnx, plant_id, active_status):
    status = ''

    if status == 'inactive':
        status = 'active'
    elif status == 'active':
        status = 'inactive'
    
    query = f''' Update master_plant Set sync_status = 'update',status = '{active_status}' Where plant_id='{plant_id}' '''
    cnx.execute(text(query))
    cnx.commit()
    update_plant_wise_sync(cnx, 'master_plant')

def get_plant_name(cnx, bu_id, company_id):
    where = ""
    
    if bu_id != "" and bu_id != 'all':
        where += f''' and bu_id = '{bu_id}' '''
    if company_id != "" and company_id != 'all':
        where += f''' and company_id = '{company_id}' '''
        
    query = f''' select * from master_plant where 1=1 and status = 'active' {where} '''
    
    result = cnx.execute(text(query)).fetchall()

    return result

def change_posting_plant(cnx, plant_id, active_status):
    status = ''

    if active_status == 'yes':
        status = 'no'
    elif active_status == 'no':
        status = 'yes'

    query = f''' Update master_plant Set sync_status = 'update',is_posting = '{status}' Where plant_id = '{plant_id}' '''
    cnx.execute(text(query))
    cnx.commit()
    update_plant_wise_sync(cnx, 'master_plant')

