from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text
def business_unit_Lists(cnx, bu_id,company_id):
    where = ""
    orderby = ""
    
    if bu_id != "":
        where += f"and mt.bu_id = '{bu_id}' "
    
    if company_id != "" and company_id != 'all':
        where += f''' and mt.company_id = '{company_id}' '''
    
    orderby += "mt.bu_id"

    query= f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name
		FROM
			master_business_unit mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER join master_company mc on mc.company_id = mt.company_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    
    result = cnx.execute(text(query)).fetchall()
    
    return result

def getbudtl(cnx, bu_id, bu_code, bu_name):
    where=""

    if bu_id != "":
        where += f"and bu_id <> '{bu_id}' "
      
    query= f'''select * from master_business_unit where 1=1 and status<>'delete' and bu_code= '{bu_code}' {where}'''

    result = cnx.execute(text(query)).fetchall()
    
    return result

def savebusiness_unit(cnx, bu_code, bu_name, company_name, user_login_id):
      query= f'''insert into master_business_unit(bu_code,bu_name, company_id, created_on,created_by )
               values('{bu_code}','{bu_name}','{company_name}',now(),'{user_login_id}')
      '''
      cnx.execute(text(query))
      insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
      cnx.commit()
      
      update_plant_wise_sync(cnx, 'master_business_unit')
    #   update_is_assign(cnx, 'master_company', 'master_business_unit', insert_id, company_name, 'company_id', 'bu_id') 
      return insert_id
      
def updatebusiness_unit(cnx, bu_id, bu_code, bu_name, company_name, user_login_id):
    query= f''' update 
                    master_business_unit
                set 
                    bu_code = '{bu_code}',
                    bu_name = '{bu_name}',
                    company_id = '{company_name}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    bu_id = '{bu_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    update_plant_wise_sync(cnx, 'master_business_unit')


def updatebusiness_unitStatus(cnx, bu_id, status='delete'):
      query=f''' Update master_business_unit Set sync_status = 'update', status = '{status}' where bu_id = '{bu_id}'
      '''
      cnx.execute(text(query))
      cnx.commit()
      update_plant_wise_sync(cnx, 'master_business_unit')
      
def changestatus_business_unit(cnx, bu_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_business_unit Set sync_status = 'update',status = '{status}' Where bu_id='{bu_id}' '''
    cnx.execute(text(query))
    cnx.commit()
    update_plant_wise_sync(cnx, 'master_business_unit')

def get_branch_name(cnx, company_id):
    where = ""
    
    if company_id != "" and company_id != 'all':
        where += f''' and company_id = '{company_id}' '''
        
    query = f''' select * from master_business_unit where 1=1 and status = 'active' {where} '''
    
    result = cnx.execute(text(query)).fetchall()

    return result



    

    