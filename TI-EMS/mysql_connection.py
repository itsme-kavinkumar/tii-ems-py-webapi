from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool,NullPool
import os
from fastapi import FastAPI,Request,Form,Body,Depends,HTTPException
from sqlalchemy.orm import Session
import aiomysql
import asyncio
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
# engine = create_engine("mysql+pymysql://apcmms:apcmms2023@apdbcmms.c9cc80iz23jn.ap-south-1.rds.amazonaws.com:3306/ems_v1",pool_pre_ping=True,poolclass=QueuePool)
isFile = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "config.ini"))
# isFile = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.ini"))

print(isFile)

# if isFile:
#     f = open(isFile,"r")
#     port = f.read()
# if port:
#     port_num = int(port)
#     print(f"The given port is : {port} " )

# else :
#     print(f"The given port is : {port} " )
def read_configuration_file(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split(' : ')
            config[key] = int(value) if value.isnumeric() else value
    return config

file_path = os.path.isfile(isFile)
if file_path:
    configuration = read_configuration_file(isFile)

    db_host = configuration['db_host']
    db_port = configuration['db_port']

 

DATABASE_URL = f"mysql+aiomysql://AIC_FAPI_EP:829fee610de85094ccf27cd353995c5c@{db_host}:{db_port}/ems_v1"



# DATABASE_URL = f"mysql+aiomysql://root@localhost:3306/ems_v1"
engine = create_async_engine(DATABASE_URL, echo=False, poolclass=NullPool)  

async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session