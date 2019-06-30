import datetime, io,time,psycopg2,requests,json
from flask import Flask,request,jsonify 
from sqlalchemy.ext.declarative import declarative_base
from flask_sqlalchemy import SQLAlchemy 
from sqlalchemy.sql import and_
from flask_marshmallow import Marshmallow
from sqlalchemy.orm import sessionmaker
#from flask.ext.mongoalchemy import MongoAlchemy
from sqlalchemy import Column, Integer, String, Date,DateTime,Float,MetaData,create_engine,text
import pandas as pd

#import os 

#Init app
app=Flask(__name__)
#basedir=os.path.abspath(os.path.dirname(__file__))
#Database
#'postgresql://postgres:Wimtech2019@localhost/data_extractor'
#'sqlite:///database.db'
#engine = create_engine('postgresql://postgres:Wimtech2019@localhost/data_extractor', echo=True)
engine = create_engine('postgresql+psycopg2://postgres:blessing12@localhost/data_extractor', echo=True)
app.config['SQLALCHEMY_DATABASE_URI']='postgresql://postgres:blessing12@localhost/data_extractor'


#Init db
db=SQLAlchemy(app)

Base = declarative_base()
#Sdb.init_app(app)
#Init Marshmallow
ma=Marshmallow(app)
Session = sessionmaker(bind=engine)
session=Session()
conn=engine.connect()

#vehicle_incident class model
class UsData_Clean(Base):
        __tablename__='UsData_Clean'  
        id=Column(Integer,primary_key=True,autoincrement=True)
        cmplid = Column(Integer)
        index=Column(Integer)
        odino  = Column(Integer)
        mfrname= Column(String(100))
        maketxt=Column(String(100))
        modeltxt=Column(String(50))
        yeartxt=Column(Integer)
        crash=Column(String(10))
        faildate=Column(Date)
        fire=Column(String(10))
        injured=Column(Integer)
        deaths=Column(Integer)
        compdesc=Column(String(500))
        city=Column(String(50))
        state=Column(String(50))
        country=Column(String(50))
        zipcode=Column(String(50))
        latitude=Column(Float)
        longitude=Column(Float)
        vin=Column(String(50))
        datea=Column(Date)
        ldate=Column(Date)
        miles=Column(Float)
        occurences=Column(Float)
        cdescr=Column(String(3000))
        cmpltype=Column(String(50))
        prodtype=Column(String(50))
        airbagexpected=Column(String(10))
        airbagdeployed=Column(String(10))
        airbagnotexpected=Column(String(10))
        airbagnotdeployed=Column(String(10))
        batchno=Column(Integer)

        def  __init__(self,cmplid,index,odino,mfrname,maketxt,modeltxt,yeartxt,crash,faildate,fire,injured,deaths,compdesc,city,state,country,zipcode,latitude,longitude,vin,datea,ldate,miles,occurences,cdescr,cmpltype,prodtype,airbagexpected,airbagdeployed,airbagnotexpected,airbagnotdeployed,batchno):
            self.cmplid=cmplid
            self.index=index
            self.odino=odino
            self.mfrname=mfrname
            self.maketxt=maketxt
            self.modeltxt=modeltxt
            self.yeartxt=yeartxt
            self.crash=crash
            self.faildate=faildate
            self.fire=fire
            self.injured=injured
            self.deaths=deaths
            self.compdesc=compdesc
            self.city=city
            self.state=state
            self.country=country
            self.zipcode=zipcode
            self.latitude=latitude
            self.longitude=longitude
            self.vin=vin
            self.datea=datea
            self.ldate=ldate
            self.miles=miles
            self.occurences=occurences
            self.cdescr=cdescr
            self.cmpltype=cmpltype
            self.prodtype=prodtype
            self.airbagexpected=airbagexpected
            self.airbagdeployed=airbagdeployed
            self.batchno=batchno

        def __repr__(self):
            return f"UsData_Clean('{self.id}','{self.cmplid}','{self.index}','{self.odino}','{self.mfrname}','{self.maketxt}','{self.modeltxt}','{self.yeartxt}','{self.crash}','{self.faildate}','{self.fire}','{self.injured}','{self.deaths}','{self.compdesc}','{self.city}','{self.state}','{self.country}','{self.zipcode}','{self.latitude}','{self.longitude}','{self.vin}','{self.datea}','{self.ldate}','{self.miles}','{self.occurences}','{self.cdescr}','{self.cmpltype}','{self.prodtype}','{self.airbagexpected}','{self.airbagdeployed}','{self.batchno}')"





            


#metadata.create_all(engine)

Base.metadata.create_all(engine)



# #copy nypd fro csv to datatable
# dfUs=pd.read_csv("NYPD.csv",encoding='latin-1',header=0)
# print(dfUs.head(n=5))
# dfUs.to_sql('NYPD', engine,if_exists='replace')

dfUs=pd.read_csv("FLAT_CMPL_airbag_Make_filter.csv",encoding='latin-1',header=0,index_col='cmplid')
dfUs.to_sql('UsData', engine,index_label='cmplid',if_exists='replace')



def notNone(string):
    if string is not None:
        return string
    else:
        return "19000101" 

def dateParse(stringDate):
    if stringDate is None:
        return 1900-1-1
    elif not stringDate:
        return 1900-1-1
    else:
        try:
            return datetime.datetime.strptime(stringDate[0:8],"%Y%m%d").date()
        except ValueError:
            print("############################################# Date is wrong")
            return datetime.datetime.strptime(stringDate[0:7],"%Y%m%d").date()




        
def dateParseNYPD(stringDate):
    if stringDate is None:
        return 1900-1-1
    elif not stringDate:
        return 1900-1-1
    else:
        return datetime.datetime.strptime(stringDate[0:10],"%Y-%m-%d").date()



rs=conn.execute('select * from public."UsData"')
for record in rs:
    print(record['cmplid'])
    ed_user = UsData_Clean(cmplid=record['cmplid'],index=None,odino=record['odino'],mfrname=record['mfrname'],maketxt=record['maketxt'],modeltxt=record['modeltxt'],yeartxt=record['yeartxt'],crash=record['crash'],faildate=dateParse(str(notNone(record['faildate']))),fire=record['fire'],injured=record['injured'],deaths=record['deaths'],compdesc=record['compdesc'],city=record['city'],state=record['state'],country='US',zipcode=None,latitude=None,longitude=None,vin=record['vin'],datea=dateParse(str(notNone(record['datea']))),ldate=dateParse(str(notNone(record['ldate']))),miles=record['miles'],occurences=record['occurences'],cdescr=record['cdescr'],cmpltype=record['cmpltype'],prodtype=record['prodtype'],airbagexpected=None, airbagdeployed=None, batchno=0)
    session.add(ed_user)
    session.commit()





 








# keyUpdate=0
# mergeCount=0

# key=conn.execute('SELECT "lastIdValue"FROM public."KeyTracker" where "tableNames"=\'NYPD\' and "idRowName"=\'index\';')
# for record in key:
#     keyUpdate=record['lastIdValue']
# #need to implement sort by 
# s=text("select * from public.\"NYPD\" where index>:x")
# NYPDRS=conn.execute(s, x=keyUpdate).fetchall() 


# UsCleanRS=conn.execute("select * from public.\"UsData_Clean\"").fetchall() 

# for NYRecord in NYPDRS:
#     for USCleanRecord in UsCleanRS:
#         if (dateParseNYPD(NYRecord['DATE'])==USCleanRecord['faildate']) & (USCleanRecord['state']=='NY'):
#             conn.execute(text("UPDATE public.\"UsData_Clean\" SET  \"index\"=:index,\"city\"=:city, \"zipcode\"=:zipcode ,\"latitude\"=:latitude ,\"longitude\"=:longitude ,\"injured\" =:injured ,\"deaths\"=:deaths where \"id\"=:id;"), index=NYRecord['index'],city=NYRecord['BOROUGH'],zipcode=NYRecord['ZIP CODE'],latitude=NYRecord['LATITUDE'],longitude=NYRecord['LONGITUDE'],injured=NYRecord['NUMBER OF PERSONS INJURED'],deaths=NYRecord['NUMBER OF PERSONS KILLED'],id=USCleanRecord['id'] ) 
#             keyUpdate=NYRecord['index']
#             mergeCount=mergeCount+1
#             session.commit()
#             break
        


# for NYRecord in NYPDRS:
#     #find if the index exists
    
#     NYPDIndexRS=conn.execute(text("select * from public.\"UsData_Clean\" where index=:x"), x=NYRecord['index']).fetchall() 
#     if len(NYPDIndexRS):
#         break
#     else:
#         try:
#             ed_user = UsData_Clean(cmplid=None,index=NYRecord['index'],odino=0,mfrname="",maketxt="",modeltxt="",yeartxt=1900-1-1,crash="",faildate=dateParseNYPD(str(notNone(NYRecord['DATE']))),fire='',injured=NYRecord['NUMBER OF PERSONS INJURED'],deaths=NYRecord['NUMBER OF PERSONS KILLED'],compdesc='',city=NYRecord['BOROUGH'],state='NY',country='US',zipcode=NYRecord['ZIP CODE'],latitude=NYRecord['LATITUDE'],longitude=NYRecord['LONGITUDE'],vin='',datea=dateParseNYPD(str(notNone('1900-01-01'))),ldate=dateParseNYPD(str(notNone('1900-01-01'))),miles=0,occurences=0,cdescr='',cmpltype='',prodtype='',airbagexpected=None, airbagdeployed=None,batchno=0)
#         except ValueError as ve:
#                 print(ve)        
#         keyUpdate=NYRecord['index']
#         session.add(ed_user)
#         session.commit()
#         break
        

    

# print("###################################################################")   
# print(mergeCount)
# print("###################################################################")   
# conn.execute(text("UPDATE public.\"KeyTracker\" SET \"lastIdValue\"=:x where \"tableNames\"='NYPD' and \"idRowName\"='index';"), x=keyUpdate) 
# session.commit()

































deployed_list = ['DEPLOYED WITHOUT WARNING','AIR BAG DEPLOYED','AIR BAG DEPLOY','AIR BAG DEPLOYMENT WAS VIOLENT','FALSE DEPLOYMENT OF DRIVER\'S/PASSENGER\'S AIR BAGS','PREMATURE DEPLOYMENT OF AIRBAG','AIRBAGS DEPLOYED CAUSING INJURIES','deployed','deploy','normal', 'as expected','premature deployment','EARLY','UNTIMELY deployment','BEFORE TIME,ill-timed','ill-timed deployment','early deployment','unforseen deployment','sudden deployment','abrupt deployment','suprise deployment','abnormal deployment','uncommon deployment','unanticipated deployment','unpredictable','unexpected deployment','unintentional deployment','unplanned deployment','unintented deployment','inadvertent deployment','involuntary deployment','failed deployment','accidental deployment','no apparent reason','unreasonable deployment','defective deployment','faulty deployment','late deployment','ACTIVATED THE DRIVERS AIR BAG','airbag activated','airbag triggered','triggered','airbag switched on','airbag inflated'] 
expected_list = ['NONE OF THE AIR BAGS','AIBAGS DID NOT DEPLOY','FALSE DEPLOYMENT OF DRIVER\'S AIRBAG','FALSE DEPLOYMENT','FALSE DEPLOYMENT OF DRIVER\'S/PASSENGER\'S AIR BAGS', 'PREMATURE DEPLOYMENT OF AIRBAG','PREMATURE DEPLOYMENT OF AIRBAGS','PREMATURE','EARLY','UNTIMELY','BEFORE TIME,ill-timed','ill-timed','too soon','unforseen','sudden','abrupt','supris','abnormal','uncommon','unanticipat','unpredict','unexpect','unintentional','unplanned','unintented','inadvertent','involuntary','failed','accidental','no apparent reason','defect','faulty','working badly' ,'malfunction','gone haywire','inoperative','flaw', 'unsound', 'distorted', 'inaccurate', 'incorrect', 'erroneous', 'imprecise', 'fallacious',' wrong','unfounded','not functioning','violent deployement' ,'unreasonable', 'DEPLOYED WITHOUT WARNING']
rs=conn.execute("select * from public.\"UsData_Clean\" where batchno=0 and compdesc in ('AIR BAGS','AIR BAGS:FRONTAL') ")

for record in rs:
    print(record['id'])
    
    for item in deployed_list:
        
        if item.lower()  in record['cdescr'].lower():
            conn.execute(text("UPDATE public.\"UsData_Clean\" SET \"airbagdeployed\"=1, batchno=0 where \"id\"=:x;"),x=record['id'])
            break
        else:
            continue


    for item in expected_list:
        
        if item.lower()  in record['cdescr'].lower():
            conn.execute(text("UPDATE public.\"UsData_Clean\" SET \"airbagexpected\"=0, batchno=0 where \"id\"=:x;"),x=record['id'])
            break
        else:
  
            continue
    session.commit()


        
















session.close()



conn.close()










if __name__=='__main__':
    app.run(use_reloader=False)