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
#DATABASE_URL = os.environ['DATABASE_URL']

#conn = psycopg2.connect(DATABASE_URL, sslmode='require')
engine = create_engine('postgresql+psycopg2://postgres:blessing12@localhost/data_extractor', echo=True)
app.config['SQLALCHEMY_DATABASE_URI']='postgresql+psycopg2://postgres:blessing12@localhost/data_extractor'


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
        avgtempc=Column(Float)
        avgtempf=Column(Float)
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
        batchno=Column(Integer)

        def  __init__(self,cmplid,index,odino,mfrname,maketxt,modeltxt,yeartxt,crash,faildate,fire,injured,deaths,compdesc,city,state,country,zipcode,latitude,longitude,avgtempc,avgtempf,vin,datea,ldate,miles,occurences,cdescr,cmpltype,prodtype,airbagexpected,airbagdeployed,batchno):
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
            self.avgtempc=avgtempc
            self.avgtempf=avgtempf
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
            return f"UsData_Clean('{self.id}','{self.cmplid}','{self.index}','{self.odino}','{self.mfrname}','{self.maketxt}','{self.modeltxt}','{self.yeartxt}','{self.crash}','{self.faildate}','{self.fire}','{self.injured}','{self.deaths}','{self.compdesc}','{self.city}','{self.state}','{self.country}','{self.zipcode}','{self.latitude}','{self.longitude}','{self.avgtempc}','{self.avgtempf}','{self.vin}','{self.datea}','{self.ldate}','{self.miles}','{self.occurences}','{self.cdescr}','{self.cmpltype}','{self.prodtype}','{self.airbagexpected}','{self.airbagdeployed}','{self.batchno}')"





            


#metadata.create_all(engine)

Base.metadata.create_all(engine)

dfUs=pd.read_csv("FLAT_CMPL_airbag_Make_filter.csv",encoding='latin-1',header=0,index_col='cmplid')


dfUs.to_sql('UsData', engine,index_label='cmplid',if_exists='replace')






# my_json=requests.get('http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=3e7b42d0ddf44d79ba813121192705&q={city}&format=json&date=2009-02-25').content.decode('utf8').replace("'", '"')
# respose=json.loads(my_json)
# print(json.dumps(json.loads(my_json), indent=4, sort_keys=True))
# print("######################################################################")
# print(respose['data']['weather'][0]['avgtempF'])
# #print(respose['data']['weather'][0]['hourly'])

# print()





# dfUs=pd.read_csv("NYPD.csv",encoding='latin-1',header=0)
# print(dfUs.head(n=5))
# #dfUs.head(n=5)
# #engine1 = create_engine('postgresql+psycopg2://postgres:Wimtech2019@localhost/data_extractor')

# dfUs.to_sql('NYPD', engine,if_exists='replace')

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
    ed_user = UsData_Clean(cmplid=record['cmplid'],index=None,odino=record['odino'],mfrname=record['mfrname'],maketxt=record['maketxt'],modeltxt=record['modeltxt'],yeartxt=record['yeartxt'],crash=record['crash'],faildate=dateParse(str(notNone(record['faildate']))),fire=record['fire'],injured=record['injured'],deaths=record['deaths'],compdesc=record['compdesc'],city=record['city'],state=record['state'],country='US',zipcode=None,latitude=None,longitude=None,avgtempc=0,avgtempf=0,vin=record['vin'],datea=dateParse(str(notNone(record['datea']))),ldate=dateParse(str(notNone(record['ldate']))),miles=record['miles'],occurences=record['occurences'],cdescr=record['cdescr'],cmpltype=record['cmpltype'],prodtype=record['prodtype'],airbagexpected=None, airbagdeployed=None, batchno=0)
    session.add(ed_user)
    session.commit()




HEADERS = {'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5)'
                          'AppleWebKit/537.36 (KHTML, like Gecko)'
                          'Chrome/45.0.2454.101 Safari/537.36'),
                          'referer': 'http://stats.nba.com/scores/'}
# Then add this to your response get:

# response = requests.get(url, headers=HEADERS)

rs=conn.execute("select * from public.\"UsData_Clean\" where faildate >='2009-01-01'")

for record in rs:
    url='http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=3e7b42d0ddf44d79ba813121192705&q={}&format=json&date={}'.format(record['city'],record['faildate'])
    print(url)
    my_json=requests.get(url,headers=HEADERS).content.decode('utf8').replace("'",'"')
    respose=json.loads(my_json)

    try:
        avgtempfar=respose['data']['weather'][0]['avgtempF']
        avgtempcel=respose['data']['weather'][0]['avgtempC']
        conn.execute(text("UPDATE public.\"UsData_Clean\" SET \"avgtempc\"=:avgtempcelsius ,\"avgtempf\"=:avgtempfarhen where \"id\"=:x;"),x=record['id'],avgtempcelsius=avgtempcel,avgtempfarhen=avgtempfar)
        session.commit()
    except KeyError as ve:
        print(ve) 
    
   






 








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























### start here to uncomment


 expectedYes_list=['NO DEPLOYMENT OF','AIR BAG FAILURE','NO DEPLOYMENT OF BOTH AIR BAGS','NO DEPLOYMENT OF BOTH AIRBAGS','NOT DEPLOYING ANY AIR BAGS','AIRBAG FAILED TO DEPLOY','AIR BAGS FAILED','AIRBAG HAS NEVER COME OUT','AIRBAG FAILED','AIR BAG FAILED','AIRBAGS FAILED','AIRBAGS FAIL TO DEPLOY','DID NOT FULLY INFLATE','AIR BAG FAIL TO DEPLOY','AIR BAGS FAIL TO DEPLOY','AIRBAG FAIL TO DEPLOY','AIRBAGS DID NOT INFLATE','AIRBAG FAILED TO DEPLOY','AIR BAG FAILED TO DEPLOY','AIR BAGs FAILED TO DEPLOY','AIRBAG FAILED TO DEPLOY','AIRBAG DID NOT INFLATE','AIR BAG DID NOT INFLATE','AIRBAGS DID NOT DEPLOY','AIR BAG DID NOT DEPLOY','Airbags didn\'t deploy','AIR BAG DIDN\'t DEPLOY','NO DEPLOYMENT OF DRIVER\'S AIR BAG','NO DEPLOYMENT OF DRIVER\'S AIRBAG','NO DEPLOYMENT OF DRIVER\'S AIRBAGS','AIR BAGS DID NOT DEPLOY','NO DEPLOYMENT OF AIRBAG','NO DEPLOYMENT OF AIR BAG','No Deployment of airbags','No Deployment of airbags','AIR BAG DIDN\'T DEPLOY','AIRBAG DIDN\'T DEPLOY''DEPLOYED WITHOUT WARNING','AIR BAG DEPLOYMENT WAS VIOLENT','AIR BAG DEPLOY','AIR BAG DEPLOYMENT WAS VIOLENT','violent deployment of airbag','violent deployment of air bag','violent deployment of airbags','FALSE DEPLOYMENT OF DRIVER\'S/PASSENGER\'S AIR BAGS','PREMATURE DEPLOYMENT OF AIRBAG','AIRBAGS DEPLOYED CAUSING INJURIES','deployed','normal','as expected','premature deployment','EARLY','UNTIMELY deployment','BEFORE TIME,ill-timed','ill-timed deployment','early deployment','unforseen deployment','sudden deployment','abrupt deployment','suprise deployment','abnormal deployment','uncommon deployment','unanticipated deployment','unpredictable','unexpected deployment','unintentional deployment','unplanned deployment','unintented deployment','inadvertent deployment','involuntary deployment','failed deployment','accidental deployment','no apparent reason','unreasonable deployment','defective deployment','faulty deployment','late deployment','ACTIVATED THE DRIVERS AIR BAG','airbag activated','airbag triggered','triggered','airbag switched on','airbag inflated','AIR BAG DEPLOYMENT FAILURE']


 deployedNo_list=['AIR BAG\'S EXPERIENCED NO DEPLOYMENT','AIR BAG\'S EXPERIENCED NO DEPLOYMENT','AIRBAG EXPERIENCED NO DEPLOYMENT','AIRBAG\'S EXPERIENCED NO DEPLOYMENT','NO DEPLOYMENT OF','AIR BAGS FAILED','AIRBAG FAILED','AIRBAG DID NOT DEPLOY','AIRBAG HAS NEVER COME OUT','NO DEPLOYMENT OF BOTH AIR BAGS','NO DEPLOYMENT OF BOTH AIRBAGS','NOT DEPLOYING ANY AIR BAGS','AIR BAG FAILED','AIRBAGS FAILED','AIRBAGS FAIL TO DEPLOY','DID NOT FULLY INFLATE','AIR BAG FAIL TO DEPLOY','AIR BAGS FAIL TO DEPLOY','AIRBAG FAIL TO DEPLOY','AIRBAGS DID NOT INFLATE','AIRBAG FAILED TO DEPLOY','AIR BAG FAILED TO DEPLOY','AIR BAGs FAILED TO DEPLOY','AIRBAG FAILED TO DEPLOY','AIRBAG DID NOT INFLATE','AIR BAG DID NOT INFLATE','AIRBAGS DID NOT DEPLOY','AIR BAG DID NOT DEPLOY','Airbags didn\'t deploy','AIR BAG DIDN\'t DEPLOY','NO DEPLOYMENT OF DRIVER\'S AIR BAG','NO DEPLOYMENT OF DRIVER\'S AIRBAG','NO DEPLOYMENT OF DRIVER\'S AIRBAGS','AIR BAGS DID NOT DEPLOY','NO DEPLOYMENT OF AIRBAG','NO DEPLOYMENT OF AIR BAG','No Deployment of airbags','No Deployment of airbags','AIR BAG DIDN\'T DEPLOY','AIRBAG DIDN\'T DEPLOY','AIR BAGS DIDN\'T DEPLOY']

 deployedYes_list=['AIR BAGS HAD NO DEPLOYMENT','AIRBAGS HAD NO DEPLOYMENT','AIR BAG HAD NO DEPLOYMENT','AIRBAG HAD NO DEPLOYMENT','DEPLOYED WITHOUT WARNING','AIR BAG DEPLOYMENT WAS VIOLENT','AIR BAG DEPLOY','AIR BAG DEPLOYMENT WAS VIOLENT','violent deployment of airbag','violent deployment of air bag','violent deployment of airbags','FALSE DEPLOYMENT OF DRIVER\'S/PASSENGER\'S AIR BAGS','PREMATURE DEPLOYMENT OF AIRBAG','AIRBAGS DEPLOYED CAUSING INJURIES','deployed','normal','as expected','premature deployment','EARLY','UNTIMELY deployment','BEFORE TIME,ill-timed','ill-timed deployment','early deployment','unforseen deployment','sudden deployment','abrupt deployment','suprise deployment','abnormal deployment','uncommon deployment','unanticipated deployment','unpredictable','unexpected deployment','unintentional deployment','unplanned deployment','unintented deployment','inadvertent deployment','involuntary deployment','failed deployment','accidental deployment','no apparent reason','unreasonable deployment','defective deployment','faulty deployment','late deployment','ACTIVATED THE DRIVERS AIR BAG','airbag activated','airbag triggered','triggered','airbag switched on','airbag inflated','AIR BAG DEPLOYMENT FAILURE']
 expectedNo_list=['VIOLENT DEPLOYMENT OF PASSENGER SIDE AIRBAG','VIOLENT DEPLOYMENT OF PASSENGER SIDE AIR BAG','AIR BAGS DEPLOYMENT WAS VIOLENT','AIRBAGS DEPLOYMENT WAS VIOLENT','AIR BAG DEPLOYMENT WAS VIOLENT','AIRBAG DEPLOYMENT WAS VIOLENT','AIRBAG EXPLODED','AIR BAG EXPLODED','AIRBAG HAS MELTED','AIR BAG HAS MELTED','AIR BAG DEPLOYED WITH A FORCE','AIR BAG DEPLOYMENT WAS VIOLENT','FALSE DEPLOYMENT OF DRIVER\'S AIRBAG','violent deployment of airbag','violent deployment of air bag','violent deployment of airbags','FALSE DEPLOYMENT','FALSE DEPLOYMENT OF DRIVER\'S/PASSENGER\'S AIR BAGS','PREMATURE DEPLOYMENT OF AIRBAG','PREMATURE DEPLOYMENT OF AIRBAGS','PREMATURE','EARLY','UNTIMELY','BEFORE TIME,ill-timed','ill-timed','too soon','BURNS CAUSED BY DRIVERS AIR BAG','BURNS CAUSED BY DRIVERS AIRBAG','unforseen','abrupt deployment','supris','abnormal','uncommon','unanticipat','unpredict','unexpect','unintentional','unplanned','unintented','inadvertent','involuntary','accidental','no apparent reason','defect','faulty','working badly','malfunction','gone haywire','inoperative','distorted','inaccurate','incorrect','erroneous','imprecise','fallacious','wrongly deployed','wrongful deployment','unfounded','violent deployement','unreasonable','DEPLOYED WITHOUT WARNING','AIR BAG DEPLOYED WITHOUT WARNING','AIRBAGS DEPOLYED WITHOUT WARNING','AIR BAGS DEPOLYED WITHOUT WARNING','AIRBAG DEPOLYED WITHOUT WARNING']
    

 rs=conn.execute("select * from public.\"UsData_Clean\" where batchno=0 and compdesc in ('AIR BAGS','AIR BAGS:FRONTAL') ")

 for record in rs:
     for item in deployedYes_list:
       
         if item.lower()  in record['cdescr'].lower():
             conn.execute(text("UPDATE public.\"UsData_Clean\" SET \"airbagdeployed\"='1' where \"id\"=:x;"),x=record['id'])
             break
         else:
             continue


     for item in expectedNo_list:
        
         if item.lower()  in record['cdescr'].lower():
             conn.execute(text("UPDATE public.\"UsData_Clean\" SET \"airbagexpected\"='0' where \"id\"=:x;"),x=record['id'])
             break
         else:
             continue

     for item in deployedNo_list:
        
         if item.lower()  in record['cdescr'].lower():
             conn.execute(text("UPDATE public.\"UsData_Clean\" SET \"airbagdeployed\"='0' where \"id\"=:x;"),x=record['id'])
             break
         else:
             continue
     for item in expectedYes_list:
        
         if item.lower()  in record['cdescr'].lower():
             conn.execute(text("UPDATE public.\"UsData_Clean\" SET \"airbagexpected\"='1' where \"id\"=:x;"),x=record['id'])
             break
         else:
             continue




     session.commit()


 session.close()



 conn.close()
#####End Here to uncomment
        


# for record in rs:
#     print(record['id'])
























#ed_user = UsData_Clean(cmplid=6, odino=6,mfrname ='we',maketxt='de',modeltxt='wre',yeartxt=1232,crash='N',faildate=dateParse('19990811'),fire='N',injured =1,deaths=1,compdesc='qweq',city='op',state='ui',vin='q123',datea=dateParse('19990811'),ldate=dateParse('19990811'),miles=91,occurences=2,cdescr='qwew',cmpltype='wer',prodtype='sdf',batchno=0)
#


#someselect.order_by(desc(table1.mycol))

#session.query(Base).join(Base.owner).order_by(Player.name)















# postgresql://postgres:Wimtech2019@localhost/data_extractor







#ins = UsData_Clean.insert()
#conn.execute(ins, id=1, cmplid=1, fullname='Wendy Williams')

if __name__=='__main__':
    app.run(use_reloader=False,debug=True)