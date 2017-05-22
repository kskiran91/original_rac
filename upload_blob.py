#! /usr/bin/python
import cx_Oracle
import sys
import tinys3
import os
from random import randint

#all the os.environ values are the environment variables
staticPath = 'blobTest/' #change it to the Jenkins job path
dbUsername = os.environ['dbUsername']
password = os.environ['password']
host = os.environ['host']
port = '1521'
sid = os.environ['sid']

#optional s3 credentials
if os.environ.get('accessKeyId') is not None:
    accessKeyId     = os.environ['accessKeyId']
    secretAccessKey = os.environ['secretAccessKey']
    bucketname      = os.environ['bucketname']
else:
    accessKeyId = "null"
    secretAccessKey = "null"
    bucketname = "null"

tlsValue        = True

#--- function for writing blob data into FILE #
def write_file(data, filename):
    with open(filename, 'wb') as f:
        f.write(data)
args = sys.argv

#-- check if required attribute is passed----
if ( len(args) >=2 ):
    table = args[1]
else:
    print('Required Attribute table missing')
    sys.exit()

if( len(args) >=3 ):
    fromdate = args[2]
else:
    print('Required Attribute From date missing')
    sys.exit()

if ( len(args) >=4 ):
    todate = args[3]
else:
    print('Required Attribute To Date missing')
    sys.exit()


connection = cx_Oracle.connect(dbUsername+'/'+password+'@'+host+':'+port+'/'+sid)
print "DB connection - 1"
cursor = connection.cursor()

#----------------------1st table ------------------#
if ( table == 'VANKIOSKMEDIA'):
    print('---------------table1 start----------')
    # getting data between dates #
    querystring = "select * from ODSDADM.VANKIOSKMEDIA WHERE DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') AND CUSTOMERIMGURL is null"
    print "Queried the table -2"
    cursor.execute(querystring)
    data = cursor.fetchall()
    for row in data :
        customerId = row[0]
        customerImgId = row[2]
        datestamp = row[1]

        #GET BLOB DATA BECAUSE OF ERROR IN FETCHALL()
        queryForBlog = "select CUSTOMERIMG from ODSDADM.VANKIOSKMEDIA WHERE DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') AND CUSTOMERIMGURL is null AND CUSTOMERID ='"+str(customerId)+"' and CUSTOMERIMGID = '"+str(customerImgId) +"'"
        print "Queried for BLOB - 3"
        cursor.execute(queryForBlog)
        blobData = cursor.fetchone()
        blobFile = blobData[0]

        ID = row[5]
        query = "select VANSTOREID FROM ODSDADM.VANCUSTOMERENGAGEMENT WHERE CUSTOMERID = "+str(customerId)+" AND ROWNUM =1"
        print "VAN Store id query -4"
        cursor.execute(query)
        stores = cursor.fetchone()
        storeId = stores[0]

        #print(str(customerId)+'-'+str(customerImgId)+'-'+str(datestamp)+'-'+str(ID)+' staff id '+'='+str(storeId))
        randomInteger = randint(100000,999999)
        filename = str(randomInteger)+'_'+str(customerId)+'_'+str(storeId)+'_'+str(customerImgId)+'.png'
        if not blobFile:
                print('blob data not found for customerid = '+str(row[0]))
        else:
           #write file into path
           blobData = blobFile.read()
           write_file(blobData,filename)
           path = staticPath+filename

           #uploading file to s3
           if accessKeyId != "null":
               print "Connection to s3 -5"
               conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
               f = open(path,'rb')
               uploadFlag  = conn.upload(filename,f,bucketname)
               if(uploadFlag.status_code != 200):
                   print('file was not upload to s3 because of some error for customerid ='+str(row[0])+' the status code is '+str(uploadFlag.status_code))
                   sys.exit()
               print "Removing the file -6"   
               os.remove(filename)

           #updating database
           querystring = "UPDATE ODSDADM.VANKIOSKMEDIA set CUSTOMERIMGURL ='"+filename+"' WHERE CUSTOMERID = '"+str(customerId)+"' and ID ='"+str(ID)+"' and CUSTOMERIMGID = '"+str(customerImgId) +"'"
           print "Updating the DB -7"
           cursor.execute(querystring)
    print('-------------end for table1-----------')


#--------------------2nd table -----------------------#
if(table == 'VANBENEFITSPLUSSTORAGE' ):
    print('--------------table2 start-------------')
    querystring = "select * from ODSDADM.VANBENEFITSPLUSSTORAGE  WHERE DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') AND BENEFITSPLUSUNSIGNEDURL is null AND BENEFITSPLUSSIGNEDURL is null"
    cursor.execute(querystring)

    #-- getting data for 2nd table #
    data = cursor.fetchall()
    for row in data:
        customerId   = row[0]

        #---unsigned blob data getting ---#
        queryForBlog = "select BENEFITSPLUSUNSIGNED from ODSDADM.VANBENEFITSPLUSSTORAGE  WHERE DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') AND BENEFITSPLUSUNSIGNEDURL is null AND BENEFITSPLUSSIGNEDURL is null AND CUSTOMERID = '"+str(customerId)+"'"
        cursor.execute(queryForBlog)
        blobData     = cursor.fetchone()
        unsignedBlob = blobData[0]

        #---signed blob data getting --#
        queryForBlog = "select BENEFITSPLUSSIGNED from ODSDADM.VANBENEFITSPLUSSTORAGE  WHERE DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') AND BENEFITSPLUSUNSIGNEDURL is null AND BENEFITSPLUSSIGNEDURL is null AND CUSTOMERID = '"+str(customerId)+"'"
        cursor.execute(queryForBlog)
        blobData     = cursor.fetchone()
        signedBlob   = blobData[0]

        query = "select VANSTOREID FROM ODSDADM.VANCUSTOMERENGAGEMENT WHERE CUSTOMERID = "+str(customerId)+" AND ROWNUM =1"
        cursor.execute(query)
        stores = cursor.fetchone()
        storeId = stores[0]
        randomInteger = randint(100000,999999)
        unsignedFilename = str(randomInteger)+'_'+str(customerId)+'_'+str(storeId)+'_docTemplate-unsigned.pdf'
        if not unsignedBlob:
                print('unsignedblob data not found for customerid = '+str( customerId ) )
        else:
            #create file for 2nd table unsigned blob
            unsignedBlobData = unsignedBlob.read()
            write_file(unsignedBlobData,unsignedFilename)
            path = staticPath+unsignedFilename

            #uploading file for 2nd table unsigned blob
            if accessKeyId != "null":
	        conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
                f = open(path,'rb')
                uploadFlag = conn.upload(unsignedFilename,f,bucketname)
                if(uploadFlag.status_code != 200):
                    print('unsigned blob file was not upload to s3 because of some error for customerid = '+str( customerId )+' the status code is '+str(uploadFlag.status_code))
                    sys.exit()
                os.remove(unsignedFilename)

            #updating url in database
            querystring = "UPDATE ODSDADM.VANBENEFITSPLUSSTORAGE set BENEFITSPLUSUNSIGNEDURL ='"+unsignedFilename+"' where CUSTOMERID = '"+str(customerId)+"'"
            cursor.execute(querystring)

        if not signedBlob:
                print('signedblob data not found for customerid = '+str( customerId ) )
        else:
            #create file for 2nd table signed blob
            signedFilename = str(randomInteger)+'_'+str(customerId)+'_'+str(storeId)+'_docTemplate.pdf'
            signedBlobData = signedBlob.read()
            write_file(signedBlobData,signedFilename)
            path = staticPath+signedFilename

            #uploading file for 2nd table signed blob
            if accessKeyId != "null":
	        conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
                f = open(path,'rb')
                uploadFlag = conn.upload(signedFilename,f,bucketname)
                if(uploadFlag.status_code != 200):
                    print('signedblob blob file was not upload to s3 because of some error for customerid = '+str( customerId )+' the status code is '+str(uploadFlag.status_code))
                    sys.exit()
                os.remove(signedFilename)

            #updating url in database
            querystring = "UPDATE ODSDADM.VANBENEFITSPLUSSTORAGE set BENEFITSPLUSSIGNEDURL ='"+signedFilename+"' where CUSTOMERID = '"+str(customerId)+"'"
            cursor.execute(querystring)

    print('-------------end for table2--------------')

#---------------3rd table -------------#
if(table == 'VANCUSTOMERAGREEMENT' ):
        print('--------------table3 start-------------')
        querystring = "select * from ODSDADM.VANCUSTOMERAGREEMENT where DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') and  UNSIGNEDDOCURL  is null AND SIGNEDDOCURL  is null"
        cursor.execute(querystring)
        data = cursor.fetchall()
        for row in data:
                engagementId = row[0]
                doctype      = row[1]

                queryForBlob = "select UNSIGNEDDOC from ODSDADM.VANCUSTOMERAGREEMENT where DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') and  UNSIGNEDDOCURL  is null AND SIGNEDDOCURL  is null AND ENGAGEMENTID = '"+str(engagementId)+"' AND DOCTYPE ='"+str(doctype)+"'"
                cursor.execute(queryForBlob)
                blobData     = cursor.fetchone()
                unsignedDoc  = blobData[0]

                queryForBlob = "select SIGNEDDOC from ODSDADM.VANCUSTOMERAGREEMENT where DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') and  UNSIGNEDDOCURL  is null AND SIGNEDDOCURL  is null AND ENGAGEMENTID = '"+str(engagementId)+"' AND DOCTYPE ='"+str(doctype)+"'"
                cursor.execute(queryForBlob)
                blobData     = cursor.fetchone()
                signedDoc    = blobData[0]

                query = "select VANSTOREID from ODSDADM.VANCUSTOMERENGAGEMENT where ENGAGEMENTID = "+str(engagementId)+" AND ROWNUM =1"
                cursor.execute(query)
                stores = cursor.fetchone()
                storeId      = stores[0]
                randomInteger = randint(100000,999999)
                unsignedFilename = str(randomInteger)+'_'+str(engagementId)+'_'+str(storeId)+'_docTemplate-unsigned.pdf'
                if not unsignedDoc:
                        print('unsignedDoc Blob not found for engagement id = '+str(engagementId) )
                else:
                        unsignedBlobData = unsignedDoc.read()
                        write_file(unsignedBlobData,unsignedFilename)
                        path = staticPath+unsignedFilename
                        if accessKeyId != "null":
			    conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
                            f = open(path,'rb')
                            uploadFlag = conn.upload(unsignedFilename,f,bucketname)
                            if(uploadFlag.status_code != 200):
                                    print('unsignedDoc blob file was not upload to s3 because of some error for engagement id = '+str(engagementId)+' the status code is '+str(uploadFlag.status_code))
                                    sys.exit()

                            os.remove(unsignedFilename)
                        querystring = "UPDATE ODSDADM.VANCUSTOMERAGREEMENT set UNSIGNEDDOCURL ='"+unsignedFilename+"' where ENGAGEMENTID = '"+str(engagementId)+"' AND DOCTYPE ='"+str(doctype)+"'"
                        cursor.execute(querystring)

                if not signedDoc:
                        print('signedDoc Blob not found for engagement id = '+str(engagementId) )
                else:
                        signedFilename = str(randomInteger)+'_'+str(engagementId)+'_'+str(storeId)+'_docTemplate.pdf'
                        signedBlobData = signedDoc.read()
                        write_file(signedBlobData,signedFilename)
                        path = staticPath+signedFilename
                        if accessKeyId != "null":
			    conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
                            #print conn
                            f = open(path,'rb')
                            uploadFlag = conn.upload(signedFilename,f,'racrdstestankurbucket')
                            if(uploadFlag.status_code != 200):
                                    print('signedDoc blob file was not upload to s3 because of some error for engagement id = '+str(engagementId)+' the status code is '+str(uploadFlag.status_code))
                                    sys.exit()
                            os.remove(signedFilename)
                        querystring = "UPDATE ODSDADM.VANCUSTOMERAGREEMENT set SIGNEDDOCURL ='"+signedFilename+"' where ENGAGEMENTID = '"+str(engagementId)+"' AND DOCTYPE ='"+str(doctype)+"'"
                        cursor.execute(querystring)
        print('-------------end for table3--------------')

connection.commit()
cursor.close ()
sys.exit()

