import os
import boto3
from botocore.exceptions import ClientError
import requests
import datetime
import time
import sqlite3
import hmac
import hashlib
import pandas as pd
import gzip
import shutil


date = datetime.date.today().strftime('%Y%m%d')
filename_uploaded = f'converted/{date}/uploaded.txt'
targetFolder = f'converted/{date}/'
DownloadFolder = f'download/{date}/'

conn = sqlite3.connect('meta.db') 
cursor = conn.cursor()




def createFile(gaid,segmentId,segmentValue,segmentPrice,mode, add_or_del):
    targetFileName = segmentId+"--"+add_or_del+"--"+segmentValue+"--"+segmentPrice+"--"+".txt"
    targetFilePath = f'{targetFolder}{targetFileName}'
    os.makedirs(os.path.dirname(targetFilePath), exist_ok=True)
    f = open(targetFilePath,mode)
    f.write(gaid+"\t"+'2'+"\n")
    f.close()

def writeS3FileUrl(url):
    targetFileName = "uploaded.txt"
    targetFilePath = targetFolder + targetFileName
    f = open(targetFilePath,"a")
    f.write(url + "\n")
    f.close()



def downloadFromS3():
    # Set up an region
    region = "ap-southeast-1"
    # # Define the name of the bucket and the object key of the folder
    bucket_name = "hw51rddmp"
    #????? wrong filefolder?????
    xx = "20230219"
    folder_key = f'1088513091236366080/{xx}/'
    # Set up an S3 client
    s3 = boto3.client('s3',
        aws_access_key_id="AKIA56GUWCT35UMJOYVT",
        aws_secret_access_key="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",
        region_name = region)

    # Create a local folder to save the downloaded files
    local_folder = DownloadFolder
    os.makedirs(local_folder, exist_ok=True)

    # List all objects in the folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_key)
    #print(response)
    
    # Download each object in the folder
    for obj in response['Contents']:
        object_key = obj['Key']
        local_file = os.path.join(local_folder, os.path.basename(object_key))
        s3.download_file(bucket_name, object_key, local_file)
        print(f'Downloaded {object_key} to {local_file}')
                
        # Check if the downloaded file is a .gz file
        if local_file.endswith('.gz'):
            # Open the compressed file in binary mode
            with gzip.open(local_file, 'rb') as f_in:
                # Open the output file in binary mode
                with open(local_file[:-3], 'wb') as f_out:
                    # Copy the contents of the compressed file to the output file
                    shutil.copyfileobj(f_in, f_out)
            print(f'Extracted {local_file} to {local_file[:-3]}')





def searchValAndPrice(segmentid):
        t = (segmentid,)
        #only select the value and price
        cursor.execute('SELECT Value,price FROM META WHERE ID=?', t)
        onemeta = cursor.fetchone()
        return onemeta[0],onemeta[1]


def createMetaTable():
    with conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS META
        (
            id text primary key,
            Value text,
            price text 
        )
        ''')

def createMetaDB():
    createMetaTable()

    # Define the path to the folder
    folder_path = DownloadFolder
    # Open the folder and read its contents
    for root, dirs, files in os.walk(folder_path):
        # Check if any of the subfolders contain 'meta' in their name
        for file_name in files:
            if 'meta' in file_name:
                filename_meta = os.path.join(root, file_name)

                with open(filename_meta,'r') as file:
                    lines_segament=file.readlines()
                    for line in lines_segament: 
                        line = line.strip()
                        splitedLine_segament = line.split('\t')
                        onemeta =(splitedLine_segament[0], splitedLine_segament[1], splitedLine_segament[2],\
                                splitedLine_segament[1], splitedLine_segament[2],splitedLine_segament[0])
                        print(onemeta)
                        with conn:
                            conn.execute('''
                            INSERT INTO META (id,Value,price) VALUES (?,?,?)
                            ON CONFLICT(id) DO UPDATE SET
                            Value=?, price=?
                            WHERE id=?
                            ''', onemeta)


def closeDB():
    conn.commit()
    cursor.close()
    conn.close()


def convertFiles():
    # Define the path to the folder
    folder_path = DownloadFolder
    # Open the folder and read its contents
    for root, dirs, files in os.walk(folder_path):
        # Check if any of the subfolders contain 'meta' in their name
        for file_name in files:
            if 'segments' in file_name:
                filename_segment = os.path.join(root, file_name)


                with open(filename_segment,'r') as file:
                    lines=file.readlines() 
                    arrDel = []
                    arrAdd = []  

                    for line in lines:       
                        splitedLine = line.split('\t')
                        allSegmentIds = splitedLine[2].strip()
                        splitedSegmentIds = allSegmentIds.split('-')

                        if len(splitedSegmentIds)>0:
                            if splitedSegmentIds[0] != "":
                                addsegmentids = splitedSegmentIds[0].split(',')
                                
                                for segmentId in addsegmentids:
                                    metaData = searchValAndPrice(segmentId)
                                    segmentValue = metaData[0]
                                    segmentPrice = metaData[1]

                                    if segmentId in arrAdd:
                                        createFile(splitedLine[0],segmentId,segmentValue,segmentPrice,"a","add")
                                    else:
                                        arrAdd.append(segmentId)
                                        createFile(splitedLine[0],segmentId,segmentValue,segmentPrice,"w","add")                                                 
                
                            
                        if len(splitedSegmentIds)>1:
                            if splitedSegmentIds[1] != "":
                                delsegmentids = splitedSegmentIds[1].split(',')

                                for segmentId in delsegmentids:
                                    metaData = searchValAndPrice(segmentId)
                                    segmentValue = metaData[0]
                                    segmentPrice = metaData[1]
                                    if segmentId in arrDel:
                                        createFile(splitedLine[0],segmentId,segmentValue,segmentPrice,"a","del")
                                    else:
                                        arrDel.append(segmentId)
                                        createFile(splitedLine[0],segmentId,segmentValue,segmentPrice,"w","del")




def uploadToS3():
    for filename in os.listdir(targetFolder):
        targetBucketName = "hw51rddmp"
        targetUploadPath = f'1088513091236366080/converted/{date}/'
        region = "ap-southeast-1"
        filepath = os.path.join(targetFolder,filename)

        s3 = boto3.Session(
            aws_access_key_id = "AKIA56GUWCT35UMJOYVT",
            aws_secret_access_key = "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",
            region_name = region
        ).resource('s3')
        
        try:
            s3.meta.client.upload_file(filepath,targetBucketName,targetUploadPath+filename)

            url = f'https://{targetBucketName}.s3.{region}.amazonaws.com/{targetUploadPath}{filename}'
            writeS3FileUrl(url)
        except ClientError as e:
            print(e)
    
    print("All file(s) uploaded")








def SHA256HASH(key,data):
    message = data
    bkey = key.encode('utf-8')
    hash_object = hmac.new(bkey, message.encode(), hashlib.sha256)
    hex_dig = hash_object.hexdigest()
    return hex_dig


def createPostHeader(realm):
    appid = "DMP1117364620374311808"
    method = "POST"
    wk = "Ew8ZcE1EUsve56t//u35i7Ti1CAExq7eERhaCO79njw="
    nonce = time.time()

    request_info = "opendmp/v2/audience/"+realm
    encryptionKey = f'{appid}:{request_info}:{wk}'
    encryptionData = f'{nonce}:{method}:{request_info}'
    digest = SHA256HASH(encryptionKey, encryptionData)
    authString = f'Digest username={appid},realm={request_info},nonce={nonce},response={digest},algorithm=HMACSHA256'
    return authString


def caculateScale(filepath): 
    df = pd.read_csv(filepath, sep='\t', header=None)
    num_lines = df.shape[0]
    return num_lines


def invokeHuaweiDMPAPI():
    with open (filename_uploaded,'r')as file:
        lines_url = file.readlines()
        for line in lines_url:
            line = line.strip()
            splitedLine_url = line.split('--')
            adrequest = splitedLine_url[len(splitedLine_url)-4]

            linesplit = line.split('/')
            AudienceFilename = linesplit[len(linesplit)-1]
            AudienceFilepath = f'converted/{date}/{AudienceFilename}'
            #print(AudienceFilepath)
            Scale = caculateScale(AudienceFilepath)
            print(Scale)



            price = format(float(splitedLine_url[len(splitedLine_url)-2]),'.4f')
            value = splitedLine_url[len(splitedLine_url)-3]
            audienceID = splitedLine_url[0].split('/')
            audienceID = audienceID[len(audienceID)-1] 
            dmpid = "1117364620374311808"      
            # print(price)
            # print(value)
            # print(audienceID)     

            if adrequest == "add":
                adrequest = "add"
                authHeader = createPostHeader(adrequest)
                url_add = "https://svc-dra.ads.huawei.com/opendmp/v2/audience/add"
    
                #URL:
                AudiencePackag = {
                    "siteID": 7,
                    "downloadUrls":[line],
                    "deviceIDType": 2
                }

                #price
                priceBusinessInfo = {
                    "price":price,
                    "currency":"USD"
                }

                #audiences:
                audienceinfo = {
                    "id": audienceID,
                    "name": [{
                        "language":"en",
                        "value":value
                    }],
                    "description": [{
                        "language":"en",
                        "value":value
                    }],
                    "scale": Scale,
                    "URL": [AudiencePackag],
                    "price": [priceBusinessInfo]
                }

                #request message body_add
                params_add = {
                    "dmpID": dmpid,
                    "audiences": [audienceinfo],
                    "siteID": 7
                }

                response = requests.post(url_add,authHeader,params_add)
                print(response.content)

            else:
                adrequest = "delete"
                authHeader = createPostHeader(adrequest)
                url_del = "https://svc-dra.ads.huawei.com/opendmp/v2/audience/delete"

                #request message body_del
                params_del = {
                    "dmpID": dmpid,
                    "audiencesIDs": audienceID
                }

                response = requests.post(url_del,authHeader,params_del)
                print(response.content)


#downloadFromS3()
createMetaDB()   
convertFiles()
closeDB()
uploadToS3()
invokeHuaweiDMPAPI()

