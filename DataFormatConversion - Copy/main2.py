import os
import boto3
from botocore.exceptions import ClientError

filename = '20230217_segments_3tc89a.tsv'
targetFolder = "converted/"

def createFile(gaid,segmentId,mode, add_or_del):
    targetFileName = segmentId+"_"+add_or_del+".txt"
    targetFilePath = targetFolder + targetFileName
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

def convertFiles():
    with open(filename,'r') as file:
        lines=file.readlines() 
        arrDel = []
        arrAdd = []   

        for line in lines:       
            splitedLine = line.split('\t')
            allSegmentIds = splitedLine[2].strip()
            splitedSegmentIds = allSegmentIds.split('-')
            print(splitedSegmentIds)

            if len(splitedSegmentIds)>0:
                if splitedSegmentIds[0] != "":
                    addsegmentids = splitedSegmentIds[0].split(',')

                    for segmentId in addsegmentids:
                        if id in arrAdd:
                            createFile(splitedLine[0],segmentId,"a","add")
                        else:
                            arrAdd.append(id)
                            createFile(splitedLine[0],segmentId,"w","add")

            if len(splitedSegmentIds)>1:
                if splitedSegmentIds[1] != "":
                    delsegmentids = splitedSegmentIds[1].split(',')

                    for segmentId in delsegmentids:
                        if id in arrAdd:
                            createFile(splitedLine[0],segmentId,"a","del")
                        else:
                            arrAdd.append(id)
                            createFile(splitedLine[0],segmentId,"w","del")

def uploadToS3():
    for filename in os.listdir(targetFolder):
        targetBucketName = "hw51rddmp"
        targetUploadPath = "1088513091236366080/converted/"
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
    
convertFiles()
uploadToS3()