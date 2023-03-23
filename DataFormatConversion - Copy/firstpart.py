filename = '20230217_segments_3tc89a.tsv'

# filename = input("Please input the name of the file:\n \
#                   You can choose from the following files:\n \
#                   1.20230217_segments_3tc89a.tsv\n \
#                   2.20230213_segments_3tc89a.tsv\n")

with open(filename,'r') as file:
    list=file.readlines() #使用这个之后后面无法再次使用readlines,所以先用list储存起来
    alldeletsegamentid = []
    alladdsegamentid = []   #创建一个空列表

    for line in list:       #自动形成一个list的遍历
        oneline = line.split('\t')
        allsegamentid = oneline[2].split('-')
        print(allsegamentid)

        if len(allsegamentid)==1:
            addsegamentids = allsegamentid[0]
            addsegamentid = addsegamentids.split(',')

            for id in addsegamentid:
                id = id.strip()       #去除字符串中的转义字符

                if id in alladdsegamentid:
                    #print("id already exists")
                    f = open(id+"add.txt","a")
                    f.write(oneline[0]+"\t"+'2'+"\n")
                    f.close()
                else:
                    alladdsegamentid.append(id)
                    f = open(id+"add.txt","w")
                    f.write(oneline[0]+"\t"+'2'+"\n")
                    f.close()

        if len(allsegamentid)==2:
            addsegamentids = allsegamentid[0]
            addsegamentid = addsegamentids.split(',')
            deletsegamentids = allsegamentid[1]
            deletsegamentid = deletsegamentids.split(',')
            
            for id1 in addsegamentid:
                id1 = id1.strip()       #去除字符串中的转义字符

                if id1 == '':
                    print('all deltet')
                    break               #终止循环
                elif  id1 in alladdsegamentid:
                    f = open(id1+"add.txt","a")
                    f.write(oneline[0]+"\t"+'2'+"\n")
                    f.close()
                else:
                    alladdsegamentid.append(id1)
                    f = open(id1+"add.txt","w")
                    f.write(oneline[0]+"\t"+'2'+"\n")
                    f.close()

            for id2 in deletsegamentid:
                id2 = id2.strip()       #去除字符串中的转义字符
                if id2 in alldeletsegamentid:
                    #print("id already exists")
                    f = open(id2+"delet.txt","a")
                    f.write(oneline[0]+"\t"+'2'+"\n")
                    f.close()
                else:
                    alldeletsegamentid.append(id2)
                    f = open(id2+"delet.txt","w")
                    f.write(oneline[0]+"\t"+'2'+"\n")
                    f.close()
