import random
from flask import Flask
import datetime
import requests
import json
from itertools import cycle
import sys

class Consumer:
    def init(self, address, count_of_retries, time_of_statistics):
        self.address = address
        self.count_of_retries = count_of_retries
        self.time_of_statistics = time_of_statistics

    """def read(self, qname):
        f = open("statistic.json", "r")
        stat = {}
        #line = f.readline().strip('\n')
        # while line.split(' ')[0] != 'date':
        #     node = line
        #     stat[node] = dict()
        #     line = f.readline().strip('\n')
        #     line = f.readline().strip('\n')
        #     while line != '}':
        #         (key, val) = line.split()
        #         stat[node][key] = val
        #         line = f.readline().strip('\n')
        #     line = f.readline().strip('\n')
        #
        # data = line.split(' ')[1].strip('\n')

        if data > self.time_of_statistics:
            self.get_statistic()
        type = requests.get("http://" + (self.address) + ":5000/get_type/")

        node = max(stat, key=lambda x: stat[x][type])  # найзавантаженіший нод

        port = stat[node]['port']

        message = requests.post("http://127.0.0.1:" + str(port.content) + "/read_message", json={"qname": qname})
        return message.content"""
    """def read(self):
        stat = requests.get('http://127.0.0.1//statisitcs/')
        ms = json.loads(stat)
        type_of_balance = requests.get('http://127.0.0.1:/....???????')
        node = ''
        maxval = 0
        req = requests.get('http://127.0.0.1//get_queue')
        name_of_q = json.loads(req.content)
        
        if type_of_balance == 'count':
            for i,item in ms:
                curr = item['write_message_duration']
                if curr>maxval:
                    maxval = curr
                    node = i


        port=requests.post('http://127.0.0.1//stati')
        if type_of_balance == 'sth':
            for keys in ms.keys():
                list+=ms[keys]['write_message_duration']-ms[keys]['read_message_duration']
            max_memory=max(list)
            name=
            for value in ms.items():
                if max_memory == value:
                     ms = requests.get('http://127.0.0.1//statisitcs/')
                     break
        else:
            max_count = max(ms['msg_num_in_qname'])
            for value in ms.items():
                if max_count == value:
                    ms = requests.get('http://127.0.0.1//statistics/')
                    break
        return  node"""

    def get_statistic(self):
        statistic = 0
        time = datetime.date.today()
        while True:
            for i in range(self.count_of_retries):
                statistic = requests.get("http://" + (self.address) + ":5000/get_statistic/")
                # json.dump(statistic.content,f)
                # json.dump(time,f)
                if statistic.status_code == 200:
                    break
            if statistic.content['time'] < self.time_of_statistics:
                requests.post("http://" + (self.address) + ":5000/update_statisctic/")
            else:
                break
        return statistic.content, time

Марта Костецька🌼, [18.12.19 09:52]
def read(self,qname):
        stat,port =0,0
        stat,_= self.get_statistic()
        # a = True
        # for i in range(self.count_of_retries):
        #     stat = requests.get("http://" + (self.address) + ":5000/get_statistic/")
        #     if stat.status_code == 200:
        #         a = False
        #         break
        # if a:
        #     raise Exception
        a = True
        qn = json.loads(stat.content)
        key = qn.keys()
        maxelem = 0
        nd = ''
        for node,st in qn:
            if 'msg_num_in_{}'.format(qname) in st and int(st['msg_num_in_{}'.format(qname)]) > maxelem:
                maxelem = int(st['msg_num_in_{}'.format(qname)])
                nd = node
        for i in range(self.count_of_retries):
            port = requests.post("http://" + (self.address) + ":5000/get_port/", json={'qname': nd})
            if port.status_code == 200:
                a = False
                break
        if a:
            raise Exception
        a = True
        for i in range(self.count_of_retries):
            ms = requests.post("http://" + port.content + ":5000/read_message/", json = {'qname': qname})
            if stat.status_code == 200:
                a = False
                break
        if a:
            raise Exception
        return ms.content

    def createq(self, qname):
        for i in range(self.count_of_retries):
            result = requests.post("http://" + (self.address) + ":5000/create_queue/", json={"qname": qname})
            if result.status_code == 200:
                break

    def deleteq(self, qname):
        for i in range(self.count_of_retries):
            result = requests.post("http://127.0.0.1:5000/delete_queue/", json={"qname": qname})
            if result.status_code == 200:
                break

    def createdatanode(self):
        for i in range(self.count_of_retries):
            result = requests.post("http://" + (self.address) + ":5000/create_data_node/")
            if result.status_code == 200:
                break
"""Декодуэмо наше повідомлення за функцією"""
def triple_decoding(strng,n):
    keytext = ""
    coding_word = "drebotiy"
    if len(keytext) >= n:
        keytext = coding_word
    else:
        while True:
            if len(keytext) < n:
                keytext = keytext + coding_word
                continue
            elif len(keytext) >= n:
                break

    alp = 'abcdefghijklmnopqrstuvwxyz.,@/ABCDEFGHIJKLMNOPQRSTUVWXYZ~`!#$%^&*() _+-=:.<>?/1234567890'
    f = lambda arg: alp[alp.index(arg[0]) - alp.index(arg[1]) % 88]
    strng=''.join(map(f, zip(strng, cycle(keytext))))

    def rail_pattern(n):
        r = list(range(n))
        return cycle(r + r[-2:0:-1])
    p = rail_pattern(n)
    indexes = sorted(range(len(strng)), key=lambda i: next(p))
    result = [''] * len(strng)
    for i, c in zip(indexes, strng):
        result[i] = c
    strng=''.join(result)
    strng = strng[10:]
    count=-1
    lst=[]
    for i in strng:
        count+=1
        if i == " ":
            lst.append(count)
    for k in range(n):
        strng=strng.split(" ")
        for i in strng:
            if i=="":
                strng.remove(i)
        for i in range(len(strng)):
            for j in range(n):
                strng[i] = strng[i][1:]+strng[i][0]
        result=""
        for i in strng:
            result+=i
        strng=result
        strng = strng[n:] + strng[0:n]
        result=""
        for i in range(len(lst)):
            strng = strng[:lst[i]] +" "+strng[lst[i]:]
    return strng

#message - повідомлення що ви отримаєте
#якщо ви отримаєте повідомлення в виді кортежу (а і б) тобто зашифроване повідомлення і ключ(або навпаки) то напишемо так
# String=message[0] чи message[1] key=message[1] чи message[0]
#
#result=triple_decoding(String,key)
"""Отримаэмо розкодоване повідомлення"""
