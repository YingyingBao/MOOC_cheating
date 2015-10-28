import pandas as pd
import csv
from datetime import datetime
import gc

#import original data
event_headers=['index','usrid','usrip','event_type','proid','timestamp']
event_logs=pd.read_csv('submissions.csv',header=None,names=event_headers)
event_logs=event_logs[['usrid','usrip','event_type','proid','timestamp']]
event_logs.to_csv('event_logs.csv')

usr_headers=['usrid','usrstate']
usr=pd.read_csv('course_user.csv',header=None,names=usr_headers)

#select active user
av_usr=pd.unique(event_logs.usrid)
usr_list=usr.loc[usr['usrid'].isin(av_usr)]
usr_list.to_csv('Active_Usr.csv')

#Filter 1
passing_usr=usr_list.groupby(['usrstate']).get_group('downloadable')
notpass_usr=usr_list.groupby(['usrstate']).get_group('notpassing')
passing_usr.to_csv('passing_usr.csv')
notpass_usr.to_csv('notpass_usr.csv')

graded_event=event_logs.groupby(['event_type']).get_group('problem_graded')
show_event=event_logs.groupby(['event_type']).get_group('showanswer')
graded_event.to_csv('graded_event.csv')
show_event.to_csv('show_event.csv')

graded_usr=pd.unique(graded_event.usrid)
CM_usr=list(set(graded_usr)&set(passing_usr.usrid))
with open('CM_usr.csv','a') as CM_file:
	CM_usr_wr=csv.writer(CM_file)
	for element in CM_usr:
		CM_usr_wr.writerow([element])
CM_file.close()

show_usr=pd.unique(show_event.usrid)
CH_usr=list(set(show_usr)&set(notpass_usr.usrid))
with open('CH_usr.csv','a') as CH_file:
	CH_usr_wr=csv.writer(CH_file)
	for element in CH_usr:
		CH_usr_wr.writerow([element])
CH_file.close()

#calculate CM_vector
CM_usr=pd.read_csv('CM_usr.csv',header=None,names=['usrid'])
problem_headers=['proid','pro_type']
problems=pd.read_csv('problems.csv',header=None,names=problem_headers)
problem_id=problems.proid
Usr_vector_columns=pd.Series(['usrid']).append(problem_id)
CM_vector_df=pd.DataFrame(columns=Usr_vector_columns)

graded_event_gp=graded_event.groupby(['usrid'])
def func_CM_vector(element):
	tempList=list()
	tempdf=graded_event_gp.get_group(element.usrid)
	tempList.append(element.usrid)
	tempUsr_pro=tempdf.proid
	tempUsr_time=tempdf.timestamp
	for i in range(0,len(problem_id)):
		flag=0
		for j in range(0,len(tempUsr_pro)):
			if tempUsr_pro.values[j]==problem_id.values[i]:
				tempList.append(tempUsr_time.values[j])
				flag=1
				break
		if flag==0:
			tempList.append('None')
	CM_vector_df.loc[len(CM_vector_df)+1]=tempList

temp=CM_usr.apply(func_CM_vector,axis=1)
CM_vector_df.to_csv('CM_vector.csv')

#calculate CH_vector
CH_usr=pd.read_csv('CH_usr.csv',header=None,names=['usrid'])
show_event_gp=show_event.groupby(['usrid'])
CH_vector_df=pd.DataFrame(Usr_vector_columns)
def func_CH_vector(element):
	tempList=list()
	tempdf=show_event_gp.get_group(element.usrid)
	tempList.append(element.usrid)
	tempUsr_pro=tempdf.proid
	tempUsr_time=tempdf.timestamp
	for i in range(0,len(problem_id)):
		flag=0
		for j in range(0,len(tempUsr_pro)):
			if tempUsr_pro.values[j]==problem_id.values[i]:
				tempList.append(tempUsr_time.values[j])
				flag=1
				break
		if flag==0:
			tempList.append('None')
	CH_vector_df.loc[len(CH_vector_df)+1]=tempList

temp=CH_usr.apply(func_CH_vector,axis=1)
CH_vector_df.to_csv('CH_vector.csv')

del tempList
del tempdf
gc.collect()

#calculate CM-CH pairs time delta
date_format="%Y-%m-%dT%H:%M:%S.%f+00:00"
def func_time_delta_CH(CHrow,CMrow):
	flag=0
	tempList=list()
	tempList.append(CMrow[0])
	tempList.append(CHrow[0])
	for i in range(1,len(CHrow)):
		if CHrow[i]=='None' or CMrow[i]=='None':
			tempList.append('None')
		else:
			time_CH=datetime.strptime(CHrow[i],date_format)
			time_CM=datetime.strptime(CMrow[i],date_format)
			time_delta=(time_CH-time_CM).total_seconds()
			tempList.append(time_delta)
			flag=1
	if flag==1:
		with open('time_delta.csv','a') as time_file:
			time_delta_wr=csv.writer(time_file)
			time_delta_wr.writerow(tempList)
		time_file.close()
def func_time_delta_CM(CMrow):
	CH_vector_df.apply(lambda x: func_time_delta_CH(x,CMrow),axis=1)

temp=CM_vector_df.apply(func_time_delta_CM,axis=1)

#calculate maximum posterior estimation
def func_n_x(time_delta):
	tempList=list()
	tempN=0
	tempX=0
	tempList.append(time_delta[0])
	tempList.append(time_delta[1])
	for i in range(2,len(time_delta)):
		if time_delta[i]!='None':
			tempList.append(time_delta[i])
			tempN=tempN+1
			if float(time_delta[i])>0:
				tempX=tempX+1
	tempList.append(tempX)
	tempList.append(tempN)
	if tempN==1:
		if tempX==1:
			tempList.append('1')
			xn_wr.writerow(tempList)
	else:
		temppi=(tempX-0.5)/(tempN-1)
		if temppi>0.9:
			if temppi>1:
				tempList.append('1')
			else:
				tempList.append(temppi)
			xn_wr.writerow(tempList)

def process(chunk):
	temp=chunk.apply(func_n_x,axis=1)

with open('xnp_filter.csv','a') as xn_file:
	xn_wr=csv.writer(xn_file)
	chunksize=500
	for chunk in pd.read_csv('time_delta.csv',header=None,chunksize=chunksize):
		process(chunk)
	xn_file.close()

#set cutoff threshold
with open('xnp_filter.csv','r') as xnp_file:
	xnp_file_reader=csv.reader(xnp_file)
	for line in xnp_file_reader:
		tempcount=0
		for i in range(2,len(line)-3):
			if float(line[i])<300:
				tempcount=tempcount+1
		temppos=tempcount/float(line[-2])
		if temppos>0.9:
			line.append(temppos)
			Pos_file=open('Pos_filter.csv','a')
			pos_wr=csv.writer(Pos_file)
			pos_wr.writerow(line)
			Pos_file.close()
xnp_file.close()

#detect shared IP address
UsrSet=set()
with open('Pos_filter.csv','r') as pos_file:
	pos_file_reader=csv.reader(pos_file)
	for row in pos_file_reader:
		UsrSet.add(row[0])
		UsrSet.add(row[1])
pos_file.close()

IP_logs=event_logs[['usrid','usrip']]
UsrSetNotEmpty=1
while UsrSetNotEmpty:
    UsrList=list(UsrSet)
    temp_usr_list=list()
    temp_usr_list.append(UsrList[0])
    for usr in temp_usr_list:
        IP_gp=IP_logs.groupby(['usrid']).get_group(usr)
        IP_gp=pd.unique(IP_gp.usrip)
        for ip in IP_gp:
            temp_usr_gp=IP_logs.groupby(['usrip']).get_group(ip)
            temp_usr_gp=pd.unique(temp_usr_gp.usrid)
            temp_usr_gp=list(temp_usr_gp)
            if len(temp_usr_gp)!=1:
                for temp_usr in temp_usr_gp:
                    if temp_usr not in temp_usr_list:
                        temp_usr_list.append(temp_usr)
    IP_file=open('IP_file.csv','a')
    IP_wr=csv.writer(IP_file)
    IP_wr.writerow(temp_usr_list)
    UsrSet=UsrSet-set(temp_usr_list)
    if len(UsrSet)==0:
        UsrSetNotEmpty=0
IP_file.close()

with open('Pos_filter.csv','r') as Pos_file:
    Pos_file_reader=csv.reader(Pos_file)
    for Pos_line in Pos_file_reader:
        with open('IP_file.csv','r') as IP_file:
            IP_file_reader=csv.reader(IP_file)
            for IP_line in IP_file_reader:
                if Pos_line[0] in IP_line and Pos_line[1] in IP_line:
                    final_file=open('final.csv','a')
                    final_wr=csv.writer(final_file)
                    final_wr.writerow(Pos_line)
final_file.close()
Pos_file.close()
IP_file.close()


	
	

