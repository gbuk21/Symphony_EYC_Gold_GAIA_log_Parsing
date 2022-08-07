import sqlite3,os,time
def get_objects_retrieved(objects_retrieved_text):
    #print(objects_retrieved_text)
    time_strip=objects_retrieved_text.split()
    #print(time_strip[1])
    return time_strip[1]
def get_seconds(time_seconds):
    #print(time_seconds)
    #print('time_seconds')
    left_bracket=time_seconds.rindex('[')
    right_bracket=time_seconds.rindex(']')
    print(time_seconds[left_bracket+1:right_bracket])
    return time_seconds[left_bracket+1:right_bracket]

def find_LogId(LogIdText):
    #print(LogIdText)
    #print('LogIdText')
    left_bracket=LogIdText.index('[')
    right_bracket=LogIdText.index(']')
    print(LogIdText[left_bracket+1:right_bracket])
    return LogIdText[left_bracket+1:right_bracket]

def find_method_class(LogIdText, mode):
    #print(LogIdText)
    #print('LogIdText')
    if 'Methode Java = ' in LogIdText and mode ==1:
        return   LogIdText.replace('Methode Java = ','')
    elif 'Class Java = ' in LogIdText   and mode ==2:
        return   LogIdText.replace('Class Java = ','')
        
def check_file_already_present(filename, filesize,conn):
	if 1==1:
		cursor=conn.execute("select count(*) posts from logfile_header  where file_name ='"+filename+"' and filesize ='"+str(filesize)+"'")
		rows_list=cursor.fetchone()
		total_posts=rows_list[0]
		print(total_posts)
		if int(total_posts)>0:
			return 3
	
		else:
			cursor=conn.execute("select count(*) posts from logfile_header  where file_name ='"+filename+"'")
			rows_list=cursor.fetchone()
			total_posts=rows_list[0]
			if int(total_posts)>0:
				return 2
			else:
				total_posts=0
				return 1
    

def process_file(filename,hostname):
   print('filesize')
   try:
      print(os.stat(filename).st_size)
      filesize=os.stat(filename).st_size
   except:
      filesize=0
   conn = sqlite3.connect('gaia_errors.db',check_same_thread=False)
   file_existence_mode=check_file_already_present(filename,filesize,conn)
   if file_existence_mode == 3:
       time.sleep(30)
       return 0
   if file_existence_mode ==2:	
      query_del1="delete from logfile_query_time_output where file_name = '"+ str(filename)+"')"
      print(query_del1)
      cursor=conn.execute(query_del1)
      query_del1="delete from logfile_exceptions where file_name = '"+ str(filename)+"')"
      print(query_del1)
      cursor=conn.execute(query_del1)
      query_del1="delete from logfile_header where file_name = '"+ str(filename)+"')"
      print(query_del1)
      cursor=conn.execute(query_del1)

   query_string=''
   param_string_list=[]
   query_line=0
   param_line=0
   query_parameters=False
   do_insert=False
   query_exec_time=0
   objects_retrieved=0
   objects_retrieved_hold=-1
   query_exec_time_hold=-1
   param_string_list_hold=[]
   param_string_list_hold_string=''
   query_started=True
   param_list=[]
   c1 = conn.cursor()
   with open(filename,'r') as email:
    # read the file with a for loop
    for line in email:
        # strip the newline character from the line   

        if ' Methode' in line.strip():
           LogId=find_LogId(line.strip()[:46])
           method_name=find_method_class(line.strip()[46:], 1)
           continue
        if  ' Class' in line.strip():
           LogId=find_LogId(line.strip()[:46])
           class_name=find_method_class(line.strip()[46:],2)
           continue
        if '<queryArray>' in line.strip() or '<updateArray>' in line.strip():
           query_started=True
           query_exec_started=False
           query_line=query_line+1
           log_id=find_LogId(line.strip()[:46])           
        if '</queryArray> 'in line.strip() or '</updateArray>' in line.strip():
           query_exec_started=False
        if 'Query parameters'  in line.strip() or 'No parameters'  in line.strip():
           query_started=False
           query_parameters=True
           query_line=0
           param_line=param_line+1
        if ('Retrieving'  in line.strip() and 'Object'  in line.strip()) or ('Temps'  in line.strip() and 'execution'  in line.strip()):
           query_parameters=False
           param_line=0           
           if ('Temps'  in line.strip() and 'execution'  in line.strip()):
                #print(line.strip()[46:])
                query_exec_time=get_seconds(line.strip()[46:])
                query_exec_time=int(query_exec_time)
                #print('Query execution time - '+ str(query_exec_time))
                query_exec_time_hold=0
                do_insert=True
           if ('Retrieving'  in line.strip() and 'Object'  in line.strip()):
                #print(line.strip())
                objects_retrieved=get_objects_retrieved(line.strip()[46:])
                objects_retrieved=int(objects_retrieved)
                #print('objects retrieved - '+ str(objects_retrieved))
                objects_retrieved_hold=objects_retrieved
        #if query_started:=
           #print(line.strip())        
        if query_line >0:
           if 'LOGID' in line.strip():
               query_string=query_string+' ' + line.strip()[46:]
           else:
               query_string=query_string+' ' + line.strip()
           query_string=query_string.replace('<queryArray>','')
           query_string=query_string.replace('<updateArray>','')
        if param_line >0:
           #print(line.strip())
           if 'LOGID' in line.strip():
               param_temp_string=line.strip()[46:].replace('Query parameters','') 
               #param_string=param_string+' ' + param_temp_string
               if param_temp_string!='':
                  param_string_list.append(param_temp_string)
           else:
               param_temp_string=line.strip().replace('Query parameters','') 
               if param_temp_string!='':
                  param_string_list.append(param_temp_string)
                  param_string_list_hold=param_string_list
        if not query_started and not query_string=='':
           #print(query_string+'\n')
           query_string_hold=query_string
           query_string=''
           
        if param_line>=0 and  not query_parameters: 
           if len(param_string_list)>0:
              print(param_string_list)
           param_string_list=[]
           param_line=0
        #print(param_string_list)
        if do_insert:
           query_string_hold=query_string_hold.replace("'","''")
           for i in param_string_list_hold:
               param_string_list_hold_string=param_string_list_hold_string+'~'+i.replace("'","''")
           query_ins="insert into logfile_query_time_output(file_name,originial_query,params,time,return_count,logid, method_name, class_name) values('"+filename+"','"+str(query_string_hold)+"','"+str(param_string_list_hold_string)+"',"+str(query_exec_time_hold)+","+str(objects_retrieved_hold)+",'"+str(LogId)+"','"+str(method_name)+"','"+str(class_name)+"')"
           print(query_ins)
           c1.execute(query_ins)
           query_string_hold=''
           objects_retrieved_hold=0
           query_exec_time_hold=0
           param_string_list_hold=[]
           param_string_list_hold_string=''
           do_insert=False
        if ('EXCEPTION'  in line.strip().upper() ):   
            query_exception="insert into logfile_exceptions(file_name,exception_text,LogId) values('"+str(filename)+"','"+str(line.strip())+"','"+str(LogId)+"')"
            print(query_exception)
            c1.execute(query_exception)
        #conn.commit()
   if not do_insert and len(query_string_hold)>0:
           query_string_hold=query_string_hold.replace("'","''")
           for i in param_string_list_hold:
               param_string_list_hold_string=param_string_list_hold_string+'~'+i.replace("'","''")
           query_ins="insert into logfile_query_time_output(file_name,originial_query,params,time,return_count,logid, method_name, class_name) values('"+filename+"','"+str(query_string_hold)+"','"+str(param_string_list_hold_string)+"',"+str(query_exec_time_hold)+","+str(objects_retrieved_hold)+",'"+str(LogId)+"','"+str(method_name)+"','"+str(class_name)+"')"
           print(query_ins)
           c1.execute(query_ins)
           query_string_hold=''
           objects_retrieved_hold=-1
           query_exec_time_hold=-1
           param_string_list_hold=[]
           param_string_list_hold_string=''
           do_insert=False
   query_ins="insert into logfile_header(file_name,filesize,hostname) values('"+filename+"','"+str(filesize)+"','"+str(hostname)+"')"
   print(query_ins)
   c1.execute(query_ins)
   conn.commit()

import datetime, os, glob
hostname=os.uname()[1]
x = datetime.datetime.now()


dir='' # change to gaia logs directory.

for i in glob.glob(dir+"*.log"):
   query_string=''
   param_string_list=[]
   query_line=0
   param_line=0
   query_parameters=False
   do_insert=False
   query_exec_time=0
   objects_retrieved=0
   objects_retrieved_hold=0
   query_exec_time_hold=0
   param_string_list_hold=[]
   param_string_list_hold_string=''
   query_started=True
   param_list=[]
   process_file(i,hostname)                

