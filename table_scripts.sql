-- Create following DB tables to store captured text.
-- Table to keep track of each log file.
create table logfile_header(file_name text,filesize text,hostname text);

-- Table to keep track of exceptions happened in a log file
Create table logfile_exceptions(file_name text,exception_text text,LogId text);

-- table to keep track of query return count and the elapsed time
Create table logfile_query_time_output(file_name text,originial_query text,params text,time text,return_count text,logid text, method_name text, class_name text);

