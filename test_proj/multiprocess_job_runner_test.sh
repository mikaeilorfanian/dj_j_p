#!/usr/bin/zsh
(python manage.py delete_jobs; sleep 2; python manage.py create_jobs; sleep 2; 
python manage.py start_consumer; 
python manage.py start_consumer; 
python manage.py start_consumer; 
python manage.py start_consumer; 
python manage.py start_consumer; 
python manage.py start_consumer;
python manage.py start_consumer; 
python manage.py start_consumer; 
python manage.py start_consumer; 
python manage.py start_consumer;)&
