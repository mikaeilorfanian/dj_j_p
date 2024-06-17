#!/usr/bin/zsh
(python manage.py delete_jobs; 
sleep 2; 
python manage.py create_jobs 1000; 
sleep 2); 
for n in {1..10};
do
  python manage.py start_consumer& 
done
