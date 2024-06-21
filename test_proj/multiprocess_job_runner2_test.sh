#!/usr/bin/zsh
(python manage.py delete_jobs; 
sleep 1; 
python manage.py create_jobs 10000; 
sleep 1;)
for n in {1..10};
do
  python manage.py start_consumer_job_runner --num_jobs_to_consume=500& 
done
