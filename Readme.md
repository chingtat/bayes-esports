# Challenge for Bayes Esports

- init.py creates 2 queues and fills them with data
- worker.py consume features from one of 2 queues and make predictions
- worker.py takes master queue name from config file ./config/config.ini
- worker.py checks config file in separate thread for given period (from command line)
- run app from run.bash (need docker-compose)
- see logs in ./logs/worker.log
- change queue in ./config/config.ini (queue_in_1 or queue_in_2) 
- worker restarts when fails with exception (errors look at logs)