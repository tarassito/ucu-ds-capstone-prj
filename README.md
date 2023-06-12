***Prerequisite***:
 - docker must be installed
 - virtual env must be installed (`pip install kafka-python==2.0.2 pandas==2.0.1`)

**How to run:**  
1. Run kafka cluster: `docker compose -f init-kafka-cluster.yaml up` Cluster is created 
with 3 nodes and 2 topics (5 and 10 partitions topics).

2. To generate data from `chatgpt1.csv` dataset to 
kafka topic:  
    - go inside generation folder - `cd generation`
    - run `python app.py`  
    
3. Data should be in `5part-topic`. To check this use
command - `docker exec --interactive --tty kafka1 kafka-console-consumer --bootstrap-server localhost:9092 --topic 5part-topic --from-beginning
`