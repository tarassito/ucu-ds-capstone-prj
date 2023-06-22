***Prerequisite***:
 - docker must be installed

**How to run:**  
1. Run kafka cluster: `docker compose -f init-kafka-cluster.yaml up` Cluster is created 
with 3 nodes and one 8partition topic. Check if all containers is running. If some of containers stopped
run it manually with `docker run CONTAINER_ID`

2. Run statistic microservice `docker compose -f run-statistics.yaml up`

3. Run other microservices `docker compose -f run-microservices.yaml up`

4. To check statistics open web browser use this URLs:
    - for language statistic - http://localhost:6066/languages
    - for sentiment -  http://localhost:6066/sentiments
    - for top10 user - http://localhost:6066/users
    
5. To finish the work - `docker compose -f run-microservices.yaml -f run-statistics.yaml -f init-kafka-cluster.yaml down`