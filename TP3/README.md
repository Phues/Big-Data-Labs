# README

# Big Data Lab 3

## Big Data Lab 

Steps:

1. Use the images from the previous lab.

3. Enter the master container to start using it:
    
    ```
    docker exec -it hadoop-master bash
    
    ```

   Start the Hadoop services:
    
    ```bash
    ./start-hadoop.sh
    
    ```

4. Add the `tp3.py` and `ngram.csv` to master image file.
    
5. Add the `ngram.csv` to HDFS:
    
   `hadoop fs -put arbres.csv`
    
6. Run the python program:
    
   `spark-submit --master spark://hadoop-master:7077 tp3.py out`
