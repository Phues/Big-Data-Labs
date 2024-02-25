# README

# Big Data Lab 2

## Big Data Lab 2

To set up the Big Data Lab environment, follow these steps:

1. Use the images from the previous lab.
2. Add the slaves `spark-slave1` and
`spark-slave2` at `'cd /usr/local/spark/conf'`
(can be done using `vim slaves`)
3. Enter the master container to start using it:
    
    ```
    docker exec -it hadoop-master bash
    
    ```
    
    To start the Spark services, run the following command in the shell
    of the master container:
    
    ```bash
    cd /usr/local/spark/sbin/
    ./start-all.sh
    ```
    
    To be able to run Python programs, create the `spark-env.sh` file:
    
    ```bash
    cd /usr/local/spark/conf
    cp spark-env.sh.template spark-env.sh
    
    ```
    
    and add the following line:
    
    ```bash
    PYSPARK_PYTHON=/usr/bin/python3
    
    ```
    
    Add the `count _lines.py` and `arbres.csv` to master image file.
    
    Add the `arbres.cs` to HDFS so it can be treated by slave nodes
    
    `hadoop fs -put arbres.csv`
    
    Run the python program:
    
    `spark-submit --master spark://hadoop-master:7077 count_lines.py`
    

Result:

![https://github.com/Phues/Big-Data-Labs/blob/main/TP1/image.png](https://github.com/Phues/Big-Data-Labs/blob/main/TP1/image.png)