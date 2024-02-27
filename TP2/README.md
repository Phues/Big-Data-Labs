# README

# Big Data Lab 2

## Big Data Lab 2

Steps:

1. Use the images from the previous lab.
2. Add the slaves `spark-slave1` and
`spark-slave2` at `cd /usr/local/spark/conf`
(can be done using `vim slaves`)
3. Add the following line to `spark-env.sh` located at `/usr/local/spark/conf` to can execute python files:
    ```
    PYSPARK_PYTHON=/usr/bin/python3

    ```
4. Enter the master container to start using it:
    
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
    
    Add the `<program_name>.py` and `arbres.csv` to master image file.
    
    Add the `arbres.csv` to HDFS so it can be treated by slave nodes
    
    `hadoop fs -put arbres.csv`
    
    To run the python program in cluser mode:
    
    `spark-submit --master spark://hadoop-master:7077 <program_name>.py --output > /root/<file_name>`

    To run the python program in local mode:

    `spark-submit --master local counts_lines_local.py --output > /root/<file_name>`
