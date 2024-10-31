# hadoop-project

## Prerequisites
- Docker  
- [Hadoop](https://hadoop.apache.org/releases.html)  
- JDK 11  
- [Dataset](https://www.kaggle.com/datasets/mohammedalsubaie/movies)  

## Installation and Configuration  
1. Clone the repository:
   ```bash
   git clone https://github.com/SolarisHash/hadoop-project.git  
   cd movie
   ```

2. Create an `input` directory inside the `resources` directory and place the previously downloaded CSV file there.

3. Download the Docker image uploaded on DockerHub:
   ```bash
   docker pull liliasfaxi/hadoop-cluster:latest  
   ```

4. Create a network that will link the three containers:
   ```bash
   docker network create --driver=bridge hadoop
   ```

5. Create and launch the three containers (the `-p` options allow mapping between the host machine's ports and those of the container):
   ```bash
   docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest  
   docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest  
   docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest  
   ```

6. Verify that the three containers are running properly by running:
   ```bash
   docker ps
   ```

7. In your IDE's Explorer, under Maven, open the Lifecycle of the `movie` project.

8. Click on `package` to compile and package the project into a JAR file. A file named `movie-1.0-SNAPSHOT-jar-with-dependencies.jar` will be created under the project's `target` directory.

9. Copy the created JAR file into the master container. To do this:
    - Open the terminal in the project directory (you can do this in VSCode by going to Terminal -> New Terminal).
    - Type the following commands:
      ```bash
      cd movie/  
      docker cp target/movie-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-master:/root/hadoop-project.jar
      ```

## Run the Program  

1. Enter the master container to start using it:
   ```bash
   docker exec -it hadoop-master bash  
   ```

2. Start Hadoop and YARN:
   ```bash
   ./start-hadoop.sh
   ```

3. Start HBase:
   ```bash
   start-hbase.sh  
   ```

4. Create a directory in HDFS called `input`:
   ```bash
   hdfs dfs -mkdir -p input
   ```

5. Move the CSV file into the `input` directory:
   ```bash
   hdfs dfs -put movie.csv input/  
   ```

6. Run the MapReduce job:
   ```bash
   hadoop jar hadoop-project.jar input output
   ```

7. Run this script to manipulate the data:
   ```bash
   hbase shell
   ```

8. Verify that the table has been created:
   ```bash
   list
   ```

9. View the result of the insertion by typing:
   ```bash
   scan 'movie_release_counts'  
   ```
