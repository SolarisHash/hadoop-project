# hadoop-project

# Prérequis
-Docker  
-Hadoop => https://hadoop.apache.org/releases.html)  
-JDK 11  
-La base de donnée => https://www.kaggle.com/datasets/mohammedalsubaie/movies  

# Installation et Configuration  
1. Cloner le dépôt :  
   git clone https://github.com/SolarisHash/hadoop-project.git  
   cd movie  

2. Télécharger l'image docker uploadée sur dockerhub:  
docker pull liliasfaxi/hadoop-cluster:latest  

3. Créer un réseau qui permettra de relier les trois contenaires:  
docker network create --driver=bridge hadoop

4. Créer et lancer les trois contenaires (les instructions -p permettent de faire un mapping entre les ports de la machine hôte et ceux du contenaire):  
docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest  
docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest  
docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest  

5. Vérifier que les trois contenaires tournent bien en lançant la commande 'docker ps'
6. Aller dans l'Explorer, sous Maven, puis ouvrir le Lifecycle du projet movie
7. Cliquer sur 'package' pour compiler et packager le projet dans un fichier JAR. Un fichier movie-1.0-SNAPSHOT-jar-with-dependencies.jar sera créé sous le répertoire target du projet.
8. Copier le fichier jar créé dans le contenaire master. Pour cela:  
    -Ouvrir le terminal sur le répertoire du projet wordcount. Cela peut être fait avec VSCode en allant au menu Terminal -> New Terminal.  
    -Taper la commande suivante:  
      docker cp target/movie-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-master:/root/hadoop-project.jar

# Lancer le programme  

1. Entrer dans le contenaire master pour commencer à l'utiliser.  
docker exec -it hadoop-master bash  

2. Lancer hadoop et yarn  
./start-hadoop.sh

3. Lancer HBase
start-hbase.sh  

4. Créer un répertoire dans HDFS, appelé input.  
hdfs dfs -mkdir -p input  

7. Lancer le job map reduce  
hadoop jar hadoop-project.jar hadoop.movie.tp.MovieReleaseCount /user/root/input /user/root/output

8. Lancer ce script pour manipuler les données
hbase shell

9. Verifier que la table est bien créer
list

10. Visualiser le résultat de l'insertion, en tapant
scan 'movie_release_counts'  

