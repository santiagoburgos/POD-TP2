# ITBA - POD TPE 2 - Grupo 6

## Compilación
1. Utilizando el comando **cd** situarse en el directorio del proyecto **tpe2-g6**.
2. (Opcional) cambiar el valor de ip deseado para el server en **tpe2-g6/server/src/main/resources/config.properties**
   ```
   ip=192.168.1.*
   ```
3. Ejecutar el comando **mvn clean install**.

## Ejecución
1. Una vez compilado situarse en las carpetas target de *client* y *server*.
2. Con el comando **tar -xzf** descomprimir los archivos: *tpe2-g6-client-1.0-SNAPSHOT-bin.tar* y *tpe2-g6-server-1.0-SNAPSHOT-bin.tar*.
   Situados en sus respectivas carpetas.
     ```
    tar -xzf ./client/target/tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz
    tar -xzf ./server/target/tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz
    ```
3. Ejecutar el comando **chmod u+x** sobre los scripts *queryX.sh* y *run-server.sh* para otorgarles permiso de ejecuccion.

    ```
    chmod u+x ./tpe2-g6-server-1.0-SNAPSHOT/run-server.sh 
    chmod u+x ./tpe2-g6-client-1.0-SNAPSHOT/query1.sh
    chmod u+x ./tpe2-g6-client-1.0-SNAPSHOT/query2.sh
    chmod u+x ./tpe2-g6-client-1.0-SNAPSHOT/query3.sh
    chmod u+x ./tpe2-g6-client-1.0-SNAPSHOT/query4.sh
    chmod u+x ./tpe2-g6-client-1.0-SNAPSHOT/query5.sh
    ```

   ### Levantar el servidor
    - Correr el script **run-server.sh**.
    ```
    cd ./tpe2-g6-server-1.0-SNAPSHOT
    ./run-server.sh
    ``` 
   ### Correr Queries
   #### Query 1: Total de arboles por barrio
   - Parametros:

   **-Dcity** indica con qué dataset de ciudad se desea trabajar. Los únicos valores posibles son BUE y VAN.

   **-Daddresses**  refiere a las direcciones IP de los nodos con sus puertos (una o más, separadas por punto y coma).

   **-DinPath** indica el path donde están los archivos de entrada de barrios y de árboles.

   **-DoutPath** indica el path donde estarán ambos archivos de salida query1.csv y time1.txt.
    ```
    ./query1.sh -DCity=BUE -Daddresses='10.6.0.1:5701' -DinPath=. -DoutPath=.
    ```
   #### Query 2: Para cada barrio, la especie con mayor cantidad de árboles por habitante
    - Parametros:

   **-Dcity** indica con qué dataset de ciudad se desea trabajar. Los únicos valores posibles son BUE y VAN.

   **-Daddresses**  refiere a las direcciones IP de los nodos con sus puertos (una o más, separadas por punto y coma).

   **-DinPath** indica el path donde están los archivos de entrada de barrios y de árboles.

   **-DoutPath** indica el path donde estarán ambos archivos de salida query1.csv y time1.txt.
    ```
    ./query2.sh -DCity=BUE -Daddresses='10.6.0.1:5701' -DinPath=. -DoutPath=.
    ```
   #### Query 3: Top n barrios con mayor cantidad de especies distintas
    - Parametros:

   **-Dcity** indica con qué dataset de ciudad se desea trabajar. Los únicos valores posibles son BUE y VAN.

   **-Daddresses**  refiere a las direcciones IP de los nodos con sus puertos (una o más, separadas por punto y coma).

   **-DinPath** indica el path donde están los archivos de entrada de barrios y de árboles.

   **-DoutPath** indica el path donde estarán ambos archivos de salida query1.csv y time1.txt.

   **-Dn** indica la cantidad maxima de barrios en la salida.
    ```
    ./query3.sh -DCity=BUE -Daddresses='10.6.0.1:5701' -DinPath=. -DoutPath=. -Dn=5
    ```
   #### Query 4: Pares de barrios que registran la misma cantidad de cientos de especies distintas
    - Parametros:

   **-Dcity** indica con qué dataset de ciudad se desea trabajar. Los únicos valores posibles son BUE y VAN.

   **-Daddresses**  refiere a las direcciones IP de los nodos con sus puertos (una o más, separadas por punto y coma).

   **-DinPath** indica el path donde están los archivos de entrada de barrios y de árboles.

   **-DoutPath** indica el path donde estarán ambos archivos de salida query1.csv y time1.txt.
    ```
    ./query4.sh -DCity=BUE -Daddresses='10.6.0.1:5701' -DinPath=. -DoutPath=.
    ```
   #### Query 5: Pares de calles de un barrio X que registran la misma cantidad de decenas de árboles de una especie Y
    - Parametros:

   **-Dcity** indica con qué dataset de ciudad se desea trabajar. Los únicos valores posibles son BUE y VAN.

   **-Daddresses**  refiere a las direcciones IP de los nodos con sus puertos (una o más, separadas por punto y coma).

   **-DinPath** indica el path donde están los archivos de entrada de barrios y de árboles.

   **-DoutPath** indica el path donde estarán ambos archivos de salida query1.csv y time1.txt.

   **-Dneighbourhood** indica el barrio X al que deberan pertenecer los árboles.

   **-DcommonName** indica la especie Y que los árboles deben ser.
    ```
    ./query5.sh -DCity=BUE -Daddresses='10.6.0.1:5701' -DinPath=. -DoutPath=. -Dneighbourhood='KITSILANO -DcommonName='NORWAY_MAPLE
    ```
    
    
    
    

