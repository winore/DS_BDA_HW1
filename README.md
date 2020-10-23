# HW1
---

## Лабораторная работа 1 

screenshots - скриншоты работы программы 	<br>
src - исходники					<br>
scripts - скрипт для генерации данных		<br>
pom.xml - конфигурационный файл для сборки	<br>
TestData - сгенерированные данные		<br>

## Сборка

1. Переходим в папку проекта `cd /ds_bda_hw1`
2. Запускаем `mvn install`, если не __maven__ не установлен, устанавливаем `yum install maven`
Итоговый __jar__ файл будет расположен в директории __target__

## Запуск
1. Необходимо запустить namenode из директории где установлен hadoop:
   ```console
    $ cd /opt/hadoop-2.10.0
	$ sbin/start-dfs.sh
    ```  
2. Необходимо запустить yarn из директории где установлен hadoop:
   ```console
    $ cd /opt/hadoop-2.10.0
	$ sbin/start-yarn.sh
    ```  
3. Генерация данных с помощью готового скрипта:
   ```console
    $ cd /ds_bda_hw1/scripts/TestData
	$ ./generateInput.sh
    ```  
4. Необходимо загрузить в целевую директорию hdfs входные данные.
   ```console
    $ hdfs dfs -mkdir input
	$ hdfs dfs -put ./Test*.txt ./input
    ```  
5. Для запуска непосредственно приложения выполнить команду: 
   ```console
    $ cd /target
	$ yarn jar ./hw1-1.0.jar BrowserUsers input output
    ```  
## Вывод данных

   ```console
    $ hdfs dfs -cat output/part-r-00000
    ```  



