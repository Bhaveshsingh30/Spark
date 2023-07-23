#Copy data to Hdfs 

abc@e21361e5939e:~/workspace$ hadoop fs -mkdir /spark

abc@e21361e5939e:~/workspace$ hadoop fs -ls /
Found 1 items
drwxr-xr-x   - abc supergroup          0 2023-07-23 12:01 /spark

abc@e21361e5939e:~/workspace$ hadoop fs -put Sample2000.csv /spark
2023-07-23 12:03:01,063 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false


abc@e21361e5939e:~/workspace$ hadoop fs -ls /spark
Found 1 items
-rw-r--r--   1 abc supergroup     143295 2023-07-23 12:03 /spark/Sample2000.csv

abc@e21361e5939e:~/workspace$ 


#Reading from Hdfs


>>> from pyspark.sql import SparkSession
>>> df1 = spark.read.option("header",True).option("delimiter",",").csv("/spark/Sample2000.csv")
>>> df1.show()                                                                  
+-------------+--------------------+---------------+--------------------+-----+
|Serial Number|        Company Name|Employee Markme|         Description|Leave|
+-------------+--------------------+---------------+--------------------+-----+
|9788189999599|      TALES OF SHIVA|           Mark|                mark|    0|
|9780099578079|1Q84 THE COMPLETE...|HARUKI MURAKAMI|                Mark|    0|
|9780198082897|            MY KUMAN|           Mark|                Mark|    0|
|9780007880331|THE GOD OF SMAAL ...|  ARUNDHATI ROY|  4TH HARPER COLLINS|    2|
|9780545060455|    THE BLACK CIRCLE|           Mark|  4TH HARPER COLLINS|    0|
|9788126525072|THE THREE LAWS OF...|           Mark|  4TH HARPER COLLINS|    0|
|9789381626610|   CHAMarkKYA MANTRA|           Mark|  4TH HARPER COLLINS|    0|
|9788184513523|            59.FLAGS|           Mark|  4TH HARPER COLLINS|    0|
|9780743234801|THE POWER OF POSI...|           Mark|     A & A PUBLISHER|    0|
|9789381529621|YOU CAN IF YO THI...|          PEALE|     A & A PUBLISHER|    0|
|9788183223966|DONGRI SE DUBAI T...|           Mark|     A & A PUBLISHER|    0|
|9788187776005|MarkLANDA ADYTAN ...|           Mark|   AADISH BOOK DEPOT|    0|
|9788187776013|MarkLANDA VISHAL ...|              -|   AADISH BOOK DEPOT|    1|
|   8187776021|MarkLANDA CONCISE...|           Mark|   AADISH BOOK DEPOT|    0|
|9789384716165|LIEUTEMarkMarkT G...|           Mark|          AAM COMICS|    2|
|9789384716233|LN. MarkIK SUNDER...|            N.A|          AAN COMICS|    0|
|9789384850319|      I AM KRISHMark|   DEEP TRIVEDI|AATMAN INNOVATION...|    1|
|9789384850357|DON'T TEACH ME TO...|   DEEP TRIVEDI|AATMAN INNOVATION...|    0|
|9789384850364|MUJHE SAHISHNUTA ...|   DEEP TRIVEDI|AATMAN INNOVATION...|    0|
|9789384850746|  SECRETS OF DESTINY|   DEEP TRIVEDI|AATMAN INNOVATION...|    1|
+-------------+--------------------+---------------+--------------------+-----+
only showing top 20 rows


>>> df1.printSchema()
root
 |-- Serial Number: string (nullable = true)
 |-- Company Name: string (nullable = true)
 |-- Employee Markme: string (nullable = true)
 |-- Description: string (nullable = true)
 |-- Leave: string (nullable = true)

>>> df2 = spark.read.option("header",True).option("delimiter",",").option("inferSchema",True).csv("/spark/Sample2000.csv")
>>> df2.printSchema()
root
 |-- Serial Number: long (nullable = true)
 |-- Company Name: string (nullable = true)
 |-- Employee Markme: string (nullable = true)
 |-- Description: string (nullable = true)
 |-- Leave: integer (nullable = true)

>>> 