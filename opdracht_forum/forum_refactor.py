from pyspark.sql import SparkSession
import mysql.connector

database_password = input("Your database password please my good sir")

import subprocess
subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)

spark = SparkSession.builder.appName("Basics").getOrCreate()

# Reading in data
comment_ids = spark.read.csv('s3n://dschallengeforum/comment_hasCreator_person_0_0.csv', sep='|', header=True, inferSchema=True)
comment_list = spark.read.csv('s3n://dschallengeforum/comment_0_0.csv', sep='|', header=True, inferSchema=True)
mail = spark.read.csv('s3n://dschallengeforum/person_email_emailaddress_0_0.csv', sep='|', header=True, inferSchema=True)
person = spark.read.csv('s3n://dschallengeforum/person_0_0.csv', sep='|', header=True, inferSchema=True)

'''
Renaming column name where we are going to join on because spark does not 
like dots and capital letters that much
'''
comment_ids = comment_ids.withColumnRenamed('Comment.id', 'comment_id')
comment_ids = comment_ids.withColumnRenamed('Person.id', 'person_id')

mail = mail.withColumnRenamed('Person.id', 'person_id')

comment_list = comment_list.withColumnRenamed('id', 'comment_id')
comment_list = comment_list.withColumnRenamed('creationDate', 'creationDate_comment')


person = person.withColumnRenamed('id', 'person_id')
person = person.withColumnRenamed('creationDate', 'creationDate_person')

# Converting to appropriate datatypes

comment_ids = comment_ids.withColumn('comment_id', comment_ids['comment_id'].cast('int'))
comment_ids = comment_ids.withColumn('person_id', comment_ids['person_id'].cast('int'))

comment_list = comment_list.withColumn('comment_id', comment_list['comment_id'].cast('int'))
comment_list = comment_list.withColumn('creationDate_comment', comment_list['creationDate_comment'].cast('timestamp'))

mail = mail.withColumn('person_id', mail['person_id'].cast('int'))

person = person.withColumn('person_id', person['person_id'].cast('int'))
person = person.withColumn('creationDate_person', person['creationDate_person'].cast('timestamp'))
person = person.withColumn('birthday', person['birthday'].cast('date'))

# We only need the 'id' and 'creationDate' columns so we'll drop the rest
comment_list = comment_list.select('comment_id', 'creationDate_comment')

# Joining the tables together
first_join = comment_ids.join(comment_list, on=['comment_id'], how= 'inner')


second_join = first_join.join(mail, on=["person_id"], how='inner')

# I might have to change person and second_join here again
final_join = person.join(second_join, on=["person_id"], how='inner')


'''
Extracting the month from the 'creationDate_comment' column. 
Please note that these are in numerical format (E.g. 1 = "January")
'''

from pyspark.sql.functions import month
final_join = final_join.withColumn("creationDate_comment_month", month(final_join['creationDate_comment']))

feature_group =['email', 'creationDate_comment_month']
final_join_short = final_join.groupBy(feature_group).count().withColumnRenamed('count', 'Number_of_comments_that_month')
final_join = final_join.join(final_join_short, feature_group) \
        .orderBy(["email", 'creationDate_comment_month']).dropDuplicates()


mode = 'overwrite'
properties = {
"user": "admin",
        "password": database_password,
"driver": "com.mysql.jdbc.Driver"
}

final_join_short.write.jdbc(url='jdbc:mysql://localhost/forum', table="mail", mode=mode, properties=properties)




