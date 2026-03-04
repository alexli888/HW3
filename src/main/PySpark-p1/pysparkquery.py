from pyspark import SparkContext

# spark context local
sc = SparkContext("local[*]", "Project3_Problem1")

# load data and split by comma (my data is in hadoop)
meta_event = sc.textFile("hdfs://localhost:9000/user/mahit/HW3/Meta-Event.txt").map(lambda line: line.split(","))
meta_no_disclosure = sc.textFile("hdfs://localhost:9000/user/mahit/HW3/Meta-Event-No-Disclosure.txt").map(lambda line: line.split(","))
reported_illnesses = sc.textFile("hdfs://localhost:9000/user/mahit/HW3/Reported-Illnesses.txt").map(lambda line: line.split(","))


# Query 1
# Return all sick people
q1_result = meta_event.filter(lambda x: x[3] == "sick")


# Query 2
# Return Sick people ID, Name, and Table
# Map ID with patient ID, second map is for is sick
no_disc_pair = meta_no_disclosure.map(lambda x: (x[0], (x[1], x[2])))
rep_ill_pair = reported_illnesses.map(lambda x: (x[0], x[1]))
# Join on ID and extract the relevant fields
q2_result = no_disc_pair.join(rep_ill_pair).map(lambda x: (x[0], x[1][0][0], x[1][0][1]))


# Query 3 
# Healthy person sitting with Sick Person
# Tables with sick person, label as sick table
sick_table = meta_event.filter(lambda x: x[3] == "sick").map(lambda x: (x[2], "sick_table")).distinct()
# healthy people with their table,
healthy = meta_event.filter(lambda x: x[3] == "not-sick").map(lambda x: (x[2], (x[0], x[1])))
# join to find healthy people sitting at sick table
q3_result = healthy.join(sick_table).map(lambda x: x[1][0]).distinct()


# Query 4
# table flag for concern (sick person sitting) or healthy (so sick people)
def evaluate_table(tests):
    test_list = list(tests)
    # Had GPT help with the flag and understanding how it works
    flag = "concern" if "sick" in test_list else "healthy"
    return (len(test_list), flag)
# Map --> group by the table --> apply the function
q4_result = meta_event.map(lambda x: (x[2], x[3])).groupByKey().mapValues(evaluate_table)


# Query 5
# Healthy people (No-Disclosure) sitting with a sick person (Reported)
# key value pairs
no_disc_table_pair = meta_no_disclosure.map(lambda x: (x[0], x))

# join with illness and get sick people for sick table
sick_people_joined = no_disc_table_pair.join(rep_ill_pair)
sick_tables_q5 = sick_people_joined.map(lambda x: (x[1][0][2], "sick_table")).distinct()

# find the healthy people and map (NEEDS A LEFT OUTER JOIN, 
# needed GPT help for this) to get the healthy people sitting at sick tables
healthy_people_q5 = no_disc_table_pair.leftOuterJoin(rep_ill_pair)\
    .filter(lambda x: x[1][1] is None)\
    .map(lambda x: (x[1][0][2], x[1][0]))

#Join healthy with sick
q5_result = healthy_people_q5.join(sick_tables_q5).map(lambda x: tuple(x[1][0])).distinct()

# save to hadoop file system
q1_result.saveAsTextFile("hdfs://localhost:9000/user/mahit/HW3/query1_output")
q2_result.saveAsTextFile("hdfs://localhost:9000/user/mahit/HW3/query2_output")
q3_result.saveAsTextFile("hdfs://localhost:9000/user/mahit/HW3/query3_output")
q4_result.saveAsTextFile("hdfs://localhost:9000/user/mahit/HW3/query4_output")
q5_result.saveAsTextFile("hdfs://localhost:9000/user/mahit/HW3/query5_output")