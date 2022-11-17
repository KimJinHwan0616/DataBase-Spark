from pyspark.sql import SparkSession

spark = SparkSession.builder.master('app').appName("sparkSQL").getOrCreate()

emp = spark.read.csv('../data/EMPLOYEES.csv', header=True, inferSchema=True)
EMP = emp.createOrReplaceTempView('EMP')

# 급여가 7000 이상인 사원 조회
sql = 'SELECT FIRST_NAME , LAST_NAME, SALARY  FROM EMP WHERE SALARY >= 7000'
spark.sql(sql).show()

# 직책별 평균 급여를 내림차순으로 정렬
sql = '''select JOB_ID , avg(SALARY) from EMP
group by JOB_ID
order by avg(SALARY) desc'''
spark.sql(sql).show()




