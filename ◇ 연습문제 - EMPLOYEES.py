from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('emp').getOrCreate()
emp = spark.read.csv('../data/EMPLOYEES.csv', header=True, inferSchema=True)

###################################################################
# 모든 사원의 이름 조회
emp.select('First_name').show()

# 급여가 7000 이상인 사원 조회
emp.filter(emp.SALARY >= 7000).select('First_name', 'Last_name', 'salary').show()

# 부서번호별 사원 수를 부서번호 역순 정렬
emp.groupBy('DEPARTMENT_ID').count().orderBy('DEPARTMENT_ID', ascending=False).show()

# 사원들의 모든 직책 수 (중복X)
emp.select('JOB_ID').distinct().count()
# emp.select(F.countDistinct('JOB_ID').alias('JOB_ID')).show()

# 사원들의 이름, 직책, 급여, 5% 인상한 급여(별칭) 조회
emp.select( 'First_name', 'Job_id', 'Salary',
           (emp.SALARY * 1.05).alias('5% Salary') ).show()

# 직책별 평균 급여를 내림차순 정렬
# select JOB_ID , avg(SALARY) from EMPLOYEES
# group by JOB_ID
# order by avg(SALARY) desc;
emp.groupBy('JOB_ID').agg(F.avg('SALARY'))\
    .orderBy('avg(SALARY)', ascending=False).show()

# 직책별 평균 급여를 내림차순 정렬 ( 별칭 )
emp.groupBy('JOB_ID').agg( F.avg('SALARY').alias('별칭') ).\
orderBy('별칭', ascending=False).show()
