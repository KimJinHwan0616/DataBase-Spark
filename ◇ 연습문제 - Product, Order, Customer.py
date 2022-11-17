from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import pyspark.sql.functions as F

spark = SparkSession.builder.appName("hanbit").getOrCreate()
customer = spark.read.csv('../data/customeromers.csv', header=True, inferSchema=True)
product = spark.read.csv('../data/productucts.csv', header=True, inferSchema=True)
order = spark.read.csv('../data/Orders.csv', header=True, inferSchema=True)

###################################################################
# 나이를 아직 입력하지 않은 고객 조회
customer.filter(customer.age.isNull()).show()

# 나이를 이미 입력한 고객 조회
customer.filter(customer.age.isNotNull()).show()

# 주문제품-오름차순, 수량-내림차순 정렬 후 조회
order.select('*').orderBy(['productid', 'amount'], ascending=[1, 0]).show()

# apple고객이 15개 이상 주문한 제품 조회
order.filter(order.userid == 'apple') & (order.amount >= 15)\
    .select('userid', 'productid').show()

# 제품가격 2000이상 3000이하 조회
product.filter(product.price.between(2000, 3000)).select('*').show(5)

# 성이 김씨인 고객 조회
customer.filter(customer.name.like('김%')).show()

# 고객 아이디의 문자길이가 5인 고객 조회
customer.filter(F.col('userid').like('_____')).show()
customer.filter(F.length('userid') == 5).show()

###################################################################
# 한빛 제과에서 제조한 제품의 재고량 합계 조회
product.filter(product.maker == '한빛제과').agg(F.sum('stock').alias('재고량 합계')).show()

# 총 고객수 조회
customer.agg(F.count('name').alias('고객수')).show()

# 주문제품별 수량의 합계 조회
order.groupBy('prodid').agg(F.sum('amount').alias('주문수량 합계')).show()

###################################################################
# 당근 고객이 주문한 상품의 이름, 가격 조회
order_product = order.join(product, 'prodid', 'inner')
order_product.show(5)

order_product.filter(order_product.userid == 'carrot')\
    .select('prodname', 'price').show()

# 주문을 한번도 하지 않은 고객의 이름, 등급을 조회
order_customer = order.join(customer, 'userid', 'outer')

order_customer.filter(order_customer.orderid.isNull())\
    .select('name', 'grade').show()

# 주문이 한번도 되지 않은 제품이름, 제조업체 조회
order_customer2 = order.join(customer, 'userid', 'outer')
order_customer2.filter(order_customer2.orderid.isNull())\
    .select('prodname', 'maker').show()

# 고객, 제품, 주문 테이블 3개 join
order_product = order.join(product, 'prodid', 'inner')
customer_order_product = order_product.join(customer, 'userid', 'inner')
