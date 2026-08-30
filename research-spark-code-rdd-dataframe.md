# KỸ THUẬT CODE APACHE SPARK: RDD & DATAFRAME API ĐẦY ĐỦ

> Tài liệu này bao gồm toàn bộ cú pháp, các hàm phổ biến, và ví dụ code thực tế cho PySpark RDD và DataFrame API. Mỗi hàm đều có ví dụ code chạy được.

---

## MỤC LỤC

1. [Khởi tạo SparkSession & SparkContext](#1-khởi-tạo)
2. [RDD API - Cú pháp & Hàm đầy đủ](#2-rdd-api)
3. [DataFrame API - Cú pháp & Hàm đầy đủ](#3-dataframe-api)
4. [Spark SQL Functions (`pyspark.sql.functions`)](#4-spark-sql-functions)
5. [Đọc & Ghi dữ liệu (I/O)](#5-đọc-và-ghi-dữ-liệu)
6. [Window Functions - Hàm cửa sổ](#6-window-functions)
7. [Xử lý kiểu dữ liệu nâng cao](#7-xử-lý-kiểu-dữ-liệu-nâng-cao)
8. [UDF (User Defined Functions)](#8-udf-user-defined-functions)
9. [Streaming API cơ bản](#9-structured-streaming)
10. [Các pattern code phổ biến trong ETL](#10-etl-patterns-thực-tế)

---

## 1. KHỞI TẠO

### SparkSession (Spark 2.x trở lên - dùng chính)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Lấy SparkContext từ SparkSession
sc = spark.sparkContext
sc.setLogLevel("WARN")  # Giảm log noise

# Lấy SQLContext
sqlContext = spark.builder.getOrCreate()
```

### Dừng Spark Session
```python
spark.stop()
```

---

## 2. RDD API

### 2.1 Tạo RDD

```python
# Cách 1: Từ Python list
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=4)  # Chỉ định 4 partitions

# Cách 2: Từ file text
rdd = sc.textFile("hdfs:///data/logs/*.txt")
rdd = sc.textFile("s3://bucket/path/file.txt", minPartitions=8)

# Cách 3: Từ sequence file
rdd = sc.sequenceFile("hdfs:///data/seq/")

# Cách 4: Từ range
rdd = sc.range(0, 100, step=2)  # 0, 2, 4, ..., 98

# Cách 5: Từ danh sách tuple (pair RDD)
pair_rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])

# Cách 6: Từ DataFrame
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
rdd = df.rdd  # Mỗi phần tử là một Row object

# Cách 7: Empty RDD
empty_rdd = sc.emptyRDD()
```

---

### 2.2 RDD Transformations (Lazy - không thực thi ngay)

#### `map(func)` – Biến đổi từng phần tử, 1 input → 1 output
```python
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Nhân đôi từng số
doubled = rdd.map(lambda x: x * 2)
# Result: [2, 4, 6, 8, 10]

# Với tuple
pair_rdd = rdd.map(lambda x: (x, x ** 2))
# Result: [(1,1), (2,4), (3,9), (4,16), (5,25)]
```

#### `flatMap(func)` – 1 input → 0 hoặc nhiều outputs, sau đó flatten
```python
rdd = sc.parallelize(["hello world", "foo bar"])

# Tách từng câu thành danh sách các từ
words = rdd.flatMap(lambda line: line.split(" "))
# Result: ["hello", "world", "foo", "bar"]

# Ví dụ tạo range
rdd2 = sc.parallelize([1, 2, 3])
flat = rdd2.flatMap(lambda x: range(1, x + 1))
# Result: [1, 1, 2, 1, 2, 3]
```

#### `filter(func)` – Chỉ giữ các phần tử thỏa điều kiện
```python
rdd = sc.parallelize([1, 2, 3, 4, 5, 6])

evens = rdd.filter(lambda x: x % 2 == 0)
# Result: [2, 4, 6]

# Lọc string
names = sc.parallelize(["Alice", "Bob", "Anna", "Charlie"])
a_names = names.filter(lambda name: name.startswith("A"))
# Result: ["Alice", "Anna"]
```

#### `mapPartitions(func)` – Biến đổi cả một partition (hiệu quả hơn map khi cần khởi tạo resource)
```python
def process_partition(iterator):
    # Khởi tạo DB connection một lần cho cả partition
    # connection = create_db_connection()
    for record in iterator:
        yield record * 2
    # connection.close()

result = rdd.mapPartitions(process_partition)
```

#### `mapPartitionsWithIndex(func)` – Như mapPartitions nhưng có thêm index của partition
```python
def add_partition_info(partition_index, iterator):
    for record in iterator:
        yield (partition_index, record)

result = rdd.mapPartitionsWithIndex(add_partition_info)
```

#### `distinct()` – Loại bỏ phần tử trùng lặp
```python
rdd = sc.parallelize([1, 2, 2, 3, 3, 3])
unique = rdd.distinct()
# Result: [1, 2, 3]
```

#### `sample(withReplacement, fraction, seed)` – Lấy mẫu ngẫu nhiên
```python
rdd = sc.parallelize(range(100))

# Lấy khoảng 20% dữ liệu, không thay thế
sample = rdd.sample(withReplacement=False, fraction=0.2, seed=42)

# Lấy mẫu chính xác N phần tử
sample_n = rdd.takeSample(withReplacement=False, num=10, seed=42)
```

#### `union(otherRDD)` – Gộp 2 RDD (giữ duplicate)
```python
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([3, 4, 5])
merged = rdd1.union(rdd2)
# Result: [1, 2, 3, 3, 4, 5]
```

#### `intersection(otherRDD)` – Phần giao (loại duplicate)
```python
common = rdd1.intersection(rdd2)
# Result: [3]
```

#### `subtract(otherRDD)` – Phần hiệu (rdd1 - rdd2)
```python
diff = rdd1.subtract(rdd2)
# Result: [1, 2]
```

#### `cartesian(otherRDD)` – Tích Descartes
```python
rdd_a = sc.parallelize([1, 2])
rdd_b = sc.parallelize(["a", "b"])
product = rdd_a.cartesian(rdd_b)
# Result: [(1,'a'), (1,'b'), (2,'a'), (2,'b')]
```

#### `zip(otherRDD)` – Ghép cặp theo vị trí (hai RDD phải cùng số phần tử và partitions)
```python
rdd_keys = sc.parallelize(["a", "b", "c"])
rdd_vals = sc.parallelize([1, 2, 3])
zipped = rdd_keys.zip(rdd_vals)
# Result: [("a", 1), ("b", 2), ("c", 3)]
```

#### `sortBy(keyfunc, ascending)` – Sắp xếp
```python
rdd = sc.parallelize([3, 1, 4, 1, 5, 9])
sorted_rdd = rdd.sortBy(lambda x: x, ascending=True)
# Result: [1, 1, 3, 4, 5, 9]

# Sắp xếp tuple theo giá trị
pair_rdd = sc.parallelize([("a", 3), ("b", 1), ("c", 2)])
sorted_pairs = pair_rdd.sortBy(lambda kv: kv[1])
# Result: [("b",1), ("c",2), ("a",3)]
```

#### `repartition(numPartitions)` – Thay đổi số partition (có shuffle)
```python
rdd = sc.parallelize(range(100))
rdd_repartitioned = rdd.repartition(10)
print(rdd_repartitioned.getNumPartitions())  # 10
```

#### `coalesce(numPartitions)` – Giảm số partition (không shuffle)
```python
rdd_small = rdd.coalesce(2)
print(rdd_small.getNumPartitions())  # 2
```

---

### 2.3 Pair RDD Transformations (Key-Value RDD)

#### `reduceByKey(func)` – Giảm theo key (có pre-aggregation, hiệu quả)
```python
pair_rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4)])

# Tổng theo key
sums = pair_rdd.reduceByKey(lambda x, y: x + y)
# Result: [("a", 4), ("b", 6)]
```

#### `groupByKey()` – Nhóm các value theo key (ít dùng vì tốn bộ nhớ)
```python
grouped = pair_rdd.groupByKey()
# Result: [("a", [1,3]), ("b", [2,4])]

# Chuyển sang list để xem
result = grouped.mapValues(list).collect()
```

#### `aggregateByKey(zeroValue, seqOp, combOp)` – Aggregate linh hoạt nhất
```python
# Tính tổng và đếm số lượng (để tính trung bình)
rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5)])

# (sum, count)
zero = (0, 0)
seq_op = lambda acc, val: (acc[0] + val, acc[1] + 1)
comb_op = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])

result = rdd.aggregateByKey(zero, seq_op, comb_op)
# Result: [("a", (9, 3)), ("b", (6, 2))]

# Tính mean
mean_rdd = result.mapValues(lambda t: t[0] / t[1])
```

#### `sortByKey(ascending)` – Sắp xếp theo key
```python
sorted_rdd = pair_rdd.sortByKey(ascending=True)
```

#### `mapValues(func)` – Biến đổi chỉ phần value, giữ nguyên key
```python
pair_rdd = sc.parallelize([("a", [1, 2, 3]), ("b", [4, 5])])
lengths = pair_rdd.mapValues(len)
# Result: [("a", 3), ("b", 2)]
```

#### `flatMapValues(func)` – flatMap chỉ trên value
```python
pair_rdd = sc.parallelize([("a", [1, 2]), ("b", [3])])
flat = pair_rdd.flatMapValues(lambda v: v)
# Result: [("a",1), ("a",2), ("b",3)]
```

#### `keys()` và `values()`
```python
keys = pair_rdd.keys()    # Result: ["a", "b", "a", "b"]
values = pair_rdd.values()  # Result: [1, 2, 3, 4]
```

#### `join(otherRDD)` – Inner Join
```python
rdd1 = sc.parallelize([("alice", 25), ("bob", 30)])
rdd2 = sc.parallelize([("alice", "NY"), ("charlie", "LA")])

joined = rdd1.join(rdd2)
# Result: [("alice", (25, "NY"))]
```

#### `leftOuterJoin / rightOuterJoin / fullOuterJoin`
```python
left = rdd1.leftOuterJoin(rdd2)
# Result: [("alice", (25, "NY")), ("bob", (30, None))]

right = rdd1.rightOuterJoin(rdd2)
# Result: [("alice", (25, "NY")), ("charlie", (None, "LA"))]
```

#### `countByKey()` – Đếm số lượng theo key (Action)
```python
pair_rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
counts = pair_rdd.countByKey()
# Result: {"a": 2, "b": 1}  (dict Python)
```

#### `lookup(key)` – Tìm tất cả values của một key
```python
values = pair_rdd.lookup("a")
# Result: [1, 3]
```

#### `subtractByKey(otherRDD)` – Loại bỏ keys xuất hiện trong RDD kia
```python
rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
rdd2 = sc.parallelize([("b", 99)])
result = rdd1.subtractByKey(rdd2)
# Result: [("a", 1), ("c", 3)]
```

---

### 2.4 RDD Actions (Kích hoạt tính toán thực sự)

#### `collect()` – Lấy toàn bộ dữ liệu về Driver (cẩn thận với dữ liệu lớn!)
```python
data = rdd.collect()  # Trả về Python list
```

#### `take(n)` – Lấy n phần tử đầu tiên
```python
first_5 = rdd.take(5)
```

#### `first()` – Lấy phần tử đầu tiên
```python
first = rdd.first()
```

#### `top(n)` / `takeOrdered(n)` – Lấy n phần tử lớn/nhỏ nhất
```python
top3 = rdd.top(3)           # [9, 5, 4] (lớn nhất)
bottom3 = rdd.takeOrdered(3)  # [1, 1, 3] (nhỏ nhất)

# Tùy chỉnh key function
top3_by_val = pair_rdd.top(3, key=lambda x: x[1])
```

#### `count()` – Đếm số phần tử
```python
n = rdd.count()
```

#### `countByValue()` – Đếm tần suất từng giá trị
```python
rdd = sc.parallelize(["a", "b", "a", "c", "b", "a"])
freq = rdd.countByValue()
# Result: {"a": 3, "b": 2, "c": 1}
```

#### `reduce(func)` – Gộp toàn bộ phần tử thành 1 giá trị
```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
total = rdd.reduce(lambda x, y: x + y)  # 15
product = rdd.reduce(lambda x, y: x * y)  # 120
```

#### `fold(zeroValue, func)` – Như reduce nhưng có giá trị khởi đầu
```python
total = rdd.fold(0, lambda x, y: x + y)
```

#### `aggregate(zeroValue, seqOp, combOp)` – Aggregate toàn bộ về một giá trị
```python
# Tính mean toàn bộ RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.aggregate(
    (0, 0),
    lambda acc, val: (acc[0] + val, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)
mean = result[0] / result[1]  # 3.0
```

#### `sum()`, `max()`, `min()`, `mean()`, `stdev()`, `variance()` – Số học (chỉ cho numeric RDD)
```python
rdd = sc.parallelize([1.0, 2.0, 3.0, 4.0, 5.0])
print(rdd.sum())       # 15.0
print(rdd.max())       # 5.0
print(rdd.min())       # 1.0
print(rdd.mean())      # 3.0
print(rdd.stdev())     # 1.4142...
print(rdd.variance())  # 2.0
```

#### `stats()` – Thống kê mô tả tổng hợp
```python
stats = rdd.stats()
print(stats.count(), stats.mean(), stats.stdev())
```

#### `foreach(func)` – Chạy hàm trên từng phần tử (không trả về kết quả về Driver)
```python
rdd.foreach(lambda x: print(x))  # Thực thi trên Executors
```

#### `foreachPartition(func)` – Chạy hàm trên từng partition (tốt để bulk insert vào DB)
```python
def write_partition_to_db(iterator):
    conn = create_db_connection()
    for record in iterator:
        conn.insert(record)
    conn.close()

rdd.foreachPartition(write_partition_to_db)
```

#### `saveAsTextFile(path)` – Ghi ra file text
```python
rdd.saveAsTextFile("hdfs:///output/result")
rdd.saveAsTextFile("s3://bucket/output/", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
```

#### `getNumPartitions()` – Xem số partitions
```python
print(rdd.getNumPartitions())
```

---

### 2.5 Biến chia sẻ (Broadcast & Accumulator)

#### Broadcast Variables – Gửi 1 lần đến tất cả Executors (read-only)
```python
# Thường dùng cho lookup table nhỏ
lookup_dict = {"US": "United States", "VN": "Vietnam", "JP": "Japan"}
broadcast_lookup = sc.broadcast(lookup_dict)

rdd = sc.parallelize([("Alice", "US"), ("Bob", "VN")])

def enrich_country(row):
    name, code = row
    country = broadcast_lookup.value.get(code, "Unknown")
    return (name, country)

result = rdd.map(enrich_country).collect()
# Result: [("Alice", "United States"), ("Bob", "Vietnam")]

# Hủy sau khi dùng xong để giải phóng bộ nhớ Executor
broadcast_lookup.unpersist()
```

#### Accumulators – Biến đếm toàn cục (write-only từ Executors)
```python
error_counter = sc.accumulator(0)
total_amount = sc.accumulator(0.0)

def process_and_count_errors(record):
    global error_counter
    try:
        amount = float(record)
        total_amount.add(amount)
        return amount
    except ValueError:
        error_counter.add(1)
        return None

rdd = sc.parallelize(["100", "abc", "200", "invalid", "300"])
processed = rdd.map(process_and_count_errors).filter(lambda x: x is not None)
processed.collect()

print(f"Total errors: {error_counter.value}")  # 2
print(f"Total amount: {total_amount.value}")   # 600.0
```

---

## 3. DATAFRAME API

### 3.1 Tạo DataFrame

```python
from pyspark.sql import Row
from pyspark.sql.types import *

# Cách 1: Từ list of tuples với schema
data = [(1, "Alice", 25, 50000.0),
        (2, "Bob", 30, 60000.0),
        (3, "Charlie", None, 45000.0)]

schema = ["id", "name", "age", "salary"]
df = spark.createDataFrame(data, schema)

# Cách 2: Khai báo schema rõ ràng bằng StructType
schema_explicit = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
])
df = spark.createDataFrame(data, schema_explicit)

# Cách 3: Từ list of Row objects
rows = [
    Row(id=1, name="Alice", age=25, salary=50000.0),
    Row(id=2, name="Bob", age=30, salary=60000.0)
]
df = spark.createDataFrame(rows)

# Cách 4: Từ Pandas DataFrame
import pandas as pd
pdf = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
df = spark.createDataFrame(pdf)

# Cách 5: Từ RDD
rdd = sc.parallelize([(1, "Alice"), (2, "Bob")])
df = spark.createDataFrame(rdd, ["id", "name"])

# Cách 6: Từ Dictionary
dict_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
df = spark.createDataFrame(dict_data)

# Cách 7: Từ file (xem Phần 5)
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
```

### 3.2 Xem thông tin DataFrame

```python
# Xem schema
df.printSchema()
df.schema          # Trả về StructType object
df.dtypes          # [("id", "int"), ("name", "string"), ...]

# Xem dữ liệu
df.show()                   # 20 dòng đầu, cắt ngắn
df.show(n=50)               # 50 dòng
df.show(truncate=False)     # Không cắt ngắn cột
df.show(5, truncate=100)    # 5 dòng, cắt ở 100 ký tự

# Thống kê mô tả
df.describe().show()        # count, mean, stddev, min, max
df.summary().show()         # Thêm quartiles 25%, 50%, 75%

# Thông tin cơ bản
print(df.count())           # Số dòng
print(len(df.columns))      # Số cột
print(df.columns)           # Danh sách tên cột

# Lấy về Pandas để xem cục bộ (chỉ dùng khi data nhỏ!)
pdf = df.toPandas()

# Lấy N dòng đầu
rows = df.head(5)           # List of Row objects
rows = df.take(5)           # List of Row objects
row = df.first()            # Row object đầu tiên

# Xem một cell cụ thể
val = df.select("name").first()[0]

# Xem execution plan
df.explain()                # Physical plan
df.explain(extended=True)   # Logical + Physical plan
df.explain(mode="formatted") # Đẹp hơn (Spark 3.x)
```

---

### 3.3 Lựa chọn Cột (Select & Column Operations)

```python
from pyspark.sql.functions import col, column, expr

# Cách chọn cột (nhiều cách tương đương)
df.select("name")
df.select(df.name)
df.select(df["name"])
df.select(col("name"))
df.select(column("name"))

# Chọn nhiều cột
df.select("id", "name", "age")
df.select(col("id"), col("name"), col("age"))

# Đổi tên cột trong select
df.select(col("name").alias("employee_name"))
df.select(col("salary").alias("income"))

# Biểu thức SQL trong select
df.select(expr("name"), expr("salary * 1.1 as new_salary"))

# Chọn tất cả cột
df.select("*")

# Loại bỏ cột
df.drop("age")
df.drop("age", "salary")

# Thêm cột mới
df.withColumn("new_salary", col("salary") * 1.1)
df.withColumn("full_name", col("name"))

# Đổi tên cột
df.withColumnRenamed("name", "employee_name")

# Casting kiểu dữ liệu
df.withColumn("age_str", col("age").cast("string"))
df.withColumn("salary_int", col("salary").cast(IntegerType()))

# Chọn theo vị trí (không khuyến khích)
df[0]  # Không hoạt động như pandas
```

---

### 3.4 Lọc dữ liệu (Filter / Where)

```python
from pyspark.sql.functions import col

# filter và where là đồng nghĩa
df.filter(df["age"] > 25)
df.where(df["age"] > 25)
df.filter(col("age") > 25)
df.filter("age > 25")  # SQL string syntax

# Điều kiện kết hợp
df.filter((col("age") > 25) & (col("salary") > 50000))
df.filter((col("age") > 25) | (col("age").isNull()))
df.filter(~(col("name") == "Alice"))  # NOT

# Các điều kiện phổ biến
df.filter(col("age").isNull())
df.filter(col("age").isNotNull())
df.filter(col("name").isin(["Alice", "Bob"]))
df.filter(~col("name").isin(["Charlie"]))

# LIKE và RLIKE (Regex)
df.filter(col("name").like("A%"))      # Bắt đầu bằng A
df.filter(col("name").rlike("^A.*"))   # Regex pattern

# Between
df.filter(col("age").between(20, 30))

# startswith / endswith / contains
df.filter(col("name").startswith("A"))
df.filter(col("name").endswith("e"))
df.filter(col("name").contains("ic"))
```

---

### 3.5 Sắp xếp (Sort / OrderBy)

```python
from pyspark.sql.functions import col, asc, desc

# Cách 1: String
df.sort("age")
df.sort("age", "salary")
df.orderBy("age")

# Cách 2: Column expression
df.sort(col("age"))
df.sort(col("age").asc())
df.sort(col("age").desc())
df.sort(asc("age"))
df.sort(desc("salary"))

# Nhiều cột với thứ tự khác nhau
df.sort(col("age").asc(), col("salary").desc())
df.orderBy(["age", "salary"], ascending=[True, False])

# Xử lý null
from pyspark.sql.functions import asc_nulls_first, desc_nulls_last
df.sort(asc_nulls_first("age"))   # Null lên đầu
df.sort(desc_nulls_last("salary")) # Null xuống cuối
```

---

### 3.6 Gom nhóm & Tổng hợp (GroupBy & Aggregation)

```python
from pyspark.sql import functions as F

# groupBy + agg
df.groupBy("department").agg(
    F.count("id").alias("headcount"),
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.min("age").alias("youngest_age"),
    F.stddev("salary").alias("salary_stddev"),
    F.collect_list("name").alias("all_names"),     # Giữ duplicate
    F.collect_set("name").alias("unique_names"),   # Loại duplicate
    F.first("name").alias("first_emp"),            # Phần tử đầu
    F.last("name").alias("last_emp"),              # Phần tử cuối
)

# Các shorthand
df.groupBy("department").count()     # Đếm số dòng
df.groupBy("department").sum("salary")
df.groupBy("department").avg("salary")
df.groupBy("department").max("salary")
df.groupBy("department").min("salary")

# GroupBy nhiều cột
df.groupBy("department", "gender").agg(F.count("*").alias("n"))

# Aggregate toàn bộ DataFrame (không groupBy)
df.agg(
    F.count("*").alias("total"),
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("avg_salary"),
)

# countDistinct
df.agg(F.countDistinct("department").alias("num_departments"))

# approx_count_distinct (nhanh hơn cho Big Data)
df.agg(F.approx_count_distinct("user_id").alias("approx_unique_users"))
```

---

### 3.7 Join Operations

```python
# Inner Join (mặc định)
result = df1.join(df2, on="id", how="inner")
result = df1.join(df2, df1["id"] == df2["user_id"], how="inner")

# Các loại join
df1.join(df2, "id", "inner")       # Inner
df1.join(df2, "id", "left")        # Left Outer
df1.join(df2, "id", "right")       # Right Outer
df1.join(df2, "id", "full")        # Full Outer
df1.join(df2, "id", "left_anti")   # Chỉ lấy dòng df1 KHÔNG có trong df2 (Anti Join)
df1.join(df2, "id", "left_semi")   # Chỉ lấy dòng df1 có match trong df2 (Semi Join)
df1.join(df2, "id", "cross")       # Cartesian Product

# Join trên nhiều cột
df1.join(df2, ["id", "date"], "inner")
df1.join(df2, (df1["id"] == df2["id"]) & (df1["date"] == df2["date"]))

# Xử lý tên cột bị trùng sau join
# Cách 1: Dùng alias
df1_a = df1.alias("a")
df2_a = df2.alias("b")
result = df1_a.join(df2_a, col("a.id") == col("b.id"))
result.select("a.id", "a.name", "b.salary")

# Cách 2: Drop cột trùng
result = df1.join(df2, df1["id"] == df2["id"]).drop(df2["id"])

# Broadcast Join (force khi biết một bảng nhỏ)
from pyspark.sql.functions import broadcast
result = df_large.join(broadcast(df_small), "id")
```

---

### 3.8 Missing Data (Null Handling)

```python
from pyspark.sql.functions import col, isnull, isnan, when, coalesce, lit

# Kiểm tra null
df.filter(col("age").isNull())
df.filter(col("age").isNotNull())
df.filter(isnull(col("age")))

# Đếm null từng cột
from pyspark.sql.functions import count, sum as spark_sum
null_counts = df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c) 
    for c in df.columns
])
null_counts.show()

# Xóa dòng có null
df.na.drop()                           # Xóa dòng có BẤT KỲ null nào
df.na.drop(how="all")                  # Chỉ xóa dòng TẤT CẢ đều null
df.na.drop(subset=["age", "salary"])   # Chỉ xét null ở các cột nhất định
df.na.drop(thresh=2)                   # Giữ dòng có ít nhất 2 giá trị không null

# Điền giá trị vào null
df.na.fill(0)                         # Điền 0 cho tất cả cột số
df.na.fill("Unknown")                 # Điền "Unknown" cho tất cả cột string
df.na.fill({"age": 0, "name": "N/A"}) # Điền từng cột với giá trị khác nhau

# Thay thế giá trị (Replace)
df.na.replace(0, -1)                          # Thay 0 bằng -1 trong tất cả cột số
df.na.replace({"Alice": "Alicia"})            # Thay value cụ thể
df.na.replace(0, -1, subset=["salary"])       # Chỉ thay ở cột salary

# Sử dụng coalesce (trả về giá trị đầu tiên không null)
df.withColumn("age_filled", coalesce(col("age"), lit(0)))
df.withColumn("name_filled", coalesce(col("name"), col("nickname"), lit("Unknown")))

# when/otherwise (SQL CASE WHEN)
df.withColumn("age_category",
    when(col("age").isNull(), "Unknown")
    .when(col("age") < 30, "Young")
    .when(col("age") < 50, "Middle")
    .otherwise("Senior")
)
```

---

### 3.9 Deduplication (Loại trùng)

```python
# Loại toàn bộ dòng trùng lặp hoàn toàn
df.distinct()

# Loại trùng theo subset cột (giữ dòng đầu tiên)
df.dropDuplicates()
df.dropDuplicates(["name"])
df.dropDuplicates(["name", "department"])

# dropDuplicates alias
df.drop_duplicates(["name"])

# Loại trùng theo nhóm (giữ dòng theo rank - cách chuyên nghiệp)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window = Window.partitionBy("user_id").orderBy(desc("updated_at"))
df_dedup = df.withColumn("rn", row_number().over(window)) \
             .filter(col("rn") == 1) \
             .drop("rn")
```

---

### 3.10 Union & Set Operations

```python
# Union (giữ duplicate) - hai DF phải cùng schema
df1.union(df2)

# Union all (Spark 2.0+) - tương đương union
df1.unionAll(df2)

# Union by name (không cần cùng thứ tự cột)
df1.unionByName(df2)
df1.unionByName(df2, allowMissingColumns=True)  # Spark 3.1+

# Intersect (phần giao)
df1.intersect(df2)
df1.intersectAll(df2)  # Giữ duplicate

# Except (phần hiệu df1 - df2)
df1.exceptAll(df2)
```

---

### 3.11 Reshape & Pivot

```python
# pivot – Chuyển giá trị dòng thành cột
pivot_df = df.groupBy("department").pivot("gender").sum("salary")
# Kết quả: department | Female | Male

# Chỉ định các giá trị pivot (tối ưu hơn, tránh extra shuffle)
df.groupBy("department").pivot("gender", ["Male", "Female"]).sum("salary")

# stack / unpivot (Spark 3.4+)
stacked = df.unpivot(
    ids=["id", "name"],
    values=["Q1", "Q2", "Q3", "Q4"],
    variableColumnName="quarter",
    valueColumnName="revenue"
)
```

---

### 3.12 Sampling & Limiting

```python
# Giới hạn số dòng
df.limit(100)

# Lấy mẫu ngẫu nhiên
df.sample(fraction=0.1, seed=42)
df.sample(withReplacement=False, fraction=0.1, seed=42)

# Lấy mẫu phân tầng (stratified sampling)
df.sampleBy(col="gender", fractions={"Male": 0.3, "Female": 0.7}, seed=42)
```

---

### 3.13 Caching & Persistence

```python
from pyspark import StorageLevel

# Cache mặc định (MEMORY_AND_DISK)
df.cache()
df.persist()

# Chỉ định mức persistence
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY_SER)      # Serialized
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized + Disk fallback
df.persist(StorageLevel.DISK_ONLY)
df.persist(StorageLevel.OFF_HEAP)

# Giải phóng cache
df.unpersist()
```

---

## 4. SPARK SQL FUNCTIONS

### 4.1 Import

```python
# Import tất cả
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, expr, when, coalesce,
    count, sum, avg, min, max, stddev,
    abs, round, ceil, floor, sqrt, pow, log,
    upper, lower, trim, ltrim, rtrim,
    length, substring, split, concat, concat_ws,
    regexp_replace, regexp_extract,
    to_date, to_timestamp, date_format,
    year, month, dayofmonth, dayofweek, dayofyear,
    hour, minute, second,
    date_add, date_sub, datediff, months_between,
    current_date, current_timestamp,
    unix_timestamp, from_unixtime,
    array, array_contains, explode, posexplode,
    struct, create_map, map_keys, map_values,
    isnull, isnan, nvl,
    monotonically_increasing_id,
    row_number, rank, dense_rank,
    lag, lead, first, last,
    ntile, percent_rank, cume_dist,
    asc, desc, asc_nulls_first, desc_nulls_last
)
```

---

### 4.2 Hàm xử lý chuỗi (String Functions)

```python
# Viết hoa / thường
df.withColumn("upper_name", F.upper(col("name")))
df.withColumn("lower_name", F.lower(col("name")))
df.withColumn("init_cap", F.initcap(col("name")))  # "john doe" → "John Doe"

# Độ dài chuỗi
df.withColumn("name_len", F.length(col("name")))

# Cắt khoảng trắng
df.withColumn("trimmed", F.trim(col("name")))    # Cả hai đầu
df.withColumn("ltrimmed", F.ltrim(col("name")))  # Bên trái
df.withColumn("rtrimmed", F.rtrim(col("name")))  # Bên phải

# Lấy chuỗi con
df.withColumn("sub", F.substring(col("name"), 1, 3))  # pos=1 (1-indexed), len=3

# Tìm kiếm trong chuỗi
df.withColumn("pos", F.locate("Bob", col("name")))    # Vị trí của "Bob" (1-indexed, 0 nếu không tìm thấy)
df.withColumn("has_alice", F.instr(col("name"), "Alice"))  # 0 hoặc >= 1

# Nối chuỗi
df.withColumn("full", F.concat(col("first_name"), lit(" "), col("last_name")))
df.withColumn("full", F.concat_ws(" ", col("first_name"), col("last_name")))  # concat với separator

# Regex
df.withColumn("cleaned", F.regexp_replace(col("phone"), "[^0-9]", ""))  # Xóa ký tự không phải số
df.withColumn("code", F.regexp_extract(col("text"), r"(\d{4})", 1))     # Trích xuất nhóm regex

# Tách chuỗi
df.withColumn("parts", F.split(col("full_name"), " "))    # Trả về ArrayType
df.withColumn("first", F.split(col("full_name"), " ")[0]) # Lấy phần tử đầu

# Lặp chuỗi
df.withColumn("repeated", F.repeat(col("name"), 3))  # "AliceAliceAlice"

# Thay thế (reverse, lpad, rpad)
df.withColumn("rev", F.reverse(col("name")))
df.withColumn("padded_left", F.lpad(col("id_str"), 10, "0"))  # Pad 0 bên trái
df.withColumn("padded_right", F.rpad(col("name"), 20, " "))   # Pad space bên phải

# Format chuỗi (printf-style)
df.withColumn("msg", F.format_string("User %s has salary %d", col("name"), col("salary")))

# Encode / Decode
df.withColumn("b64", F.base64(col("binary_col")))
df.withColumn("decoded", F.unbase64(col("b64_col")))

# MD5 / SHA1 / SHA2 hash
df.withColumn("hash_md5", F.md5(col("data")))
df.withColumn("hash_sha256", F.sha2(col("data"), 256))

# Từ chuỗi sang hex
df.withColumn("hex_val", F.hex(col("id")))
df.withColumn("int_val", F.unhex(col("hex_col")))

# Soundex (phonetic encoding)
df.withColumn("sdx", F.soundex(col("name")))

# Kiểm tra pattern
df.withColumn("is_digit", F.col("phone").rlike(r"^\d+$"))

# Levenshtein distance
from pyspark.sql.functions import levenshtein
df.withColumn("dist", levenshtein(col("word1"), col("word2")))
```

---

### 4.3 Hàm xử lý số (Numeric Functions)

```python
# Giá trị tuyệt đối
df.withColumn("abs_val", F.abs(col("value")))

# Làm tròn
df.withColumn("rounded", F.round(col("price"), 2))      # 2 chữ số thập phân
df.withColumn("ceiling", F.ceil(col("price")))           # Làm tròn lên
df.withColumn("flooring", F.floor(col("price")))         # Làm tròn xuống
df.withColumn("trunc", F.bround(col("price"), 1))        # Round to even (banker's rounding)

# Căn bậc hai, lũy thừa
df.withColumn("sqrt_val", F.sqrt(col("value")))
df.withColumn("sq", F.pow(col("value"), 2))
df.withColumn("cube", F.pow(col("value"), 3))

# Log
df.withColumn("log_e", F.log(col("value")))          # ln (natural log)
df.withColumn("log_2", F.log(2, col("value")))        # log base 2
df.withColumn("log_10", F.log10(col("value")))        # log base 10

# Số ngẫu nhiên
df.withColumn("rand", F.rand(seed=42))       # [0, 1) uniform distribution
df.withColumn("randn", F.randn(seed=42))     # Standard Normal distribution

# Số nguyên tự tăng
df.withColumn("seq_id", F.monotonically_increasing_id())

# Hàm số học khác
df.withColumn("exp_val", F.exp(col("value")))          # e^value
df.withColumn("sign", F.signum(col("value")))           # -1, 0, 1
df.withColumn("factorial", F.factorial(col("n")))
df.withColumn("pmod", F.pmod(col("a"), col("b")))      # Python-style modulo (always non-negative)

# Bit operations
df.withColumn("bit_and", F.bitwiseNOT(col("a")))
```

---

### 4.4 Hàm xử lý ngày tháng (Date & Time Functions)

```python
# Lấy ngày, giờ hiện tại
df.withColumn("today", F.current_date())
df.withColumn("now", F.current_timestamp())

# Chuyển đổi kiểu
df.withColumn("date_col", F.to_date(col("date_str"), "yyyy-MM-dd"))
df.withColumn("ts_col", F.to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"))

# Trích xuất thành phần
df.withColumn("yr", F.year(col("date_col")))
df.withColumn("mo", F.month(col("date_col")))
df.withColumn("d", F.dayofmonth(col("date_col")))
df.withColumn("dow", F.dayofweek(col("date_col")))     # 1=Sunday, 7=Saturday
df.withColumn("doy", F.dayofyear(col("date_col")))
df.withColumn("woy", F.weekofyear(col("date_col")))
df.withColumn("q", F.quarter(col("date_col")))

# Với timestamp
df.withColumn("h", F.hour(col("ts_col")))
df.withColumn("m", F.minute(col("ts_col")))
df.withColumn("s", F.second(col("ts_col")))

# Tính toán ngày tháng
df.withColumn("next_week", F.date_add(col("date_col"), 7))
df.withColumn("last_week", F.date_sub(col("date_col"), 7))
df.withColumn("diff_days", F.datediff(col("end_date"), col("start_date")))
df.withColumn("diff_months", F.months_between(col("end_date"), col("start_date")))

# Truncate về đầu tháng, đầu năm, ...
df.withColumn("month_start", F.trunc(col("date_col"), "month"))  # YYYY-MM-01
df.withColumn("year_start", F.trunc(col("date_col"), "year"))    # YYYY-01-01

# date_trunc (như trunc nhưng cho timestamp, hỗ trợ nhiều granularity hơn)
df.withColumn("hour_trunc", F.date_trunc("hour", col("ts_col")))
df.withColumn("day_trunc", F.date_trunc("day", col("ts_col")))

# Format ngày thành chuỗi
df.withColumn("formatted", F.date_format(col("date_col"), "dd/MM/yyyy"))
df.withColumn("month_name", F.date_format(col("date_col"), "MMMM"))

# Unix timestamp (epoch seconds)
df.withColumn("unix_ts", F.unix_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
df.withColumn("ts_from_unix", F.from_unixtime(col("unix_ts"), "yyyy-MM-dd HH:mm:ss"))

# Chuyển múi giờ
from pyspark.sql.functions import from_utc_timestamp, to_utc_timestamp
df.withColumn("local_ts", F.from_utc_timestamp(col("utc_ts"), "Asia/Ho_Chi_Minh"))
df.withColumn("utc_ts", F.to_utc_timestamp(col("local_ts"), "Asia/Ho_Chi_Minh"))

# Last day of month
df.withColumn("last_day", F.last_day(col("date_col")))

# Next day
df.withColumn("next_monday", F.next_day(col("date_col"), "Monday"))
```

---

### 4.5 Hàm xử lý Array (Array Functions)

```python
from pyspark.sql.functions import (
    array, array_contains, array_distinct, array_except,
    array_intersect, array_join, array_max, array_min,
    array_position, array_remove, array_sort, array_union,
    arrays_zip, explode, posexplode, flatten, slice, shuffle,
    size, sequence, transform, aggregate, filter as array_filter,
    zip_with, forall, exists, element_at
)

# Tạo array column
df.withColumn("arr", F.array(col("a"), col("b"), col("c")))
df.withColumn("arr_lit", F.array(F.lit(1), F.lit(2), F.lit(3)))

# Kích thước array
df.withColumn("arr_len", F.size(col("arr_col")))

# Kiểm tra
df.withColumn("has_a", F.array_contains(col("arr_col"), "a"))

# Lấy phần tử (1-indexed!)
df.withColumn("first_elem", F.element_at(col("arr_col"), 1))
df.withColumn("last_elem", F.element_at(col("arr_col"), -1))  # Đếm từ cuối

# Slice
df.withColumn("slice", F.slice(col("arr_col"), start=2, length=3))

# Min/Max trong array
df.withColumn("arr_max", F.array_max(col("arr_col")))
df.withColumn("arr_min", F.array_min(col("arr_col")))

# Sort array
df.withColumn("sorted", F.array_sort(col("arr_col")))

# Distinct elements
df.withColumn("unique", F.array_distinct(col("arr_col")))

# Set operations trên array
df.withColumn("union", F.array_union(col("arr1"), col("arr2")))
df.withColumn("intersect", F.array_intersect(col("arr1"), col("arr2")))
df.withColumn("diff", F.array_except(col("arr1"), col("arr2")))

# Xóa element
df.withColumn("removed", F.array_remove(col("arr_col"), "value_to_remove"))

# Join array thành string
df.withColumn("joined", F.array_join(col("arr_col"), delimiter=","))
df.withColumn("joined", F.array_join(col("arr_col"), ",", null_replacement="null"))

# Flatten (array of arrays → flat array)
df.withColumn("flat", F.flatten(col("nested_arr")))

# Shuffle
df.withColumn("shuffled", F.shuffle(col("arr_col")))

# Concatenate two arrays
df.withColumn("combined", F.concat(col("arr1"), col("arr2")))

# Position (1-indexed, 0 nếu không tìm thấy)
df.withColumn("pos", F.array_position(col("arr_col"), "target"))

# EXPLODE – "Nổ" array thành nhiều dòng
# Dữ liệu: [("a", [1, 2, 3]), ("b", [4, 5])]
df.withColumn("item", F.explode(col("items")))
# Kết quả: ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("b", 5)

# explode_outer (giữ dòng nếu array là null hoặc empty)
df.withColumn("item", F.explode_outer(col("items")))

# posexplode (kèm index)
df.select("id", F.posexplode(col("items")).alias("pos", "item"))
# Kết quả: ("a", 0, 1), ("a", 1, 2), ("a", 2, 3)

# Sequence (tạo array từ start đến stop)
df.withColumn("seq", F.sequence(F.lit(1), col("max_val")))
df.withColumn("seq", F.sequence(col("start"), col("end"), col("step")))

# Higher-order functions (Spark 2.4+)
df.withColumn("doubled", F.transform(col("numbers"), lambda x: x * 2))
df.withColumn("evens", F.filter(col("numbers"), lambda x: x % 2 == 0))
df.withColumn("sum", F.aggregate(col("numbers"), F.lit(0), lambda acc, x: acc + x))
df.withColumn("all_pos", F.forall(col("numbers"), lambda x: x > 0))
df.withColumn("has_neg", F.exists(col("numbers"), lambda x: x < 0))
df.withColumn("zipped", F.zip_with(col("arr1"), col("arr2"), lambda x, y: x + y))
df.withColumn("zipped_arr", F.arrays_zip(col("keys"), col("values")))
```

---

### 4.6 Hàm xử lý Map (Map / Struct Functions)

```python
from pyspark.sql.functions import (
    create_map, map_keys, map_values, map_contains_key,
    map_from_arrays, map_concat, map_entries, map_filter,
    map_from_entries, map_zip_with, struct, to_json, from_json,
    to_csv, schema_of_json
)

# Tạo Map column
df.withColumn("props", F.create_map(col("key"), col("value")))
df.withColumn("props", F.map_from_arrays(col("keys_array"), col("values_array")))

# Truy cập keys/values
df.withColumn("all_keys", F.map_keys(col("props")))
df.withColumn("all_values", F.map_values(col("props")))

# Kiểm tra key tồn tại
df.withColumn("has_name", F.map_contains_key(col("props"), "name"))

# Lấy giá trị theo key
df.withColumn("name_val", col("props")["name"])
df.withColumn("name_val", F.element_at(col("props"), "name"))

# Concat maps
df.withColumn("merged", F.map_concat(col("map1"), col("map2")))

# Explode map
df.select("id", F.explode(col("props")).alias("key", "value"))

# Tạo Struct column
df.withColumn("address", F.struct(col("city"), col("country"), col("zip")))

# Truy cập field trong struct
df.withColumn("city", col("address.city"))
df.withColumn("city", col("address")["city"])

# JSON handling
df.withColumn("json_str", F.to_json(col("struct_col")))
schema = "id INT, name STRING"  # Hoặc StructType
df.withColumn("parsed", F.from_json(col("json_str"), schema))
df.withColumn("inferred_schema", F.schema_of_json(F.lit('{"id": 1, "name": "Alice"}')))
```

---

### 4.7 Hàm Conditional (Điều kiện)

```python
# CASE WHEN THEN ELSE
df.withColumn("category",
    F.when(col("age") < 18, "Minor")
     .when(col("age") < 65, "Adult")
     .otherwise("Senior")
)

# Nested when
df.withColumn("label",
    F.when((col("score") >= 90) & (col("attend") == True), "A")
     .when(col("score") >= 80, "B")
     .when(col("score") >= 70, "C")
     .otherwise("F")
)

# IF (dùng when thay thế)
df.withColumn("is_adult", F.when(col("age") >= 18, True).otherwise(False))

# nullif (trả về null nếu a == b)
from pyspark.sql.functions import nullif
df.withColumn("result", F.nullif(col("value"), F.lit(0)))

# NVL / ifnull (trả về b nếu a là null)
df.withColumn("filled", F.coalesce(col("a"), col("b")))
df.withColumn("filled", F.nvl(col("a"), F.lit(0)))  # Spark 3.5+

# Greatest / Least (so sánh nhiều giá trị)
df.withColumn("max_val", F.greatest(col("a"), col("b"), col("c")))
df.withColumn("min_val", F.least(col("a"), col("b"), col("c")))
```

---

## 5. ĐỌC VÀ GHI DỮ LIỆU

### 5.1 Đọc CSV

```python
# Cơ bản
df = spark.read.csv("path/file.csv", header=True, inferSchema=True)

# Đầy đủ options
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .option("quote", '"') \
    .option("escape", "\\") \
    .option("multiLine", "true") \
    .option("encoding", "UTF-8") \
    .option("nullValue", "NULL") \
    .option("nanValue", "NaN") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("mode", "PERMISSIVE")  # PERMISSIVE | DROPMALFORMED | FAILFAST
    .csv("path/file.csv")

# Với schema tường minh
from pyspark.sql.types import *
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("amount", DoubleType())
])
df = spark.read.schema(schema).csv("path/", header=True)

# Nhiều file cùng lúc
df = spark.read.csv(["file1.csv", "file2.csv"], header=True, inferSchema=True)
df = spark.read.csv("path/to/dir/*.csv", header=True)  # Glob pattern
```

### 5.2 Đọc JSON

```python
df = spark.read.json("path/file.json")
df = spark.read.json("path/dir/")  # Mỗi file có thể là JSON array hoặc JSON lines

# Options
df = spark.read \
    .option("multiLine", "true")  # Nếu JSON là object nhiều dòng (không phải JSON lines)
    .option("allowComments", "true")
    .json("path/file.json")

# Với schema
df = spark.read.schema(schema).json("path/file.json")
```

### 5.3 Đọc Parquet

```python
# Đọc một file
df = spark.read.parquet("path/file.parquet")

# Đọc thư mục partitioned
df = spark.read.parquet("s3://bucket/data/transactions/")
# Tự động nhận biết partition columns (year=2026/month=07/)

# Đọc với filter (Partition Pruning tự động)
df = spark.read.parquet("path/").filter(col("year") == 2026)

# Đọc chỉ một số cột (Column Pruning)
df = spark.read.parquet("path/").select("id", "amount")
```

### 5.4 Đọc ORC

```python
df = spark.read.orc("path/file.orc")
```

### 5.5 Đọc từ Database (JDBC)

```python
# Đọc toàn bộ bảng
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/finance") \
    .option("dbtable", "transactions") \
    .option("user", "postgres") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Đọc với SQL query
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/finance") \
    .option("query", "SELECT * FROM transactions WHERE date > '2026-01-01'") \
    .option("user", "postgres") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Đọc song song (tối ưu performance)
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/finance") \
    .option("dbtable", "transactions") \
    .option("user", "postgres") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .option("numPartitions", "10") \
    .option("partitionColumn", "id") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .option("fetchsize", "10000") \
    .load()
```

### 5.6 Đọc Delta Lake

```python
df = spark.read.format("delta").load("path/to/delta/table")
df = spark.read.format("delta").option("versionAsOf", 5).load("path/")  # Time travel
df = spark.read.format("delta").option("timestampAsOf", "2026-01-01").load("path/")
```

### 5.7 Ghi dữ liệu

```python
# Ghi CSV
df.write.csv("output/path/", header=True, mode="overwrite")
df.write.mode("overwrite").option("header", "true").csv("output/path/")

# Ghi Parquet (khuyến nghị cho production)
df.write.parquet("output/path/")
df.write.mode("overwrite").parquet("output/path/")

# Ghi với partition
df.write.partitionBy("year", "month").parquet("output/path/")

# Mode options:
# "overwrite"  - Xóa và ghi lại
# "append"     - Thêm vào
# "ignore"     - Bỏ qua nếu đã tồn tại
# "error"      - Báo lỗi nếu đã tồn tại (mặc định)

# Ghi JSON
df.write.mode("overwrite").json("output/json/")

# Ghi vào Database (JDBC)
df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/finance") \
    .option("dbtable", "output_table") \
    .option("user", "postgres") \
    .option("password", "secret") \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", "10000") \
    .mode("append") \
    .save()

# Ghi Delta Lake
df.write.format("delta").mode("overwrite").partitionBy("year", "month").save("path/")

# MERGE (upsert) với Delta Lake
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "path/to/delta/")
delta_table.alias("target").merge(
    df_updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Kiểm soát số file đầu ra
df.repartition(1).write.csv("output/")       # Ghi vào 1 file duy nhất
df.coalesce(1).write.csv("output/")          # Tương tự nhưng hiệu quả hơn khi giảm
df.repartition(10).write.parquet("output/")  # Ghi vào 10 files
```

---

## 6. WINDOW FUNCTIONS (HÀM CỬA SỔ)

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank,
    lag, lead, first, last,
    sum, avg, min, max, count,
    ntile, percent_rank, cume_dist
)

# Định nghĩa Window Spec (cửa sổ)
w = Window.partitionBy("department").orderBy(desc("salary"))
w = Window.partitionBy("user_id", "month").orderBy("date")

# Ranking Functions
df.withColumn("row_num", row_number().over(w))     # Thứ tự tuần tự, không trùng
df.withColumn("rank", rank().over(w))               # Có nhảy bậc nếu trùng (1,2,2,4)
df.withColumn("dense_rank", dense_rank().over(w))  # Không nhảy bậc (1,2,2,3)

# LAG / LEAD (so sánh với dòng trước/sau)
w_ordered = Window.partitionBy("user_id").orderBy("date")
df.withColumn("prev_salary", lag(col("salary"), 1).over(w_ordered))   # Dòng trước
df.withColumn("next_salary", lead(col("salary"), 1).over(w_ordered))  # Dòng sau
df.withColumn("prev_salary_default", lag(col("salary"), 1, 0).over(w_ordered))  # Default 0 nếu null

# Tính biến động so với kỳ trước (MoM - Month over Month)
df.withColumn("salary_change", 
    col("salary") - lag(col("salary"), 1).over(w_ordered))

# Aggregation trên Window
# Mặc định: toàn bộ partition
df.withColumn("dept_total", sum("salary").over(Window.partitionBy("department")))
df.withColumn("dept_avg", avg("salary").over(Window.partitionBy("department")))
df.withColumn("dept_max", max("salary").over(Window.partitionBy("department")))
df.withColumn("dept_count", count("*").over(Window.partitionBy("department")))

# Running Total (Cumulative Sum) - tổng lũy kế
w_cumul = Window.partitionBy("user_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", sum("amount").over(w_cumul))

# 7-day Moving Average
w_7day = Window.partitionBy("user_id").orderBy("date_unix").rangeBetween(-6*86400, 0)  # 7 ngày theo giây
# Hoặc dùng rowsBetween
w_7day_rows = Window.partitionBy("user_id").orderBy("date").rowsBetween(-6, Window.currentRow)
df.withColumn("moving_avg_7d", avg("amount").over(w_7day_rows))

# Percent Rank / Ntile
w_dept = Window.partitionBy("department").orderBy("salary")
df.withColumn("pct_rank", percent_rank().over(w_dept))         # [0, 1]
df.withColumn("quartile", ntile(4).over(w_dept))               # 1, 2, 3, 4
df.withColumn("decile", ntile(10).over(w_dept))                # 1..10
df.withColumn("cume_dist", cume_dist().over(w_dept))           # Cumulative distribution

# First/Last trong partition
w_unbound = Window.partitionBy("department").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df.withColumn("first_in_dept", first("name").over(w_unbound))
df.withColumn("last_in_dept", last("name").over(w_unbound))

# RANGE BETWEEN vs ROWS BETWEEN
# ROWS BETWEEN: Đếm theo số dòng vật lý
w_rows = Window.orderBy("date").rowsBetween(-2, 0)   # 3 dòng gần nhất (hiện tại và 2 trước)
# RANGE BETWEEN: Dựa trên giá trị của ORDER BY column
w_range = Window.orderBy("amount").rangeBetween(-100, 100)  # Trong khoảng ±100 của amount

# Sử dụng Window để Top-N per group
from pyspark.sql.functions import row_number
w_top = Window.partitionBy("department").orderBy(desc("salary"))
top_3_per_dept = df.withColumn("rank", row_number().over(w_top)) \
                   .filter(col("rank") <= 3) \
                   .drop("rank")
```

---

## 7. XỬ LÝ KIỂU DỮ LIỆU NÂNG CAO

### 7.1 Schema Manipulation

```python
from pyspark.sql.types import *

# Tạo schema phức tạp
complex_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("scores", ArrayType(IntegerType()), nullable=True),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("zip", StringType()),
    ]), nullable=True),
    StructField("metadata", MapType(StringType(), StringType()), nullable=True),
])

# Kiểm tra kiểu dữ liệu
df.schema["salary"].dataType    # DoubleType()
isinstance(df.schema["salary"].dataType, DoubleType)  # True

# Đổi kiểu dữ liệu nhiều cột cùng lúc
from pyspark.sql.functions import col
cols_to_cast = ["price", "quantity", "discount"]
for c in cols_to_cast:
    df = df.withColumn(c, col(c).cast(DoubleType()))

# Tạo schema từ DDL string
schema = StructType.fromDDL("id INT, name STRING, amount DOUBLE")
```

### 7.2 Nested Data (JSON, Struct, Array phức tạp)

```python
# Parse JSON string thành struct
json_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("event", StringType()),
    StructField("properties", MapType(StringType(), StringType()))
])
df = df.withColumn("parsed", F.from_json(col("json_str"), json_schema))
df = df.withColumn("user_id", col("parsed.user_id"))

# Làm phẳng struct (expand all nested fields)
def flatten_struct(schema, prefix=""):
    fields = []
    for field in schema.fields:
        name = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            fields += flatten_struct(field.dataType, prefix=name)
        else:
            fields.append(col(name).alias(name.replace(".", "_")))
    return fields

df_flat = df.select(flatten_struct(df.schema))

# Struct to JSON và ngược lại
df.withColumn("json", F.to_json(col("struct_col")))
df.withColumn("struct", F.from_json(col("json_col"), schema))
```

---

## 8. UDF (USER DEFINED FUNCTIONS)

### 8.1 Python UDF thông thường (Row-by-row)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Khai báo UDF
@udf(returnType=StringType())
def categorize_age(age):
    if age is None:
        return "Unknown"
    elif age < 18:
        return "Minor"
    elif age < 65:
        return "Adult"
    else:
        return "Senior"

# Sử dụng
df.withColumn("age_cat", categorize_age(col("age")))

# Cách khác không dùng decorator
def parse_phone(phone):
    if phone is None:
        return None
    import re
    cleaned = re.sub(r"[^0-9]", "", phone)
    return cleaned if len(cleaned) == 10 else None

parse_phone_udf = udf(parse_phone, StringType())
df.withColumn("phone_clean", parse_phone_udf(col("phone")))

# Đăng ký UDF để dùng trong SQL
spark.udf.register("categorize_age_sql", categorize_age)
spark.sql("SELECT id, categorize_age_sql(age) AS age_cat FROM my_table")
```

### 8.2 Pandas UDF (Vectorized - khuyến nghị thay Python UDF)

```python
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

# Scalar Pandas UDF (1 Series → 1 Series)
@pandas_udf(StringType())
def normalize_name(series: pd.Series) -> pd.Series:
    return series.str.strip().str.title()

df.withColumn("normalized", normalize_name(col("name")))

# Scalar UDF với nhiều đầu vào
@pandas_udf(DoubleType())
def weighted_score(score: pd.Series, weight: pd.Series) -> pd.Series:
    return score * weight / weight.sum()

df.withColumn("w_score", weighted_score(col("score"), col("weight")))

# Grouped Map Pandas UDF (group → DataFrame → DataFrame)
# Thường dùng để apply thuật toán ML trên từng nhóm
from pyspark.sql.functions import PandasUDFType

schema = StructType([
    StructField("department", StringType()),
    StructField("salary", DoubleType()),
    StructField("normalized_salary", DoubleType()),
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def normalize_salary_per_dept(df: pd.DataFrame) -> pd.DataFrame:
    min_s = df["salary"].min()
    max_s = df["salary"].max()
    df["normalized_salary"] = (df["salary"] - min_s) / (max_s - min_s + 1e-8)
    return df

result = df.groupby("department").apply(normalize_salary_per_dept)
```

---

## 9. STRUCTURED STREAMING (CƠ BẢN)

```python
# Đọc từ Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON message từ Kafka
schema = "id INT, amount DOUBLE, ts TIMESTAMP"
df_parsed = df_stream.select(
    F.from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Đọc từ thư mục (file stream)
df_stream = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load("path/to/incoming/dir/")

# Aggregation trên stream
windowed_agg = df_parsed \
    .withWatermark("ts", "10 minutes") \
    .groupBy(
        F.window(col("ts"), "5 minutes", "1 minute"),
        col("user_id")
    ) \
    .agg(F.sum("amount").alias("total"))

# Output
query = windowed_agg.writeStream \
    .outputMode("update") \     # append | complete | update
    .format("console") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "hdfs:///checkpoints/stream1/") \
    .start()

query.awaitTermination()
query.stop()
```

---

## 10. ETL PATTERNS THỰC TẾ

### 10.1 Pattern: Incremental Load (Nạp gia tăng)
```python
from pyspark.sql.functions import col, max, lit
import datetime

def incremental_load(spark, source_table, target_path, watermark_col="updated_at"):
    # Đọc watermark hiện tại (dòng mới nhất đã load)
    try:
        last_loaded = spark.read.parquet(target_path).agg(max(col(watermark_col))).first()[0]
    except:
        last_loaded = datetime.datetime(2000, 1, 1)
    
    print(f"Loading data since: {last_loaded}")
    
    # Đọc dữ liệu mới
    df_new = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://host:5432/db") \
        .option("query", f"SELECT * FROM {source_table} WHERE {watermark_col} > '{last_loaded}'") \
        .option("user", "user") \
        .option("password", "pass") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    if df_new.count() == 0:
        print("No new data to load.")
        return
    
    # Append vào target
    df_new.write.mode("append").parquet(target_path)
    print(f"Loaded {df_new.count()} new records.")
```

### 10.2 Pattern: SCD Type 2 (Slowly Changing Dimension)
```python
from pyspark.sql.functions import col, lit, current_date, sha2, concat_ws
from delta.tables import DeltaTable

def apply_scd2(spark, source_df, delta_path, natural_key, track_cols):
    """Cập nhật SCD Type 2 vào Delta Table"""
    
    # Thêm hash để phát hiện thay đổi
    source_df = source_df \
        .withColumn("row_hash", sha2(concat_ws("||", *[col(c) for c in track_cols]), 256)) \
        .withColumn("eff_start_date", current_date()) \
        .withColumn("eff_end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    delta_table = DeltaTable.forPath(spark, delta_path)
    
    delta_table.alias("target").merge(
        source_df.alias("source"),
        f"target.{natural_key} = source.{natural_key} AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.row_hash != source.row_hash",
        set={"eff_end_date": "current_date()", "is_current": "false"}
    ).execute()
    
    # Insert rows đã thay đổi và rows mới
    new_rows = source_df.join(
        delta_table.toDF().filter(col("is_current") == True),
        [natural_key], "left_anti"
    )
    new_rows.write.format("delta").mode("append").save(delta_path)
```

### 10.3 Pattern: Data Quality Check
```python
from pyspark.sql.functions import col, count, sum as spark_sum, when

def data_quality_check(df, table_name):
    total = df.count()
    print(f"\n=== Data Quality Report: {table_name} ===")
    print(f"Total rows: {total}")
    
    # 1. Null counts
    null_df = df.select([
        spark_sum(col(c).isNull().cast("int")).alias(c) 
        for c in df.columns
    ])
    print("\nNull counts:")
    null_df.show()
    
    # 2. Duplicate check
    dup_count = total - df.dropDuplicates().count()
    print(f"Duplicate rows: {dup_count}")
    
    # 3. Numeric column stats
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, LongType, FloatType))]
    if numeric_cols:
        print("\nNumeric stats:")
        df.select(numeric_cols).summary().show()
    
    return null_df

# Sử dụng
data_quality_check(df_transactions, "transactions")
```

### 10.4 Pattern: Repartition Tối ưu trước khi ghi
```python
# Tối ưu số lượng partitions trước khi ghi file để tránh nhiều file nhỏ
# Quy tắc: Mỗi partition/file nên khoảng 128MB - 256MB

def optimized_write(df, output_path, partition_cols=None, target_partition_size_mb=128):
    # Ước tính kích thước dữ liệu
    estimated_size_mb = df.count() * len(df.columns) * 8 / (1024 * 1024)
    num_partitions = max(1, int(estimated_size_mb / target_partition_size_mb))
    
    print(f"Estimated size: ~{estimated_size_mb:.1f}MB, writing to {num_partitions} partition files")
    
    df_opt = df.repartition(num_partitions) if partition_cols is None \
               else df.repartition(num_partitions, *[col(c) for c in partition_cols])
    
    writer = df_opt.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.parquet(output_path)
```

---

## QUICK REFERENCE CARD

### Các hàm hay dùng nhất trong ETL
| Công việc | Hàm |
| :--- | :--- |
| Lọc null | `col("x").isNull()`, `col("x").isNotNull()`, `na.drop()` |
| Điền null | `na.fill(0)`, `coalesce(col("a"), lit(0))` |
| Đổi kiểu | `col("x").cast("int")`, `.cast(IntegerType())` |
| Rename | `withColumnRenamed("old", "new")`, `.alias("new")` |
| Thêm cột | `withColumn("new_col", expr)` |
| Xóa cột | `drop("col1", "col2")` |
| Lọc dòng | `filter(col("x") > 0)` |
| Gom nhóm | `groupBy("dept").agg(F.sum("sal"))` |
| Join | `df1.join(df2, "id", "left")` |
| Sort | `orderBy(desc("salary"))` |
| Unique | `distinct()`, `dropDuplicates(["id"])` |
| Top N/group | `row_number().over(Window.partitionBy("g").orderBy(desc("s")))` |
| Running sum | `sum("x").over(Window...rowsBetween(unboundedPreceding, currentRow))` |
| JSON parse | `from_json(col("s"), schema)` |
| Regex extract | `regexp_extract(col("s"), pattern, groupIndex)` |
| Date convert | `to_date(col("s"), "yyyy-MM-dd")` |
| Cắt string | `substring(col("s"), 1, 3)` |
| Nối string | `concat_ws("-", col("a"), col("b"))` |
| Array to rows | `explode(col("arr"))` |
| Row to array | `collect_list("x")`, `collect_set("x")` |
| Broadcast join | `df1.join(broadcast(df_small), "id")` |
| Cache | `df.cache()`, `df.unpersist()` |
| Count rows | `df.count()` |
| Show data | `df.show(20, truncate=False)` |
| Print schema | `df.printSchema()` |
| Write Parquet | `df.write.mode("overwrite").partitionBy("year").parquet("path/")` |
| Read Parquet | `spark.read.parquet("path/")` |
| Read JDBC | `spark.read.format("jdbc").option("url",...).option("dbtable",...).load()` |
