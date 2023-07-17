# Тема работы

Сбор, предобработка и анализ данных о работе сервиса по доставке продуктов.

## 1.1 Информация о выбранном наборе данных

Данные о доставке продуктов различных компаний были взяты с сайта [karpov courses](https://lab.karpov.courses)

![image](https://github.com/Fourzeroo/PySpark_project/assets/92236009/028ea826-a425-496b-b208-a0e2e16198c1)

## 1.2 Архитектура конвейера для получения и предобработки данных 

Схема конвейера представлена ниже:

![image](https://github.com/Fourzeroo/PySpark_project/assets/92236009/05398d68-89ed-4b84-a4e8-019d73514133)

Для начала мы очищаем немного данные и приводим их к нормальному виду для того, чтобы в дальнейшем данные могли хранится в таблицах Hive. В очистку данных входит преобразование времени к стандарту ISO 8601 и удаление квадратных скобок в колонке product_ids и т.д.


```python
courier_actions['time'] = pd.to_datetime(courier_actions['time'], format='%d/%m/%y %H:%M')
couriers.head(2)

courier_id 	birth_date 	sex
0 	1 	1981-06-11 	female
1 	2 	1991-06-27 	male


couriers['birth_date'] = pd.to_datetime(couriers['birth_date'], format='%d/%m/%y')
couriers.head(2)

	courier_id 	birth_date 	sex
0 	1 	1981-06-11 	female
1 	2 	1991-06-27 	male
```

```python
# до
orders = pd.read_csv('/home/student/Downloads/orders.csv')
orders.head(2)

order_id 	creation_time 	product_ids
0 	1 	24/08/22 01:52 	[65, 28]
1 	2 	24/08/22 06:37 	[35, 30, 42, 34]

# После
orders['creation_time'] = pd.to_datetime(orders['creation_time'], format='%d/%m/%y %H:%M')
orders['product_ids'] = orders['product_ids'].str.replace(r'[\[\],]', ' ', regex=True)
orders.tail(2)

	order_id 	creation_time 	product_ids
59593 	59594 	2022-09-08 23:59:00 	2 62
59594 	59595 	2022-09-08 23:59:00 	18 30 67
```

```python
user_actions = pd.read_csv('/home/student/Downloads/user_actions.csv')

user_actions.tail(2)

	user_id 	order_id 	action 	time
62572 	10881 	59594 	create_order 	08/09/22 23:59
62573 	19774 	59595 	create_order 	08/09/22 23:59

user_actions['time'] = pd.to_datetime(user_actions['time'], format='%d/%m/%y %H:%M')
user_actions.tail(2)

user_id 	order_id 	action 	time
62572 	10881 	59594 	create_order 	2022-09-08 23:59:00
62573 	19774 	59595 	create_order 	2022-09-08 23:59:00
```

```python
users = pd.read_csv('/home/student/Downloads/users.csv')

users.tail(2)

	user_id 	birth_date 	sex
20329 	338 	07/08/90 	female
20330 	17998 	05/07/92 	male

users['birth_date'] = pd.to_datetime(users['birth_date'], format='%d/%m/%y' )

users.tail(2)

	user_id 	birth_date 	sex
20329 	338 	1990-08-07 	female
20330 	17998 	1992-07-05 	male
```

```python
users.to_csv(
    '/home/student/Data/users.csv', 
    index=False, 
    )

user_actions.to_csv(
    '/home/student/Data/user_actions.csv', 
    index=False, 
    )

products.to_csv(
    '/home/student/Data/products.csv', 
    index=False, 
    )

orders.to_csv(
    '/home/student/Data/orders.csv', 
    index=False, 
    )

couriers.to_csv(
    '/home/student/Data/couriers.csv', 
    index=False, 
    )

courier_actions.to_csv(
    '/home/student/Data/courier_actions.csv', 
    index=False, 
    )
```


После предобработки данные можно помещать в локальное хранилище HDFS

```bash
[student@localhost ~]$ hdfs dfs -mkdir /user/student/kursovaya
[student@localhost ~]$ hdfs dfs -put /home/student/Data/orders.csv /user/student/kursovaya
[student@localhost ~]$ hdfs dfs -put /home/student/Data/couriers.csv /user/student/kursovaya
[student@localhost ~]$ hdfs dfs -put /home/student/Data/courier_actions.csv /user/student/kursovaya
[student@localhost ~]$ hdfs dfs -put /home/student/Data/users.csv /user/student/kursovaya
[student@localhost ~]$ hdfs dfs -put /home/student/Data/user_actions.csv /user/student/kursovaya
[student@localhost ~]$ hdfs dfs -put /home/student/Data/products.csv /user/student/kursovaya
```

Далее создадим таблицы в Hive для дальнейшего транспортирования наших файлов в HDFS в таблицы Hive. Все это будет выполнено в веб-приложении HUE для просмотра хранилища, связанного с Hadoop кластером, запуска Hive заданий, Pig скриптов и т.д.

```sql

-- создание таблицы courier_actions 
CREATE TABLE courier_actions (
  courier_id INT,
  order_id INT,
  action VARCHAR(50),
  deliver_time TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- создание таблицы couriers 
CREATE TABLE couriers (
  courier_id INT,
  birth_date DATE,
  sex VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- создание таблицы orders 
CREATE TABLE orders (
  order_id INT,
  creation_time TIMESTAMP,
  product_ids ARRAY<INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ' '
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- создание таблицы products 
CREATE TABLE products (
  product_id INT,
  name VARCHAR(70),
  price DECIMAL(10, 2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- создание таблицы user_actions 
CREATE TABLE user_actions (
  user_id INT,
  order_id INT,
  action VARCHAR(50),
  deliver_time TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- создание таблицы users 
CREATE TABLE users (
  user_id INT,
  birth_date DATE,
  sex VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");
```

После создания таблиц мы переместим туда наши данные из HDFS:
```sql
LOAD DATA INPATH '/user/student/kursovaya/orders.csv' INTO TABLE kursdb.orders;
LOAD DATA INPATH '/user/student/kursovaya/couriers.csv' INTO TABLE kursdb.orders;
LOAD DATA INPATH '/user/student/kursovaya/courier_actions.csv' INTO TABLE kursdb.orders;
LOAD DATA INPATH '/user/student/kursovaya/users.csv' INTO TABLE kursdb.orders;
LOAD DATA INPATH '/user/student/kursovaya/user_actions.csv' INTO TABLE kursdb.orders;
LOAD DATA INPATH '/user/student/kursovaya/products.csv' INTO TABLE kursdb.orders;
```


**После этого данные идут по двум путям:**

1.	Данные идут в PySpark с помощью SparkSQL для дальнейшего анализа

```python
# создание SparkSession с поддержкой Hive
spark = SparkSession \
    .builder \
    .appName("Spark Hive") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.hive.metastore.jars.path", "/usr/local/hive/hive-3.1.2/conf") \
    .enableHiveSupport() \
    .getOrCreate()
```

2.	Рассчитанные метрики и другие важные показатели при помощи языка запросов HiveQL и с помощью Sqoop поступают в уже заготовленные таблицы в MariaDB

 * Расчет динамика прибыли 

```bash
hive -e "WITH order_items AS (
  SELECT
    orders.order_id,
    product_id,
    creation_time
  FROM
    orders LATERAL VIEW explode(product_ids) product_id_table AS product_id
  WHERE
    orders.order_id NOT IN (
      SELECT
        order_id
      from
        courier_actions
      WHERE
        action = 'cancel_order'
    )
),
daily_revenue AS (
  SELECT
    from_unixtime(
      unix_timestamp(orders.creation_time),
      'yyyy-MM-dd'
    ) AS order_date,
    SUM(products.price) AS revenue
  FROM
    order_items
    JOIN orders ON order_items.order_id = orders.order_id
    CROSS JOIN products
  WHERE
    order_items.product_id = products.product_id
  GROUP BY
    from_unixtime(
      unix_timestamp(orders.creation_time),
      'yyyy-MM-dd'
    )
)
SELECT
  order_date,
  revenue,
  SUM(revenue) OVER (
    ORDER BY
      order_date
  ) AS total_revenue,
  ROUND(
    (revenue - LAG(revenue) OVER ( ORDER BY order_date))
 / LAG(revenue) OVER (
      ORDER BY
        order_date
    ) * 100,
    2
  ) AS revenue_change
FROM
  daily_revenue
ORDER BY
  order_date;" > /user/student/kursovaya/query_result_revenue.csv


sqoop export \
--connect "jdbc:mysql://localhost/vova" \
--username student \
--password student \
--table revenue \
--export-dir /user/student/kursovaya/query_result_revenue.csv
--input-fields-terminated-by ',' 
--lines-terminated-by '\n'

```
---
 * Расчет метрик:
    * ARPU (Average Revenue Per User) — средняя выручка на одного пользователя за определённый период.

    * ARPPU (Average Revenue Per Paying User) — средняя выручка на одного платящего пользователя за определённый 
    период.

    * AOV (Average Order Value) — средний чек, или отношение выручки за определённый период к общему количеству 
    заказов за это же время.

```bash
hive -e "
    SELECT
      t1.deliver_date, 
      ROUND(CAST(revenue AS double) / users, 2) AS arpu,
      ROUND(CAST(revenue AS double) / paying_users, 2) AS arppu,
      ROUND(CAST(revenue AS double) / orders, 2) AS aov
    FROM (
      SELECT
        t4.deliver_date,
        COUNT(DISTINCT t4.order_id) AS orders,
        SUM(t4.price) AS revenue
      FROM (
        SELECT
          o.order_id,
          CAST(o.creation_time AS DATE) AS deliver_date,
          p.price
        FROM
          orders o
          JOIN (
            SELECT
              order_id,
              CAST(creation_time AS DATE) AS creation_date,
              product_id
            FROM
              orders LATERAL VIEW explode(product_ids) exploded AS product_id
          ) o_exploded ON o.order_id = o_exploded.order_id
          JOIN products p ON o_exploded.product_id = p.product_id
        WHERE
          o.order_id NOT IN (
            SELECT
              order_id
            FROM
              user_actions
            WHERE
              action = 'cancel_order'
          )
      ) t4
      GROUP BY
        t4.deliver_date
    ) t1
    LEFT JOIN (
      SELECT
        CAST(ua.deliver_time AS DATE) AS deliver_date,
        COUNT(DISTINCT ua.user_id) AS users
      FROM
        user_actions ua  
      GROUP BY
        CAST(ua.deliver_time AS DATE)
    ) t2 ON t1.deliver_date = t2.deliver_date 
    LEFT JOIN (
      SELECT
        CAST(ua_filtered.deliver_time AS DATE) AS deliver_date,
        COUNT(DISTINCT ua_filtered.user_id) AS paying_users  
      FROM (
        SELECT
          ua.deliver_time,
          ua.user_id,
          ua.order_id  
        FROM
          user_actions ua 
          LEFT JOIN (
            SELECT
              order_id
            FROM
              user_actions 
            WHERE
              action = 'cancel_order'
          ) ua_cancel ON ua.order_id = ua_cancel.order_id 
        WHERE
          ua_cancel.order_id IS NULL 
      ) ua_filtered 
      GROUP BY
        CAST(ua_filtered.deliver_time AS DATE)
    ) t3 ON t1.deliver_date = t3.deliver_date 
    ORDER BY
      t1.deliver_date;" > /user/student/kursovaya/query_result_metrix.csv


sqoop export \
--connect "jdbc:mysql://localhost/vova" \
--username student \
--password student \
--table metrix \
--export-dir /user/student/kursovaya/query_result_metrix.csv
--input-fields-terminated-by ',' 
--lines-terminated-by '\n'

```
---
 # **Дальнейший анализ и интерпретация результатов представлена в jupyter notebook**
