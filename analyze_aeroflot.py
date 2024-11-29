from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, year, date_format
from datetime import datetime, timedelta

# Создание SparkSession
spark = SparkSession.builder 
    .appName("AFLT Data Analysis") 
    .getOrCreate()

# Загрузка данных
df = spark.read.option("header", "true").csv("AFLT_data.csv")

# Преобразование столбца "Date" в тип данных Date
df = df.withColumn("Date", col("Date").cast("date"))

# Установка текущей даты и даты два года назад
now = datetime.now()
two_years_ago = now - timedelta(days=730)

# Фильтрация данных за последние 2 года
filtered_data = df.filter(col("Date") >= two_years_ago)

# Расчет максимальной цены закрытия
max_close = filtered_data.agg(max("Close")).first()[0]
print(f"Максимальная цена закрытия за последние 2 года: {max_close}")

# Тренд анализа по месяцам
# Добавление года и месяца в новую колонку
trend_analysis = filtered_data.withColumn("YearMonth", date_format(col("Date"), "yyyy-MM"))

# Получение максимальной и минимальной цены закрытия по месяцам
trend_result = trend_analysis.groupBy("YearMonth").agg(
    max("Close").alias("Max_Close"),
    min("Close").alias("Min_Close")
)

# Показ результатов
trend_result.show()

# Завершение работы
spark.stop()
