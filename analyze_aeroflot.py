from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, current_date, datediff, expr

# Создание SparkSession
spark = SparkSession.builder 
    .appName("Aeroflot Stock Analysis") 
    .getOrCreate()

# Загрузка данных
file_path = "AFLT.csv"  # Убедитесь, что указали правильный путь к файлу
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Выводим схему данных для понимания структуры
data.printSchema()
data.show(5)  # Показываем первые 5 строк данных

# Фильтрация данных за последние 2 года
today_date = current_date()
two_years_ago = expr('add_months(current_date(), -24)')
filtered_data = data.filter(col("Дата") >= two_years_ago)

# Приведение столбца к типу Float для вычислений
filtered_data = filtered_data.withColumn("Цена", col("Цена").cast("float"))

# Расчет максимальной цены закрытия
max_price = filtered_data.agg(max("Цена")).collect()[0][0]
print(f"Максимальная цена закрытия за последние 2 года: {max_price}")

# Подсчет тренда (например, кол-во дней роста и падения)
daily_trend = filtered_data.withColumn("Изм.", col("Цена").cast("float") - col("Откр.").cast("float"))

increasing_days = daily_trend.filter(col("Изм.") > 0).count()
decreasing_days = daily_trend.filter(col("Изм.") < 0).count()

print(f"Количество дней роста: {increasing_days}")
print(f"Количество дней падения: {decreasing_days}")

# Остановка Spark
spark.stop()
