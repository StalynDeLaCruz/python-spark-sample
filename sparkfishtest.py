
from pyspark.sql import SparkSession
from pyspark.sql.functions import percentile_approx,avg
from pgpy import PGPKey as pgk, PGPMessage as pgm

# Get keys
private_key = 'slim.shady.sec.asc'
file_to_decrypt = 'titanic.csv.gpg'

# Decrypt titatinic.csv.gpg file
pkey, _ = pgk.from_file(str(private_key))
to_decrypt = pgm.from_file(file_to_decrypt)
decrypted_file = pkey.decrypt(to_decrypt).message
with open('titanci.csv', 'wb') as csvfile:
    csvfile.write(decrypted_file)

# Start Spark session
spark = SparkSession.builder.appName('sparkfish').getOrCreate()
spark: SparkSession = spark

# Create dataframe
df_titanic = spark.read.csv('titanci.csv', header='True')

df_titanic.show(5)

# Caculate Age and 75 percentil
df_titanic.select(avg("Age").alias("Average Age")
                  , percentile_approx("Age",[0.75],100).alias("75 Age Percentil")
                  ).show()

# Save file in parquet format
f_name = 'titatic'
f_extension = 'parquet'

df_titanic.write.mode('overwrite').option('header',True).parquet(f'{f_name}.{f_extension}')
