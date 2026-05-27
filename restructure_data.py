from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# Start a Spark session
spark = SparkSession.builder.appName("JobPostingsAnalysis").getOrCreate()

# Load the CSV file
df = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine","true").option("escape", "\"").csv("/home/ubuntu/Lab1/data/lightcast_job_postings.csv")

# Table 1: Job_Postings
job_postings_df = df.select(
    col("ID"), col("TITLE_RAW"), col("TITLE_CLEAN"),
    col("POSTED"), col("EXPIRED"),
    col("SALARY_FROM"), col("SALARY_TO"),
    col("MIN_YEARS_EXPERIENCE"), col("MAX_YEARS_EXPERIENCE"),
    col("SKILLS_NAME").alias("SKILLS"),
    col("SPECIALIZED_SKILLS_NAME").alias("SPECIALIZED_SKILLS"),
    col("SOFTWARE_SKILLS_NAME").alias("SOFTWARE_SKILLS"),
    col("EMPLOYMENT_TYPE_NAME").alias("EMPLOYMENT_TYPE"),
    col("COMPANY_NAME").alias("COMPANY_ID")
)

# Table 2: Company
company_df = df.select(
    col("COMPANY_NAME"), col("COMPANY_RAW"), col("COMPANY_IS_STAFFING")
).distinct().withColumn("COMPANY_ID", monotonically_increasing_id())

# Table 3: Job_Location
job_location_df = df.select(
    col("ID"), col("CITY_NAME").alias("CITY"),
    col("STATE_NAME").alias("STATE"),
    col("COUNTY_NAME").alias("COUNTY"),
    col("LOCATION")
)

# Table 4: SOC_Details
soc_details_df = df.select(
    col("ID"), col("SOC_2"), col("SOC_2_NAME"),
    col("SOC_3"), col("SOC_3_NAME"),
    col("SOC_4"), col("SOC_4_NAME"),
    col("SOC_5"), col("SOC_5_NAME")
)

# Table 5: LOT_Details
lot_details_df = df.select(
    col("ID"), col("LOT_CAREER_AREA"), col("LOT_CAREER_AREA_NAME"),
    col("LOT_OCCUPATION"), col("LOT_OCCUPATION_NAME"),
    col("LOT_SPECIALIZED_OCCUPATION"), col("LOT_SPECIALIZED_OCCUPATION_NAME")
)

# Table 6: NAICS_Details
naics_details_df = df.select(
    col("ID"),
    col("NAICS_2022_2").alias("NAICS2"), col("NAICS_2022_2_NAME").alias("NAICS2_NAME"),
    col("NAICS_2022_3").alias("NAICS3"), col("NAICS_2022_3_NAME").alias("NAICS3_NAME"),
    col("NAICS_2022_4").alias("NAICS4"), col("NAICS_2022_4_NAME").alias("NAICS4_NAME"),
    col("NAICS_2022_5").alias("NAICS5"), col("NAICS_2022_5_NAME").alias("NAICS5_NAME"),
    col("NAICS_2022_6").alias("NAICS6"), col("NAICS_2022_6_NAME").alias("NAICS6_NAME")
)

# Save all tables
output = "/home/ubuntu/Lab1/_output"

job_postings_df.coalesce(1).write.option("header","true").mode("overwrite").csv(f"{output}/job_postings")
company_df.coalesce(1).write.option("header","true").mode("overwrite").csv(f"{output}/company")
job_location_df.coalesce(1).write.option("header","true").mode("overwrite").csv(f"{output}/job_location")
soc_details_df.coalesce(1).write.option("header","true").mode("overwrite").csv(f"{output}/soc_details")
lot_details_df.coalesce(1).write.option("header","true").mode("overwrite").csv(f"{output}/lot_details")
naics_details_df.coalesce(1).write.option("header","true").mode("overwrite").csv(f"{output}/naics_details")

print("All tables saved successfully!")
spark.stop()