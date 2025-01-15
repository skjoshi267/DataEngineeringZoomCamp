# Import libraries
import pandas as pd
from sqlalchemy import create_engine

#
engine = create_engine("postgresql://admin:admin@localhost:5432/nyc_taxi")

#
connect = engine.connect()

#
df_itr = pd.read_csv(r"yellow_tripdata_2019-01.csv",chunksize=100000,iterator=True)

#
data = next(df_itr)

#
data.info()

#
data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"])
data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"])

#
print(pd.io.sql.get_schema(data,name="yellow_tripdata_2019_01",con=engine))

#
data.head(0).to_sql("yellow_tripdata_2019-01",con=connect,if_exists="replace",index=False)

#
data.to_sql("yellow_tripdata_2019-01",con=connect,if_exists="append",index=False)


