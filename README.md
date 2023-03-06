# snowflake-service
Minimalist Snowflake Service With Stored Credentials

## dependencies 
run the following pip commands 
```
pip install "snowflake-connector-python[secure-local-storage,pandas]"
pip install snowflake.sqlalchemy
pip install sqlalchemy
```

## setup
Modify the file credentials.json to add your credentials adn then run in the local directory:
```
python setup.py install
```

## usage 
```python 
import pandas as pd
from service.snowflake_service import SnowflakeService

snowflake = SnowflakeService()
snowflake.connect()

dummy = {'Team Member': ['Dinosaure', 'Jack Black', 'Panda', 'Origano', 'Lethal', 'Hammer'],
         'Is permanent employee': [1, 0, 1, 1, 1, 1]}
df = pd.DataFrame(dummy)

# creates a table based on df definition and dtypes
snowflake.create_table(df, '<DB>', '<SCHEMA>', '<TABLE_NAME>')

# pushes df contents to a table 
snowflake.push_to_snowflake(df, '<DB>', '<SCHEMA>', '<TABLE_NAME>')

# query data & returns df
snowflake.query('SELECT * FROM db.schema.my_table')
```