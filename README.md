# Hive Connector

This repository contains a Python code to set up a Python connection to a Hive server and execute SQL queries.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the required dependencies.

```bash
pip install -r requirements.txt
```

## Usage

```python
import pandas as pd
from connect2hive import Hive

def main():
    
    host  = 'test.direction.com'    % String containing the URL hosting your datanase (use "localhost" for databases running in your own system)
    port  = 10000                   % Integer containing the port in which your database is running
    db    = 'db_test'               % String containing your database name
    table = 'tb_test'               % String containing your table name

    # Define your SQL query

    query = "SELECT * FROM " + table

    # Connect to Hive server

    hiv = Hive(host, port, db)

    # Execute the required query
       
    hiv.executequery(query)

    # Retrieve query into Pandas dataframe
       
    df = pd.DataFrame(hiv.fetchall())

    return

if __name__ == __main__:

    main()

```

## Authorship
This repository has been develop by Andrés Muñoz. Alejandro Güemes has collaborated in the set up of the repository.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)

