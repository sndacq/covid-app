# Covid API

The app uses Flask and Python 3.
Also uses Postgresql as database

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install requirements.

```bash
pip install -r requirements.txt
```

## Configuration
Please update config.py according to your postgresql setup


## Usage

To run the app

```bash
export FLASK_APP=app.py
python -m flask run
 * Running on http://127.0.0.1:5000/
```

Access the app through http://127.0.0.1:5000/ to load/parse the csv file and save it to database.

perform a GET request to the API through:

http://127.0.0.1:5000/top/confirmed?observation_date=2020-01-22&max_results=2

using observation_date and max_results as parameters