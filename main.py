from flask import Flask, request
import pandas as pd
import sqlite3
import os

app = Flask(__name__)


def create_inerg_db():
     df = pd.read_excel("production.xls")
     df = df.rename(columns={"API WELL  NUMBER": "well_number", "OIL":"oil", "GAS":"gas", "BRINE":"brine"})
     df = df.groupby(["well_number"]).sum()
     final_df = df[["oil", "gas", "brine"]]
     connection = sqlite3.connect("inerg.db")
     final_df.to_sql("data", connection, if_exists='replace')
     connection.close()


def get_db_connection():
     connection = sqlite3.connect("inerg.db")
     return connection


@app.route("/")
def index():
     return "specify a well number to get 2020 production"



@app.route("/data", methods=["GET"])
def well():
     args = request.args
     well_number = args.get("well")
     connection = get_db_connection()
     cur = connection.cursor()
     cur.execute("SELECT * FROM cur WHERE well_number = {well_number}")
     rows = cur.fetchall()
     well_number, year, oil, gas, brine = rows[8]
     return {"oil":oil, "gas":gas, "brine":brine}



if __name__=="__main__":
     app.run(host="127.0.0.1", port=8080, debug=True)