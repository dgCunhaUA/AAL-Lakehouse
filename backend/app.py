from copy import deepcopy
from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
import delta_sharing
from IPython.display import display
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

app = Flask(__name__)

PROFILE_FILE = "./docker-datasets.share"
HR_DATA = "#grafana.dashboard.hr_data"
BREATHING_DATA = "#grafana.dashboard.breathing_data"
RRI_DATA = "#grafana.dashboard.rri_histogram_data"
PSD_FREQ_DATA = "#grafana.dashboard.psd_frequencies_data"
SESSION_DATA = "#grafana.dashboard.sessions_data"
HEARTPY_MEASURES = "#grafana.dashboard.heartpy_measures"


# Create a SharingClient.
client = delta_sharing.SharingClient(PROFILE_FILE)

@app.route("/")
def alive():
    print(client.list_all_tables())
    return jsonify(success=True)


@app.route("/subjects")
def subjects():
    table_url = PROFILE_FILE + SESSION_DATA

    df = delta_sharing.load_as_pandas(table_url)
    subjects_list = df["subject_uuid"].unique()

    return jsonify(subjects_list.tolist())

""" @app.route("/subject/<subject_uuid>/hr")
def get_hr_by_uuid(subject_uuid):

    table_url = PROFILE_FILE + HR_DATA

    pdf = delta_sharing.load_as_pandas(table_url)
    pdf = pdf.loc[pdf["subject_uuid"] == subject_uuid]

    data = []
    for i, row in pdf.iterrows():
        start_timestamp = row["start_timestamp"]
        end_timestamp = row["end_timestamp"]
        hr_list = row["hr"]

        if hr_list is not None:
            duration = end_timestamp - start_timestamp
            time_interval = duration / len(hr_list)

            timestamp = start_timestamp
            for hr in hr_list:
                data.append(
                    {
                        "timestamp": timestamp.isoformat(
                            timespec="microseconds", sep=" "
                        ),
                        "hr": hr,
                    }
                )
                timestamp += time_interval

    return jsonify(data) """

""" @app.route("/subject/<subject_uuid>/hr/<time_from>/<time_to>")
def get_hr_by_uuid(subject_uuid, time_from, time_to):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HR_DATA
    pdf = delta_sharing.load_as_pandas(table_url)
    pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]

    data = []
    for i, row in pdf.iterrows():
        start_timestamp = row["start_timestamp"].to_pydatetime()
        end_timestamp = row["end_timestamp"].to_pydatetime()
        hr_list = row["hr"]

        if hr_list is not None :
            duration = end_timestamp - start_timestamp
            time_interval = duration / len(hr_list)

            timestamp = start_timestamp
            for hr in hr_list:
                data.append(
                    {
                        "timestamp": timestamp.isoformat(
                            timespec="microseconds", sep=" "
                        ),
                        "hr": hr,
                    }
                )
                timestamp += time_interval

    return jsonify(data) """
@app.route("/subject/<subject_uuid>/hr/<time_from>/<time_to>/<video>")
def get_hr_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HR_DATA
    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        start_timestamp = row["start_timestamp"].to_pydatetime()
        end_timestamp = row["end_timestamp"].to_pydatetime()
        hr_list = row["hr"]

        if hr_list is not None :
            duration = end_timestamp - start_timestamp
            time_interval = duration / len(hr_list)

            timestamp = start_timestamp
            for hr in hr_list:
                data.append(
                    {
                        "timestamp": timestamp.isoformat(
                            timespec="microseconds", sep=" "
                        ),
                        "hr": hr,
                    }
                )
                timestamp += time_interval

    return jsonify(data)

@app.route("/subject/<subject_uuid>/breathing_signal/<time_from>/<time_to>/<video>")
def get_breathing_signal_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + BREATHING_DATA
    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        start_timestamp = row["start_timestamp"]
        end_timestamp = row["end_timestamp"]
        breating_signal = row["breathing_signal"]

        if breating_signal is not None:
            duration = end_timestamp - start_timestamp
            time_interval = duration / len(breating_signal)

            timestamp = start_timestamp
            for bs in breating_signal:
                data.append(
                    {
                        "timestamp": timestamp.isoformat(
                            timespec="microseconds", sep=" "
                        ),
                        "breathing_signal": bs,
                    }
                )
                timestamp += time_interval

    return jsonify(data)


@app.route("/subject/<subject_uuid>/breathing_spectogram/<video>")
def get_breathing_spectogram_by_uuid(subject_uuid, video):

    table_url = PROFILE_FILE + BREATHING_DATA
    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    last_row = pdf.tail(1)
    breathing_psd = last_row.iloc[0]["breathing_psd"]
    breathing_frq = last_row.iloc[0]["breathing_frq"]

    assert len(breathing_frq) == len(
        breathing_psd
    ), "'breathing_psd' and 'breathing_frq' with different lengths"
    assert (
        len(breathing_frq) > 0 and len(breathing_psd) > 0
    ), "'breathing_psd' and 'breathing_frq' need to be > 0"

    data = []
    for i in range(len(breathing_frq)):
        if breathing_frq[i] > 1:        # limit x axis
            break
        data.append(
            {"breathing_psd": breathing_psd[i], "breathing_frq": breathing_frq[i]}
        )

    return jsonify(data)


@app.route("/subject/<subject_uuid>/rri/<video>")
def get_rri_by_uuid(subject_uuid, video):
    table_url = PROFILE_FILE + RRI_DATA

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    last_row = pdf.tail(1)

    data = []
    try:
        bins = last_row.iloc[0]["bins"]
        bins = bins[:-1]
        counts = last_row.iloc[0]["counts"]
        assert len(bins) == len(counts), "Bins and counts with different lengths"

        for i in range(len(bins)):

            if i > 0:
                bucket = str(round(bins[i - 1])) + "-" + str(round(bins[i]))
            else:
                bucket = "0-" + str(round(bins[i]))

            data.append({"bucket": bucket, "count": int(counts[i])})
    except:
        pass

    return jsonify(data)

@app.route("/subject/<subject_uuid>/psd_freq/<video>")
def get_psd_freq_by_uuid(subject_uuid, video):
    table_url = PROFILE_FILE + PSD_FREQ_DATA

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        for l in ["ULF", "VLF", "LF", "VHF", "HF"]:
            freq_key = "freq_"+l
            power_key = "power_"+l

            freq = row[freq_key]
            power = row[power_key]
            for x, y in zip(freq, power):
                data.append({
                    freq_key: x,
                    power_key: y
                })
        
    return jsonify(data)

@app.route("/subject/<subject_uuid>/bpm/<time_from>/<time_to>/<video>")
def get_bpm_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HR_DATA

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "bpm": row["bpm"]
            }
        ) 
        
    return jsonify(data)

@app.route("/subject/<subject_uuid>/breathing/<time_from>/<time_to>/<video>")
def get_breathing_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + BREATHING_DATA

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "breathing": row["breathing_rate"]
            }
        ) 
        
    return jsonify(data)
 
# TODO: COMPLETE (change hr table to hrv table) 
@app.route("/subject/<subject_uuid>/rmssd/<time_from>/<time_to>/<video>")
def get_rmssd_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HEARTPY_MEASURES

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "rmssd": row["rmssd"]
            }
        ) 
        
    return jsonify(data)

@app.route("/subject/<subject_uuid>/sdnn/<time_from>/<time_to>/<video>")
def get_sdnn_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HEARTPY_MEASURES

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "sdnn": row["sdnn"]
            }
        ) 
        
    return jsonify(data)


@app.route("/subject/<subject_uuid>/pnn50/<time_from>/<time_to>/<video>")
def get_pnn50_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HEARTPY_MEASURES

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "pnn50": row["pnn50"]
            }
        ) 
        
    return jsonify(data)

@app.route("/subject/<subject_uuid>/pnn20/<time_from>/<time_to>/<video>")
def get_pnn20_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HEARTPY_MEASURES

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "pnn20": row["pnn20"]
            }
        ) 
        
    return jsonify(data)


@app.route("/subject/<subject_uuid>/sd1/<time_from>/<time_to>/<video>")
def get_sd1_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HEARTPY_MEASURES

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "sd1": row["sd1"]
            }
        ) 
        
    return jsonify(data)

@app.route("/subject/<subject_uuid>/sd2/<time_from>/<time_to>/<video>")
def get_sd2_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HEARTPY_MEASURES

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "sd2": row["sd2"]
            }
        ) 
        
    return jsonify(data)

@app.route("/subject/<subject_uuid>/sd1_sd2/<time_from>/<time_to>/<video>")
def get_sd1_sd2_by_uuid(subject_uuid, time_from, time_to, video):
    time_from = pd.to_datetime(int(time_from) / 1000, unit='s')
    time_to = pd.to_datetime(int(time_to) / 1000, unit='s')

    table_url = PROFILE_FILE + HEARTPY_MEASURES

    pdf = delta_sharing.load_as_pandas(table_url)
    if video == "None":
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["start_timestamp"] > time_from) & (pdf["start_timestamp"] < time_to)]
    else:
        pdf = pdf.loc[(pdf["subject_uuid"] == subject_uuid) & (pdf["video"] == video)]

    data = []
    for i, row in pdf.iterrows():
        data.append(
            {
                "sd1/sd2": row["sd1/sd2"]
            }
        ) 
        
    return jsonify(data)


@app.route("/subject/<subject_uuid>/sessions")
def get_sessions_by_uuid(subject_uuid):
    table_url = PROFILE_FILE + SESSION_DATA

    pdf = delta_sharing.load_as_pandas(table_url)
    pdf = pdf.loc[pdf["subject_uuid"] == subject_uuid]

    data = [{"video": "None"}]
    for i, row in pdf.iterrows():
        """ data.append(
            {
                "value": 1,
                "time": row["start_timestamp"].isoformat(
                    timespec="microseconds", sep=" "
                ),
            }
        )  """
        data.append(
            {
                "video": row["video"]
            }
        )
        
    return jsonify(data)


""" def are_timestamps_in_same_hour_day_month_year(timestamp1, hourly_timestamps):

    for timestamp2 in hourly_timestamps:
        if timestamp1.year == timestamp2.year and \
           timestamp1.month == timestamp2.month and \
           timestamp1.day == timestamp2.day and \
           timestamp1.hour == timestamp2.hour:

           print("ola")
           return """

def are_timestamps_in_same_hour_day_month_year(timestamp1, timestamp2):

    return timestamp1.year == timestamp2.year and \
           timestamp1.month == timestamp2.month and \
           timestamp1.day == timestamp2.day
           #timestamp1.hour == timestamp2.hour

@app.route("/subject/<subject_uuid>/sessions_history")
def get_sessions_history_by_uuid(subject_uuid):

    # Get the current year
    current_year = datetime.now().year

    # Define the start date and time
    # Create a datetime object for the beginning of the year
    start_date = datetime(current_year, 1, 1, 0, 0, 0)        

    # Calculate the end date by adding 5 days to the start date
    end_date = start_date + timedelta(days=364)            

    # Generate timestamps for each hour between the start and end dates
    current_date = start_date
    hourly_timestamps = dict()

    data = []

    while current_date <= end_date:
        tmp = dict()
        tmp["time"] = current_date

        for i in range(24):
            tmp[i] = 0
        
        data.append(tmp)
        current_date += timedelta(days=1)


    table_url = PROFILE_FILE + SESSION_DATA

    pdf = delta_sharing.load_as_pandas(table_url)
    pdf = pdf.loc[pdf["subject_uuid"] == subject_uuid]

    for i, row in pdf.iterrows():
        timestamp = row["start_timestamp"]

        for j in data:
            if are_timestamps_in_same_hour_day_month_year(j["time"], timestamp):
                hour = timestamp.hour
                j[hour] += 1


    for j in data:
        value = j["time"]
        del j["time"]
        j["time"] = str(datetime.strftime(value, "%Y-%m-%d"))

    return json.dumps(data)


""" @app.route("/subject/<subject_uuid>/hr/alert")
def get_hr_by_uuid_alert(subject_uuid):
    current_time = datetime.now()
    #new_time = current_time - timedelta(minutes=1)
    new_time = current_time - timedelta(days=5)
    start_timestamp = new_time.strftime("%Y-%m-%d %H:%M:%S")

    table_url = PROFILE_FILE + HR_DATA

    data = []
    try:
        pdf = delta_sharing.load_table_changes_as_pandas(table_url, starting_timestamp=start_timestamp)
        
        for i, row in pdf.iterrows():
            start_timestamp = row["start_timestamp"].to_pydatetime()
            end_timestamp = row["end_timestamp"].to_pydatetime()
            hr_list = row["hr"]

            if hr_list is not None :
                duration = end_timestamp - start_timestamp
                time_interval = duration / len(hr_list)

                timestamp = start_timestamp
                for hr in hr_list:
                    data.append(
                        {
                            "timestamp": timestamp.isoformat(
                                timespec="microseconds", sep=" "
                            ),
                            "hr": hr,
                        }
                    )
                    timestamp += time_interval

    except requests.exceptions.HTTPError as e:
        print("Invalid parameter value error occurred. Details:")
        print("Error Code:", e.response.json()["errorCode"])
        print("Message:", e.response.json()["message"])

    return jsonify(data) """
    

@app.route("/subject/<subject_uuid>/bpm/alert")
def get_bpm_alert_by_uuid(subject_uuid):
    current_time = datetime.now()
    #new_time = current_time - timedelta(minutes=1)
    new_time = current_time - timedelta(days=5)
    start_timestamp = new_time.strftime("%Y-%m-%d %H:%M:%S")

    table_url = PROFILE_FILE + HR_DATA

    data = []
    try:
        pdf = delta_sharing.load_table_changes_as_pandas(table_url, starting_timestamp=start_timestamp)

        for i, row in pdf.iterrows():
            data.append(
                {
                    "bpm": row["bpm"],
                }
            ) 
    except requests.exceptions.HTTPError as e:
        print("Invalid parameter value error occurred. Details:")
        print("Error Code:", e.response.json()["errorCode"])
        print("Message:", e.response.json()["message"])
        
    return jsonify(data)

# ======================================================================

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000', debug=True)
