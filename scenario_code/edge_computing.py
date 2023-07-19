from minio import Minio
from minio.error import S3Error
from moviepy.video.io.VideoFileClip import VideoFileClip
import argparse
import mimetypes
import subprocess
import heartpy as hp
from datetime import datetime

from json import dumps

from video_rppg_extraction import VideoRppgExtraction
from signal_processer import SignalProcessor
from settings import LANDING_BUCKET_NAME

import paho.mqtt.client as mqtt


def slice_video(video_path, object_name):

    object_name, extension = object_name.split(".")

    # Read the input video
    input_video = VideoFileClip(video_path)

    # Define the desired duration of each video part (in seconds)
    part_duration = 5

    # Determine the total number of parts
    total_parts = int(input_video.duration // part_duration)

    # Extract and write video parts
    parts_list = []
    for i in range(total_parts):
        start_time = i * part_duration
        end_time = (i + 1) * part_duration
        video_part = input_video.subclip(start_time, end_time)

        video_part_name = f'{object_name}_{i}.mp4'
        parts_list.append(f'{object_name}_{i}.mp4')
        video_part.write_videofile(video_part_name)

    return parts_list, object_name

def analyze_videos(parts_list, subject):

    video_extractor = VideoRppgExtraction()
    signal_processer = SignalProcessor()

    for v in parts_list:
        print(f"Analyzing video: {v}")

        filtered_bvp, fps, _ = video_extractor.extract_bvp_signal(v)
        _, measures = signal_processer.process_bvp_signal(filtered_bvp, fps)

        send_bpm_measures(measures["bpm"], subject)


def send_bpm_measures(bpm, subject):
    data = {
        "timestamp": str(datetime.now()),
        "bpm": int(round(bpm)),
        "subject": str(subject)
    }
    print(data)

    mqtt_client.publish("bpm", int(round(bpm)), 2)


def main(video_path):

    # Specify the object name
    object_name = video_path.split("/")[-1]

    parts_list, subject = slice_video(video_path, object_name)

    analyze_videos(parts_list, subject)

    # TODO: Identify person in video? Or simply specify with args
            
    # Upload video to MinIO bucket
    try:
        content_type, encoding = mimetypes.guess_type(video_path)

        # Upload the video file
        minio_client.fput_object(
            bucket_name=LANDING_BUCKET_NAME,
            object_name=object_name,
            file_path=video_path,
            content_type=content_type  # Set the content type of the video file
        )

        print(f'Successfully uploaded video to {LANDING_BUCKET_NAME}/{object_name}')
        subprocess.run(f'rm *.mp4', shell=True)

    except S3Error as e:
        print(f'Error uploading video: {e}')


if __name__ == "__main__":

    # Args Setup
    parser = argparse.ArgumentParser(
                    prog='Edge Computing Video')
    parser.add_argument('-v', '--video', type=str,
                    help='Path of video to process', required=True)
    args = vars(parser.parse_args())
    
    # Create connection with minio
    minio_client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

    # Create a mqtt client to send the bpm measures
    mqtt_client = mqtt.Client()
    mqtt_client.connect("localhost", 1883, 60)

    main(args["video"])