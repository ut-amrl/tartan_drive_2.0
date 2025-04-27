import csv
import yaml
import pathlib
import argparse
from collections import defaultdict

import cv2
import numpy as np
from tqdm import tqdm
from loguru import logger

from ros_utils import (
    read_bagfile,
    read_image_msg,
    read_gps_msg,
    read_odometry_msg,
    read_twist_msg,
    read_twist_stamped_msg,
)


def process_image_msgs(msgs, outdir, ts_file, prefix):
    """
    Generic function to process messages. The process_fn is a callback to handle
    the individual message conversion and writing.
    """
    timestamps = []
    for frame, (ts, msg) in tqdm(
        enumerate(msgs, 1),
        total=len(msgs),
        desc=f"Processing ({outdir.name})",
        leave=False,
    ):
        # Skip messages with invalid timestamps
        if hasattr(msg, "header") and ts < 1e-3:
            logger.warning(f"Invalid timestamp {ts} for message {msg}")
            continue

        outfile = str(outdir / f"{prefix}{frame:06d}.png")
        image = read_image_msg(msg)
        cv2.imwrite(outfile, image)
        timestamps.append(ts)

    np.savetxt(ts_file, timestamps, fmt="%.6f")
    logger.success(f"Saved {len(timestamps)} files to {outdir}")


def process_csv_msgs(msgs, outfile):
    rows = []
    header = None

    for ts, msg in tqdm(msgs, total=len(msgs), leave=False):
        # Skip messages with invalid timestamps
        if hasattr(msg, "header") and ts < 1e-3:
            logger.warning(f"Invalid timestamp {ts} for message {msg}")
            continue

        msg_type = msg.__class__.__name__
        if msg_type == "sensor_msgs__msg__NavSatFix":
            data = read_gps_msg(msg)
        elif msg_type == "nav_msgs__msg__Odometry":
            data = read_odometry_msg(msg)
        elif msg_type == "geometry_msgs__msg__Twist":
            data = read_twist_msg(msg)
        elif msg_type == "geometry_msgs__msg__TwistStamped":
            data = read_twist_stamped_msg(msg)
        else:
            raise NotImplementedError(f"Unsupported message type {msg_type}")

        if header is None:
            header = ["timestamp"] + list(data.keys())

        def format_value(val):
            if isinstance(val, np.ndarray):
                return "[" + ";".join(f"{x:.8f}" for x in val.flatten()) + "]"
            elif isinstance(val, (int, float)):
                return f"{val:.8f}"
            else:
                return str(val)

        row = {"timestamp": f"{ts:.6f}"}
        row.update({k: format_value(v) for k, v in data.items()})
        rows.append(row)

    # Write to CSV file
    with open(outfile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    logger.success(f"Saved {len(rows)} rows to {outfile}")


def main(args):
    with open(args.config) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)

        # Extract bag files
        topics_info = config["topics"]
        scene_info = config["scenes"]
        for idx, (scene_name, bag_path) in enumerate(scene_info.items()):
            logger.info(f"[{idx+1}/{len(scene_info)}] Extract {scene_name}...")

            # Read the bag file
            topics_to_msgs = defaultdict(list)
            for bag_path in bag_path["files"]:
                if not bag_path.endswith(".bag"):
                    logger.warning(f"Skipping non-bag file: {bag_path}")
                    continue

                bagfile = pathlib.Path(config["bagfile_root"]) / bag_path
                tmp_topics_to_msgs = read_bagfile(bagfile, topics_info.keys())
                for topic, msgs in tmp_topics_to_msgs.items():
                    topics_to_msgs[topic].extend(msgs)

            # Process the messages for each topic
            output_dir = pathlib.Path(config["output_root"]) / scene_name
            output_dir.mkdir(parents=True, exist_ok=True)
            for topic, topic_info in topics_info.items():
                if topic not in topics_to_msgs or len(topics_to_msgs[topic]) == 0:
                    logger.warning(f"No messages found for topic {topic}")
                    continue

                msg = topics_to_msgs[topic]
                fmt = topic_info["format"]

                if fmt == "image":
                    sub = topic_info["sub"]
                    ts_file = output_dir / topic_info["outdir"] / f"timestamp_{sub}.txt"
                    outdir = output_dir / topic_info["outdir"] / topic_info["sub"]
                    outdir.mkdir(parents=True, exist_ok=True)
                    prefix = f"{sub}_"
                    process_image_msgs(msg, outdir, ts_file, prefix)
                elif fmt == "csv":
                    outfile = output_dir / f"{topic_info['name']}.csv"
                    process_csv_msgs(msg, outfile)
                else:
                    raise NotImplementedError(f"Unsupported format {fmt}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract bag files")
    parser.add_argument("--config", type=str, default="config/extract.yaml")
    args = parser.parse_args()

    main(args)
