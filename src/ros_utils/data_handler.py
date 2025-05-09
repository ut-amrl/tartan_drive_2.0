import cv2
import numpy as np
from scipy.spatial.transform import Rotation as R


def read_image_msg(msg) -> np.ndarray:
    """sensor_msgs/Image to numpy array."""
    np_arr = np.frombuffer(msg.data, np.uint8)
    if hasattr(msg, "format") and "compressed" in msg.format:
        image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    else:
        image = np_arr.reshape(msg.height, msg.width, -1)
    return image


def read_depth_msg(msg) -> np.ndarray:
    """sensor_msgs/Image to numpy array."""
    # https://docs.carnegierobotics.com/S27/api.html#api:camera:depth
    np_arr = np.frombuffer(msg.data, np.float32)
    if hasattr(msg, "format") and "compressed" in msg.format:
        depth = cv2.imdecode(np_arr, cv2.IMREAD_UNCHANGED)
    else:
        depth = np_arr.reshape(msg.height, msg.width)
    return depth


def read_gps_msg(msg) -> np.ndarray:
    """sensor_msgs/NavSatFix to numpy array."""
    return {
        "status": msg.status.status,
        "service": msg.status.service,
        "latitude": msg.latitude,
        "longitude": msg.longitude,
        "altitude": msg.altitude,
        "position_covariance": msg.position_covariance,
        "position_covariance_type": msg.position_covariance_type,
    }


def read_odometry_msg(msg) -> dict:
    """nav_msgs/Odometry to numpy array."""
    return {
        "x": msg.pose.pose.position.x,
        "y": msg.pose.pose.position.y,
        "z": msg.pose.pose.position.z,
        "qx": msg.pose.pose.orientation.x,
        "qy": msg.pose.pose.orientation.y,
        "qz": msg.pose.pose.orientation.z,
        "qw": msg.pose.pose.orientation.w,
        "vx": msg.twist.twist.linear.x,
        "vy": msg.twist.twist.linear.y,
        "vz": msg.twist.twist.linear.z,
        "wx": msg.twist.twist.angular.x,
        "wy": msg.twist.twist.angular.y,
        "wz": msg.twist.twist.angular.z,
    }


def read_twist_msg(msg) -> dict:
    """geometry_msgs/Twist to numpy array."""
    return {
        "vx": msg.linear.x,
        "vy": msg.linear.y,
        "vz": msg.linear.z,
        "wx": msg.angular.x,
        "wy": msg.angular.y,
        "wz": msg.angular.z,
    }


def read_twist_stamped_msg(msg) -> dict:
    """geometry_msgs/TwistStamped to numpy array."""
    return {
        "vx": msg.twist.linear.x,
        "vy": msg.twist.linear.y,
        "vz": msg.twist.linear.z,
        "wx": msg.twist.angular.x,
        "wy": msg.twist.angular.y,
        "wz": msg.twist.angular.z,
    }


def read_racepack_shock_msg(msg) -> dict:
    """racepak/rp_shock_sensors to numpy array."""
    return {
        "front_left": msg.front_left,
        "front_right": msg.front_right,
        "rear_left": msg.rear_left,
        "rear_right": msg.rear_right,
    }