import os
import yaml
import numpy as np
import concurrent.futures
import shutil
import re

import rclpy
from rclpy.node import Node
from geometry_msgs.msg import PoseStamped
from rosbag2_py import SequentialWriter, StorageOptions, ConverterOptions, TopicMetadata, SequentialReader, StorageFilter
from rclpy.time import Time
from rclpy.serialization import serialize_message
from transforms3d.quaternions import mat2quat

"""
This nodes reads YAML files containing 4x4 transformation matrices (relative poses)
obtained from autoware.universe /localization/pose_twist_fusion_filter/pose topic and writes them to a bag file.
Currently, the obtained bag from this node should be merged with the original bag containing lidar frames using the following command:

(ros2 bag play first_bag.db3 & ros2 bag play second_pose_only_bag.db3 & ros2 bag record -o merged_bag /topic1 /topic2 /topic3 & wait)
or
(ros2 bag play first_bag.db3 -d 1 --remap /ouster/points:=/ouster/points/car1 /pose:=/pose/car1 & 
 ros2 bag play second_bag.db3 -d 1 --remap /ouster/points:=/ouster/points/car2 /pose:=/pose/car2 &
 ros2 bag record -o V2X --all & wait)
 
"""

class YAMLToBag(Node):
    def __init__(self):
        super().__init__('yaml_to_bag')
        self.get_logger().info('Pose to YAML Node has started!')
        self.declare_parameter('bag_kia1', '/workspace/htw_rosbag/KIA1/2024-03-19-14-18-43_KIA1_ros2.db3')
        self.declare_parameter('bag_kia2', '/workspace/htw_rosbag/KIA2/2024-03-19-14-18-43_KIA2_ros2.db3')
        self.declare_parameter('output_bag', '/workspace/htw_rosbag/V2X/v2x_bag.db3')
        self.declare_parameter('yaml_dir_kia1', '/workspace/2024-03-19-14-18-43/1/')
        self.declare_parameter('yaml_dir_kia2', '/workspace/2024-03-19-14-18-43/0/')
        self.declare_parameter('pose_topic_dst', '/pose')
        self.declare_parameter('lidar_topic_src', '/ouster/points')
        
        self.yaml_dir_kia1 = self.get_parameter('yaml_dir_kia1').get_parameter_value().string_value
        self.yaml_dir_kia2 = self.get_parameter('yaml_dir_kia2').get_parameter_value().string_value
        self.bag_kia1 = self.get_parameter('bag_kia1').get_parameter_value().string_value
        self.bag_kia2 = self.get_parameter('bag_kia2').get_parameter_value().string_value
        self.output_bag = self.get_parameter('output_bag').get_parameter_value().string_value
        self.pose_topic = self.get_parameter('pose_topic_dst').get_parameter_value().string_value
        self.lidar_topic = self.get_parameter('lidar_topic_src').get_parameter_value().string_value

        sopts_output = StorageOptions(uri=self.output_bag, storage_id='sqlite3')
        self.writer = SequentialWriter()
        self.writer.open(sopts_output, ConverterOptions('cdr', 'cdr'))
        
        self.reader1 = self.open_reader(self.bag_kia1, [self.lidar_topic])
        self.reader2 = self.open_reader(self.bag_kia2, [self.lidar_topic])

        self.create_topic(self.pose_topic + '/kia1', 'geometry_msgs/msg/PoseStamped')
        self.create_topic(self.pose_topic + '/kia2', 'geometry_msgs/msg/PoseStamped')
        self.create_topic(self.lidar_topic + '/kia1', 'sensor_msgs/msg/PointCloud2')
        self.create_topic(self.lidar_topic + '/kia2', 'sensor_msgs/msg/PointCloud2')
        
        self.add_poses_to_bag(self.yaml_dir_kia1, self.pose_topic + '/kia1')
        self.add_poses_to_bag(self.yaml_dir_kia2, self.pose_topic + '/kia2')
        self.add_pointclouds_to_bag(self.reader1, self.lidar_topic, self.lidar_topic + '/kia1')
        self.add_pointclouds_to_bag(self.reader2, self.lidar_topic, self.lidar_topic + '/kia2')

        del self.writer
    
    def open_reader(self, bag_path, topics=[]):
        reader = SequentialReader()
        storage_options = StorageOptions(uri=bag_path, storage_id='sqlite3')
        converter = ConverterOptions('cdr', 'cdr')
        reader.open(storage_options, converter)
        if topics:
            reader.set_filter(StorageFilter(topics=topics))
        return reader
    
    def create_topic(self, topic_name, topic_type, serialization_format='cdr'):
        topic = TopicMetadata(
            name=topic_name,
            type=topic_type,
            serialization_format=serialization_format
        )
        self.writer.create_topic(topic)

    def matrix_to_pose_stamped(self, matrix, timestamp):
        """Converts a 4x4 transformation matrix to a PoseStamped message."""
        position = matrix[:3, 3]
        rotation_matrix = matrix[:3, :3]
        quaternion = mat2quat(rotation_matrix)

        # Create a PoseStamped message
        msg = PoseStamped()
        msg.header.stamp = Time(seconds=int(timestamp.split('.')[0]), nanoseconds=int(timestamp.split('.')[1])).to_msg()
        msg.pose.position.x = position[0]
        msg.pose.position.y = position[1]
        msg.pose.position.z = position[2]
        msg.pose.orientation.x = quaternion[1]
        msg.pose.orientation.y = quaternion[2]
        msg.pose.orientation.z = quaternion[3]
        msg.pose.orientation.w = quaternion[0]
        return msg

    def add_pointclouds_to_bag(self, reader, topic_src, topic_dst):
        while reader.has_next():
            (topic, data, t) = reader.read_next()
            if topic == topic_src:
                self.writer.write(topic_dst, data, t)

    def add_poses_to_bag(self, pose_dir, pose_topic):
        # Read YAML files and append data
        for file_name in sorted(os.listdir(pose_dir)):
            if re.match(r'^\d{6}\.yaml$', file_name):
                file_path = os.path.join(pose_dir, file_name)
                with open(file_path, 'r') as file:
                    yaml_data = yaml.load(file, Loader=yaml.UnsafeLoader)
                    pose_matrix = np.array(yaml_data.get('lidar_pose', []))
                    timestamp = yaml_data.get('timestamp', '0.0')
                    if pose_matrix.size == 16:  # Ensure it's a 4x4 matrix
                        pose_matrix = pose_matrix.reshape((4, 4))
                        msg = self.matrix_to_pose_stamped(pose_matrix, timestamp)
                        serialized_msg = serialize_message(msg)
                        timestamp = Time.from_msg(msg.header.stamp).nanoseconds
                        self.writer.write(pose_topic, serialized_msg, timestamp)

def main(args=None):
    rclpy.init(args=args)
    y2b = YAMLToBag()
    # rclpy.spin(y2b)
    rclpy.shutdown()

if __name__ == '__main__':
    main()
