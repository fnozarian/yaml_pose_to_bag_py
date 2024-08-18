import os
import yaml
import numpy as np
import rclpy
from rclpy.node import Node
from geometry_msgs.msg import PoseStamped
from rosbag2_py import SequentialWriter, StorageOptions, ConverterOptions, TopicMetadata
from rclpy.time import Time
from rclpy.serialization import serialize_message
from transforms3d.quaternions import mat2quat
import shutil


"""
This nodes reads YAML files containing 4x4 transformation matrices (relative poses)
obtained from autoware.universe /localization/pose_twist_fusion_filter/pose topic and writes them to a bag file.
Currently, the obtained bag from this node should be merged with the original bag containing lidar frames using the following command:
```
ros2 bag convert -i htw_rosbag/2024-03-19-14-18-43_KIA2_ros2.db3 -i \
    htw_rosbag/2024-03-19-14-18-43_KIA2_pose/2024-03-19-14-18-43_KIA2_pose_0.db3 \
    -o merge_ops.yaml
```

"""
class YAMLToBag(Node):
    def __init__(self):
        super().__init__('yaml_to_bag')
        self.get_logger().info('Pose to YAML Node has started!')
        # Merge the resulted pose bag with the existing bag using the following command:
        # ros2 bag convert -i htw_rosbag/2024-03-19-14-18-43_KIA2_ros2.db3 -i \
        # htw_rosbag/2024-03-19-14-18-43_KIA2_pose/2024-03-19-14-18-43_KIA2_pose_0.db3 \
        # -o merge_ops.yaml
        self.declare_parameter('bag_file', '/workspace/htw_rosbag/2024-03-19-14-18-43_KIA2_pose')
        self.declare_parameter('yaml_directory', '/workspace/2024-03-19-14-18-43/0/')
        self.declare_parameter('pose_topic', 'pose')
        self.bag_file = self.get_parameter('bag_file').get_parameter_value().string_value
        self.yaml_directory = self.get_parameter('yaml_directory').get_parameter_value().string_value
        self.pose_topic = self.get_parameter('pose_topic').get_parameter_value().string_value
        storage_options = StorageOptions(uri=self.bag_file, storage_id='sqlite3')
        # Check if the directory exists and delete it
        if os.path.exists(storage_options.uri):
            shutil.rmtree(storage_options.uri)

        converter_options = ConverterOptions('', '')
        self.writer = SequentialWriter()
        self.writer.open(storage_options, converter_options)
        topic_info = TopicMetadata(
            name=self.pose_topic,
            type='geometry_msgs/msg/PoseStamped',
            serialization_format='cdr'
        )
        self.writer.create_topic(topic_info)
        self.append_to_bag()

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

    def append_to_bag(self):
        # Read YAML files and append data
        for file_name in sorted(os.listdir(self.yaml_directory)):
            if file_name.endswith('.yaml'):
                file_path = os.path.join(self.yaml_directory, file_name)
                with open(file_path, 'r') as file:
                    yaml_data = yaml.load(file, Loader=yaml.UnsafeLoader)
                    pose_matrix = np.array(yaml_data.get('lidar_pose', []))
                    timestamp = yaml_data.get('timestamp', '0.0')
                    if pose_matrix.size == 16:  # Ensure it's a 4x4 matrix
                        pose_matrix = pose_matrix.reshape((4, 4))
                        msg = self.matrix_to_pose_stamped(pose_matrix, timestamp)
                        serialized_msg = serialize_message(msg)
                        timestamp = int(Time.from_msg(msg.header.stamp).nanoseconds)
                        self.writer.write(self.pose_topic, serialized_msg, timestamp)

def main(args=None):
    rclpy.init(args=args)
    y2b = YAMLToBag()
    # rclpy.spin(y2b)
    rclpy.shutdown()

if __name__ == '__main__':
    main()
