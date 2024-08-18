from setuptools import find_packages, setup

package_name = 'yaml_pose_to_bag_py'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Farzad Nozarian',
    maintainer_email='farzad.nozarian@dfki.de',
    description='TODO: Package description',
    license='Apache License 2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'yaml_to_bag = yaml_pose_to_bag_py.yaml_to_bag:main',
        ],
    },
)
