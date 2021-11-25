import setuptools

setuptools.setup(
    name='amariltb',
    version='0.0.3',
    author='Shahar Halutz',
    author_email='shahar.halutz@gmail.com',
    description='provides amaril lab services',
    url='https://github.com/Muls/toolbox',
    project_urls = {
        "Bug Tracker": "https://github.com/Muls/toolbox/issues"
    },
    license='MIT',
    include_package_data=True,
    packages=['amariltb'],
    install_requires=['boto3','openpyxl','google-cloud-storage','pydub','ffmpeg','numpy','pandas'],
    zip_safe=False,
)
