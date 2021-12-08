"""
setuptools for mg_app_framework

python setup.py bdist_wheel

"""

from setuptools import setup

entry_points = {
    "console_scripts": [
        "mt_reboot = mt.cli.mt_cluster_reboot:cluster_reboot"
    ]
}

with open("requirements.txt") as f:
    requires = [l for l in f.read().splitlines() if l]

setup(
    name='mt',
    version='0.0.1',
    packages=['mt', 'mt.cli'],
    package_dir={'mt': 'src/mt', 'mt.core': 'src/mt/core', 'mt.profiler': 'src/mt/profiler',
                 'mt.operation': 'src/mt/operation', 'mt.cli': 'src/mt/cli',
                 'mt.conf': 'src/mt/conf'},
    include_package_data=True,
    install_requires=requires,
    entry_points=entry_points,
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows'
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Build Tools',
    ]
)
