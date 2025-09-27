"""Setup configuration for weather feed plugin."""

from setuptools import setup, find_packages

setup(
    name="aurum-weather-feed-plugin",
    version="1.0.0",
    description="Weather data integration plugin for Aurum platform",
    author="Aurum Team",
    author_email="team@aurum.dev",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "aiohttp>=3.8.0",
        "pandas>=1.5.0",
        "pydantic>=1.10.0"
    ],
    entry_points={
        "aurum.plugins": [
            "weather_feed = weather_feed.weather_plugin:create_weather_feed_plugin"
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="aurum plugin weather energy forecasting",
    project_urls={
        "Documentation": "https://docs.aurum.dev/plugins/",
        "Source": "https://github.com/aurum-platform/plugins",
        "Tracker": "https://github.com/aurum-platform/plugins/issues",
    },
)
