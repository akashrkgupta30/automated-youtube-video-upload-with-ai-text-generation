# **Automated YouTube Video Upload with AI Text Generation**

## Introduction
This project showcases an automated system designed to generate engaging videos from input text and upload them to YouTube seamlessly. Leveraging a combination of Python, AWS services, and third-party APIs, this system efficiently processes textual data, merges it with video content, and automates the upload process to reach a wider audience on YouTube.

## Technology Used
1. Programming Language
   - Python
2. AWS Cloud Services
   - S3
   - RDS
   - EC2  
3. Database Language
   - SQL  
4. Third Party API
   - YouTube Data
   - ChatGPT

## Architecture
![boaz](https://github.com/akashrkgupta30/automated-youtube-video-upload-with-ai-text-generation/assets/53559214/0d3ff605-32f1-4e4c-9cf5-3a13b52eae31)

## Scope
1. Text-to-Video Conversion: The system takes input text stored in the database table, including book name, chapter number, verse number, and text, and merges it with video content. Using the ffmpeg library in Python, it overlays text onto input videos, enriching them with animations for enhanced visual appeal.
2. AWS Integration: Utilizing AWS S3 for storing input videos and output files, AWS EC2 for hosting the processing scripts, and AWS RDS for database management, the system ensures secure and scalable storage and computation capabilities.
3. Database Logging: Upon video generation, the system logs the video's path in a database, facilitating easy tracking and management of uploaded content.
4. Description and Tag Generation: Leveraging ChatGPT APIs, the system generates compelling descriptions and tags for each video, optimizing their discoverability on YouTube.
5. YouTube Upload Automation: A separate script handles the upload process to YouTube using the YouTube API. Whether the videos are long-form or short clips, the system ensures seamless uploading to the platform.
6. Batch Processing: The system operates in batch mode on a daily basis, automating the generation and upload of videos to YouTube, catering to a consistent flow of content for viewers.
