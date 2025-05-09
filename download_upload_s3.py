import boto3
import random
import string
import pymongo
import shutil
import requests
import pandas as pd
from datetime import date
import os
import concurrent.futures
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# Configs
myclient = pymongo.MongoClient("URL")
mydb = myclient[""]
dblist = myclient.list_database_names()
dump = mydb[""]
images_on_s3 = mydb[""]
temp_dir = './temp'
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)
    print(f"Created temp directory at {temp_dir}")
def download_and_upload_property_image(source_product_id, image_url, caption, count):
    try:
        # Check if the image URL is valid
        if not image_url:
            print(f"Invalid image URL for product {source_product_id}")
            return

        # Proxy configuration (optional)
        proxy = ''
        username = ''
        password = ''
        headers = {'User-Agent': 'Mozilla/5.0'}
        proxies = {}
        if proxy and username and password:
            proxy_url = f"http://{username}:{password}@{proxy}"
            proxies = {"http": proxy_url, "https": proxy_url}
        elif proxy:
            proxies = {"http": proxy, "https": proxy}

        # Make a request to get the image data
        res = requests.get(image_url, headers=headers, stream=True, timeout=10, proxies=proxies)


        content_type = res.headers.get('Content-Type', '')
        if 'image' not in content_type:
            print(content_type)
            print(f"URL {image_url} is not an image. Content-Type: {content_type}")
            return False

        # Check if the response is successful
        res.raise_for_status()

        # Try to open and verify the image
        img = Image.open(BytesIO(res.content))
        img.verify()  # Verify if it's a valid image
        save_path = f"{temp_dir}/{source_product_id}_{count}_image.jpg"
        # Save the image to the specified path
        with open(save_path, "wb") as file:
            file.write(res.content)

        print(f"Successfully downloaded and saved image to {save_path}")
        # Upload to S3
        s3 = boto3.resource(
            service_name='s3',
            region_name='',
            aws_access_key_id='',
            aws_secret_access_key=''
        )

        partner_name = ""
        rand_string_len = 30
        rand_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=rand_string_len))
        image = f"{rand_string}.jpg"
        image_dir = f"{date.today().year}/{date.today().month}/{date.today().day}/{count}/"
        s3_image_url = f"images/{image_dir}{image}"

        # Upload the file to S3
        s3.Bucket('bucket_name').upload_file(Filename=save_path, Key=s3_image_url, ExtraArgs={"ContentType": "image/jpeg"})
        if os.path.exists(save_path):
            os.remove(save_path)

        print(f"Image uploaded to S3 with key: {s3_image_url}")
        # Insert document into MongoDB
        images_on_s3.insert_one({
            "source_url": image_url,
            "s3_image_url": f"domain.com/{s3_image_url}",
            "caption": caption,
            "imgPriority": count,
            "product_image": product_image,
            "original_image_extension": "jpg",
            "product_image_dir": product_image_dir,
            "is_valid": True,
            "error": ""
        })
        return

    except (requests.exceptions.RequestException, IOError, Exception) as e:
        print(f"Failed to download and upload image from {image_url} - {str(e)}")
        images_on_s3.insert_one({
            "source_url": image_url,
            "s3_image_url": "",
            "caption": caption,
            "imgPriority": count,
            "product_image": "",
            "original_image_extension": "",
            "product_image_dir": "",
            "is_valid": False,
            "error": f"Error: {str(e)}"
        })
        return
        # ThreadPoolExecutor for concurrent image processing
next = dump.find({"is_valid": False})
with ThreadPoolExecutor(max_workers=24) as property_image_pool:
    count = 0
    last_hotel_id = None
    for document in next:
        
        source_product_id = document['source_product_id']
        print(f'going for profuct id {source_product_id}')
        if source_product_id != last_hotel_id:
            count = 0  # Reset the count for a new HotelID
        # Store the current HotelID as last_hotel_id
        last_hotel_id = source_product_id
        count += 1
        image_url = document['source_url']
        caption = document['caption']
        property_image_pool.submit(download_and_upload_property_image, source_product_id, image_url, caption, count)

print("Script Completed !!!")