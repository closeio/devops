"""Removes old images from ECR."""

import argparse
import datetime
import json
import logging
import sys

import boto3
import pytz

def init():
    """Initialize system."""

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def get_images(ecr_client, token, registry_id, repository_name):
    """Retries a batch of images from ECR.

    Retrieves a batch of images from ECR.  ECR currently has a limit of 100 images per request.

    Args:
        ecr_client: Boto3 client
        token: String of next token from last list_images calling
        registry_id: AWS Account ID string
        repository_name: ECR repository name string

    Returns:
        List of boto3 imageIds
    """

    if token:
        response = ecr_client.list_images(registryId=registry_id, repositoryName=repository_name, maxResults=50,
                                          nextToken=token)
    else:
        response = ecr_client.list_images(registryId=registry_id, repositoryName=repository_name, maxResults=50)
    token = None
    if 'nextToken' in response:
        token = response['nextToken']

    return list(response['imageIds']), token


def delete_old_images(ecr_client, images, registry_id, repository_name):
    """Deletes an image from ECR.

    Delete a single ECR image.

    Args:
        ecr_client: Boto3 client
        images: Boto ECR images list
        registry_id: AWS Account ID string
        repository_name: ECR repository name string
    """

    for image in images:
        logging.info("Deleteing %s %s", image[1], image[2])
        ecr_client.batch_delete_image(registryId=registry_id, repositoryName=repository_name,
                                      imageIds=[{'imageDigest': image[0], }, ])


def find_old_images(ecr_client, registry_id, repository_name, min_num, max_age):
    """Find old ECR images eligible for deletion.

    Finds images that are older than max_age but still keeps a minimum number available.

    Args:
        ecr_client: Boto3 client
        registry_id: AWS Account ID string
        repository_name: ECR repository name string
        min_num: The minimum number of images we should keep in ECR.  Old images will be kept to ensure the total
            number does not end up below this number.
        max_age: Maximum age in days before images should be considered for deletion.
    """

    logging.info("Getting images")

    images = []
    token = None
    old_images = []

    # We get batches of images.  For loop ensures we don't get stuck in an infinite loop.
    for i in range(0, 100):
        logging.info("Getting another batch of images %d", i)
        batch_of_images, token = get_images(ecr_client, token, registry_id, repository_name)
        images = images + batch_of_images
        if not token:
            break

    logging.info("Found %d images", len(images))

    if len(images) < min_num:
        logging.info('Number of images (%d) is less than min_num (%d)', len(images), min_num)
        return []

    sorted_images = []

    logging.info('Looking up image create dates')
    for image in images:

        if 'imageTag' not in image:
            image_tag = ""
        else:
            image_tag = image['imageTag']
        image_detail = ecr_client.describe_images(registryId=registry_id, repositoryName=repository_name,
                                                  imageIds=[{'imageDigest': image['imageDigest'], }, ])

        image_date = image_detail['imageDetails'][0]['imagePushedAt']
        sorted_images.append((image['imageDigest'], image_tag, image_date))

    sorted_images = sorted(sorted_images, key=lambda tup: tup[2])

    if len(sorted_images) > 0:
        logging.info("Oldest image: %s %s", sorted_images[0][0], sorted_images[0][2])
        logging.info("Newest image: %s %s", sorted_images[-1][0], sorted_images[-1][2])

    # We always keep the latest for safety
    sorted_images = sorted_images[:-1]

    for image in sorted_images:
        if len(sorted_images) - len(old_images) + 1 <= min_num:
            logging.info("Need to stop to stay above min_num")
            break
        age = (datetime.datetime.now(pytz.UTC) - image[2]).total_seconds() / 60.0 / 60.0 / 24.0
        if age > max_age:
            logging.debug('Old image %s', image)
            old_images.append(image)
    logging.info("Found %d old images", len(old_images))
    return old_images


def prune_ecr(region, account, repo_name, days, min_num):
    """Prune images out of ecr repository."""

    logging.info('Starting prune')
    client = boto3.client('ecr', region_name=region)
    old_ecr_images = find_old_images(client, account, repo_name, min_num, days)
    delete_old_images(client, old_ecr_images, account, repo_name)


def doit():
    """Run this thing."""

    init()
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--days', help='Maximum days old Default: 14', type=int, default=14)
    parser.add_argument('-m', '--min', help='Minimum number to keep Default 10', type=int, default=10)
    parser.add_argument('-a', '--account', help='AWS Account Number', required=True)
    parser.add_argument('-n', '--reponame', help='Repo name, make sure you are connected to right region',
                        required=True)
    parser.add_argument('-r', '--region', help='AWS Region', default=None)
    args = parser.parse_args()

    client = boto3.client('ecr', region_name=args.region)
    old_ecr_images = find_old_images(client, args.account, args.reponame, args.min, args.days)
    delete_old_images(client, old_ecr_images, args.account, args.reponame)

if __name__ == "__main__":
    try:
        doit()
    except Exception as exception:  # pylint: disable=w0703
        logging.error("Error:", exc_info=True)
        sys.exit(1)
