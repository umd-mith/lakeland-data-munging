# This was adapted from umd-mith/lchp-digitization to find images
# associated with folders and convert them to PNG derivatives

import os
import re
import json
import shutil
import logging

from os import stat
from PIL import Image
from os import listdir as ls
from os.path import join, isdir, isfile, abspath

IMAGE_ROOT = abspath('mith-lastclass-raw')

def get_images(folder):
    images_dir = find_images_dir(folder)

    for name in ls(images_dir):
        image_path = join(images_dir, name)

        # find filenames that look correct {folder-id}-{item-id}.tif
        #
        # unfortunately there is a lot of variation in how the tiff files ended
        # up on disk. Some have .tif or .tiff extensions, in all sorts of
        # capitalization. Some have no extension at all. Some lack dashes,
        # some have underscores instead of dashes.

        if re.match('^[a-z0-9]{6}[-_]?\d{2,3}(\.(tiff?)|\.(jpe?g))?$', name, re.IGNORECASE):
            image_id = get_image_id(images_dir, name)
            if not image_id:
                print('unable to determine image_id for {}'.format(mage_path))
            else:
                yield({"path": image_path, "id": image_id})
        else:
            print('does not look like an image: {}'.format(name))


def find_images_dir(folder):
    """
    Locate the images directory for a folder. The TIFFs could be in
    the folder directly, or in a subfolder named something like Uncorrected TIFF
    Files. Sometimes the TIFF files have been postprocessed and put into a folder 
    called Jpegs. Sometimes it all looks crazy and we do nothing.
    """

    if len(folder) == 6:

        images_dir = None
        for name in ls(join(IMAGE_ROOT, folder)):
            path = join(IMAGE_ROOT, folder, name)

            # Jpegs contains post-processed images for the website
            # If we find this folder we're done looking.
            if isdir(path) and name == 'Jpegs':
                images_dir = path
                break

            # Otherwise look for a tiff directory. We keep looking 
            # because there could be more than one and 'corrected' tiffs
            # are given preference.
            elif isdir(path) and re.search(r'tiff', name, re.IGNORECASE):
                normal_name = name.lower()
                if 'uncorrected' not in normal_name and 'corrected' in normal_name:
                    images_dir = path
                else:
                    images_dir = path

            # If no images subdirectory is present the tiffs could be located
            # directly in the folder directory?
            elif not images_dir and re.search('[a-z0-9]+-\d{2,3}', name, re.IGNORECASE):
                images_dir = join(IMAGE_ROOT, folder)

        # warn user about missing images dir
        if not images_dir:
            logging.warning("unable to find images dir for %s", join(IMAGE_ROOT, folder))
        else:
            logging.info('found images dir %s', images_dir)
            return images_dir
    else:
        logging.warning('%s does not look like a folder', folder)
        return None


def get_image_id(tiffs_dir, name):
    image_id = name.lower()

    # remove extension
    image_id, ext = os.path.splitext(image_id)

    # replace occasional underscores
    image_id = image_id.replace('_', '-')

    # add a dash if needed 
    if '-' not in image_id:
        image_id = image_id[0:6] + '-' + image_id[6:]

    # zero pad item if needed
    folder, seq = image_id.split('-')
    image_id = '{}-{:03d}'.format(folder, int(seq))

    # sanity check
    if not re.match(r'^[a-z0-9]{6}-[0-9]{3}$', image_id):
        logging.warning('cleaned image name not correct: %s', image_id)
        return None

    image_id = image_id.replace('.tif', '')
    return image_id


def make_image(orig_image_path, image_id):
    # this is the path where the new file will be written
    image_path = 'static/images/{}.png'.format(image_id)

    # skip if the file has been converted before and its newer than the original 
    if isfile(image_path) and stat(image_path).st_mtime > stat(orig_image_path).st_mtime:
        logging.info('image already processed %s', image_id)
        return

    # convert!
    img = Image.open(orig_image_path)
    img.thumbnail((1200, 1200))
    img.save(image_path)
    logging.info('saved %s', image_path)
