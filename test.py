#!/usr/bin/python
# -*- coding: utf-8 -*-


"""
This is a module for unit testing different functionalities for xml parsing
This module has been tested on Python 3.6

Author => Pragyat Gupta
"""


import pytest
from main import Connection, SaveFile, HandleXML, UtilityFunctions
import os
import csv
import requests

@pytest.fixture
def test_connection():
    """
    This is a function for instantiating Connection class
    """
    return Connection()

@pytest.fixture
def test_write_to_disk():
    """
    This is a function for instantiating SaveFile class
    """
    return SaveFile()

@pytest.fixture
def test_utility_functions():
    """
    This is a function for instantiating UtilityFunctions class
    """
    return UtilityFunctions()

@pytest.fixture
def test_xml_handling():
    """
    This is a function for instantiating HandleXML class
    """
    return HandleXML()

@pytest.mark.parametrize("url, file_name", [("https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100","Intermediate.xml")])
def test_url_download(test_connection, url, file_name):
    """
    This is a function for unit testing download functionality from given url
    """

    try:
       os.remove(file_name)
    except Exception:
       pass
    test_connection.get_file_from_web(url, file_name)
    assert os.stat(file_name).st_size > 0

@pytest.mark.parametrize("file_name", [("Test.csv")])
def test_write_to_csv(test_write_to_disk, file_name):
    """
    This is a function for unit testing writing data to CSV
    """

    try:
       os.remove(file_name)
    except Exception:
       pass
    data = [{"Id":"DE000MC7YRD4","FullNm":"Open End Turbo Long Planet Fitness emittiert von Morgan Stanley & Co. Int. plc","ClssfctnTp":"RFSTCA","CmmdtyDerivInd":"false","NtnlCcy":"EUR","Issr":"4PQUHN3JPFGFNF3BB653"}]
    test_write_to_disk.save_as_csv(file_name, data)

    assert os.stat(file_name).st_size > 0 and len(list(csv.reader(open(file_name)))) == len(data)

@pytest.mark.parametrize("file_name, output_file_name", [("Data.zip","DLTINS_20210117_01of01.xml")])
def test_zip_extraction(test_utility_functions, file_name, output_file_name):
    """
    This is a function for unit testing zip extraction functionality
    """

    try:
       os.remove(output_file_name)
    except Exception:
       pass
    test_utility_functions.unzip_file(file_name)
    assert os.stat(output_file_name).st_size > 0

@pytest.mark.parametrize("file_name, link", [("Intermediate.xml", "http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip")])
def test_get_first_link_with_file_type_DLTINS(test_xml_handling, file_name, link):
    """
    This is a function for unit testing getting first link with file type DLTINS from Intermediate XML file
    """

    assert test_xml_handling.get_first_link_from_xml(file_name) == link

# This test case will fail if correct AWS credentials arer not provided in the script
@pytest.mark.parametrize("file_name", [("Info.csv")])
def test_csv_s3_bucket_upload(test_connection, file_name):
    """
    This is a function for unit testing file upload to s3 bucket
    """

    file_url = test_connection.store_file_to_s3_bucket(file_name)
    assert requests.get(file_url).status_code == 200

