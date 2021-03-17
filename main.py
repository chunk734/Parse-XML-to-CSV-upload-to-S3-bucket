#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This Python module has different classes for downloading XML files
from web, parsing them into csv and uploading that csv to S3 bucket

This module has been tested on Python 3.6

Author => Pragyat Gupta
"""

import requests
from requests.exceptions import Timeout, HTTPError
import csv
import xml.etree.ElementTree as ET
from zipfile import ZipFile
import logging
import boto3
import os 

#####################################
# To be Assigned by User
AWS_ACCESS_KEY_ID = ""
AWS_ACCESS_KEY_SECRET = ""
REGION_NAME = ""
BUCKET_NAME = ""
#####################################


TIMEOUT = 10
SIZE = 1024
ROWS_COUNT = 10000
URL = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

class Connection:
    """
    This is a class for handling network connections.
    """

    def get_file_from_web(self, url: str, file_name: str):
        """
        This is an utility function for downloading XML files off web

        Parameters: 
        url (str): URL of the file to be downloaded
        file_name (str): The name with which downloaded file will be stored locally 
        """
        try:
            # Keeping the connection open and getting file iteratively as file size could be large 
            response = requests.get(url, stream=True, timeout=TIMEOUT)
            response.raise_for_status()
            with open(file_name, 'wb') as file:
                for file_part in response.iter_content(chunk_size=SIZE):
                    if file_part:
                        file.write(file_part)

        except HTTPError as http_err:
            logging.error("HTTP Error: {http_err}".format(http_err=http_err))
        except Timeout:
            logging.error("HTTP Connection Timeout Occured")
        except Exception as err:
            logging.error("Exception: {err}".format(err=err))

    def store_file_to_s3_bucket(self, file_name: str) -> str:
        """
        This is an utility function for uploading CSV file from local to S3 bucket

        Parameters: 
        bucket_name (str): The name of the S3 bucket to store data in
        file_name (str): The name of the local CSV file to be uploaded to S3
  
        Returns: 
        str: Returns the link of the file name stored in S3 bucket
        """
        try:
            if not AWS_ACCESS_KEY_ID or not AWS_ACCESS_KEY_SECRET or not REGION_NAME or not BUCKET_NAME:
                logging.error("Please assign AWS credentials and S3 bucket name in Global Variables")
                return ""
                
            aws_session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                                aws_secret_access_key=AWS_ACCESS_KEY_SECRET,
                                                region_name=REGION_NAME
                                                )
            s3_resource = aws_session.resource('s3')
            bucket = s3_resource.Bucket(BUCKET_NAME)
            bucket.upload_file(
                Filename=file_name,
                Key=file_name,
                ExtraArgs={'ACL': 'public-read'}
            )
            s3_url = "https://{BUCKET_NAME}.s3.{REGION_NAME}.amazonaws.com/{file_name}".format(BUCKET_NAME=BUCKET_NAME,REGION_NAME=REGION_NAME,file_name=file_name)
            logging.info("CSV File successfully uploaded to url {s3_url}".format(s3_url=s3_url))
            return s3_url
        except Exception as err:
            logging.error("Exception in storing file to S3 Bucket: {err}".format(err=err))


class HandleXML:
    """
    This is a class for handling XML parsing related utilities.
    """
    result = []
    count = 0
    urn = '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}'

    def get_first_link_from_xml(self, file_name: str) -> str:
        """
        This is an utility function for parsing web link from Intermediate XML

        Parameters: 
        file_name (str): The name of the XMLfile to be parsed
  
        Returns: 
        str: Return the web link parsed from XML
        """
        try:
            tree = ET.parse(file_name)
            root = tree.getroot()
            for node in root:
                for child in node[0]:
                    if child.tag == 'str' and child.attrib['name'] \
                        == 'file_type' and child.text == 'DLTINS':
                        for value in node[0]:
                            if value.tag == 'str' and value.attrib['name'] \
                                == 'download_link':
                                web_link = value.text
                                return web_link
        except Exception as err:
            logging.error("Parsing Download link from XML Failed")
            logging.error("Exception: {err}".format(err=err))

    def parse_xml_document(
        self,
        inputFileName: str,
        writeObj,
        outputFileName: str,
        ):
        """
        This is an utility function for parsing information from XML file

        Parameters: 
        inputFileName (str): The name of the input XML file to be parsed
        writeObj (str): Object of class custom class "SaveFile" for writing as CSV
        outputFileName (str): The name of the output CSV file
        """

        # Reading the XML file serially using iterparse as loading whole large XML
        # in memory may cause memory issues.
        # Writing 10000 records at a time to Info.csv due to avoid memoru issues
        try:
            try:
                os.remove("Info.csv")
            except Exception:
                pass
            
            for (event, element) in ET.iterparse(inputFileName, events=('start'
                , 'end')):
                if event == 'end':
                    data = {}
                    if element.tag == self.urn + 'FinInstrm':
                        for child in element.getchildren()[0]:
                            if child.tag == self.urn + 'FinInstrmGnlAttrbts':
                                for value in child.getchildren():
                                    if value.tag == self.urn + 'Id':
                                        data['Id'] = value.text
                                    if value.tag == self.urn + 'FullNm':
                                        data['FullNm'] = value.text
                                    if value.tag == self.urn + 'ClssfctnTp':
                                        data['ClssfctnTp'] = value.text
                                    if value.tag == self.urn + 'CmmdtyDerivInd':
                                        data['CmmdtyDerivInd'] = value.text
                                    if value.tag == self.urn + 'NtnlCcy':
                                        data['NtnlCcy'] = value.text
                            elif child.tag == self.urn + 'Issr':
                                data['Issr'] = child.text
                        self.count += 1
                        self.result.append(data)
                        if self.count == ROWS_COUNT:
                            writeObj.save_as_csv(outputFileName, self.result)
                            self.result.clear()
                            self.count = 0
            writeObj.save_as_csv(outputFileName, self.result)
            self.result.clear()
            self.count = 0
            logging.info("Parsing Records from {inputFileName} Successful".format(inputFileName=inputFileName))
        except Exception as err:
            logging.error("Parsing Records from {inputFileName} Failed".format(inputFileName=inputFileName))
            logging.error("Exception: {err}".format(err=err))
        


class SaveFile:
    """
    This is a class for handling saving to disk related utilities.
    """
    headers = [
        'Id',
        'FullNm',
        'ClssfctnTp',
        'CmmdtyDerivInd',
        'NtnlCcy',
        'Issr',
        ]
    def save_as_csv(self, file_name: str, result: list):
        """
        This is an utility function for writing records in output CSV file

        Parameters: 
        file_name (str): The name of output CSV file
        result (list): List of records to be stored
        """
        try:
            with open(file_name, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.headers)
                writer.writerows(result)
            logging.info("Writing Records to {file_name} Successful".format(file_name=file_name))
        except Exception as err:
            logging.error("Writing Records to {file_name} Failed".format(file_name=file_name))
            logging.error("Exception: {err}".format(err=err))


class UtilityFunctions:
    """
    This is a class for containing miscellaneous utility functions.
    """
    def unzip_file(self, file_name: str) -> int:
        """
        This is an utility function for extracting files from zip format

        Parameters: 
        file_name (str): The name of the zip file to be extracted
        """
        try:
            with ZipFile(file_name, 'r') as zip:
                zip.extractall()
            logging.info("Zip File {file_name} Extraction Successful".format(file_name=file_name))
        except Exception as err:
            logging.error("Zip File {file_name} Extraction Failed".format(file_name=file_name))
            logging.error("Exception: {err}".format(err=err))


def main():
    conn = Connection()
    parse = HandleXML()
    util = UtilityFunctions()
    conn.get_file_from_web(URL, 'Intermediate.xml')
    doc_link = parse.get_first_link_from_xml('Intermediate.xml')
    conn.get_file_from_web(doc_link, 'Data.zip')
    util.unzip_file('Data.zip')
    parse.parse_xml_document('DLTINS_20210117_01of01.xml', SaveFile(),'Info.csv')
    conn.store_file_to_s3_bucket('Info.csv')

if __name__ == '__main__':
    logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s %(filename)s %(levelname)s %(message)s')
    logging.getLogger().setLevel(logging.DEBUG)
    main()
