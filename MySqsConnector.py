# coding:utf-8
from boto3 import Session
import json
import logging
from error import SqsConnectionError

class SqsConnectionError(object):
    pass 

logger = logging.getLogger(__name__)

class MyServiceSqs(object):

    def __init__(self, **kwargs):
        """
        example kwargs = {'region':region_name,'access_key':'dadaderefa1432','secret_key':'jhfoiewhro43h53'}
        """
        self.region = kwargs['region']
        self.access_key = kwargs['access_key']
        self.secret_key = kwargs['secret_key']
        self.session = self.__session()

    def __session(self):
        try:
            session = Session(aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key,
                            region_name=self.region)
        except:
            raise SqsConnectionError("Failed to connect session in region{0}".format(self.region))

        return session

    def sqs_connect_queue(self, queue_name):
        """
        """
        try:
            sqs = self.session.resource('sqs')
         except:
            raise SqsConnectionError("Failed to retrieve queue{0}".format(queue_name))
        try:
            queue = sqs.get_queue_by_name(QueueName=queue_name)
        except:
            raise SqsConnectionError("Failed to retrieve queue {0} in{1}".format(queue_name))
        return queue


    def poll_message(self, queue_name):
        """
        get batch message from queue, return a list about meesage.body
        """
        queue = self.sqs_connect_queue(queue_name)
        response = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5)
        messages = []
        if not response:
            logger.info("{0} queue don't has any message Notification")
        try:
            for message in response:
                notice = json.loads(message.body)
                messages.append(notice['Message'])
                message.delete
        except:
            logger.error("Can't get message from queue{0}".format(queue))
        return messages