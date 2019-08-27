from kafka import KafkaProducer
import argparse
import logging
import os
import time
from json import dumps 

sources = [
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_00_3.jpg','label':3},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_13_7.jpg','label':7},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_12_5.jpg','label':5},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_03_0.jpg','label':0},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_05_6.jpg','label':6},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_02_8.jpg','label':8},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_16_5.jpg','label':5},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_18_8.jpg','label':8},
        {'url':'https://raw.githubusercontent.com/EldritchJS/cifar10_challenge/master/images/cifar10_04_6.jpg','label':6}
        ]

def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('creating kafka producer')    
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                             value_serializer=lambda x: 
                             dumps(x).encode('utf-8'))
    logging.info('finished creating kafka producer')

    while True:
        for source in sources:
            producer.send('images', value=source)
            time.sleep(15.0)

def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default

def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-python producer')
    parser = argparse.ArgumentParser(description='producer some stuff on kafka')
    parser.add_argument(
            '--brokers',
            help='The bootstrap servers, env variable KAFKA_BROKERS',
            default='kafka:9092')
    parser.add_argument(
            '--topic',
            help='Topic to write to, env variable KAFKA_TOPIC',
            default='images')
    args = parse_args(parser)
    main(args)
    logging.info('exiting')


        
