#!/usr/bin/env python
from __future__ import division

from servicex.transformer.kafka_messaging import KafkaMessaging
from servicex.transformer.object_store_manager import ObjectStoreManager
from servicex.transformer.xaod_events import XAODEvents
from servicex.transformer.xaod_transformer import XAODTransformer

import pika
import pyarrow as pa
import pyarrow.parquet as pq

import requests
import argparse
import datetime
import json
import os
import sys

# Use for default kafka backends
default_brokerlist = "servicex-kafka-0.slateci.net:19092, " \
                     "servicex-kafka-1.slateci.net:19092," \
                     "servicex-kafka-2.slateci.net:19092"


def post_status_update(endpoint, status_msg):
    'Send a status back to the system'
    requests.post(endpoint+"/status", data={
        "timestamp": datetime.datetime.now().isoformat(),
        "status": status_msg
    })


def put_file_complete(endpoint, file_path, file_id, status,
                      num_messages=None, total_time=None, total_events=None,
                      total_bytes=None):
    'Post back that we have finished processing a file.'
    avg_rate = 0 if not total_time else total_events/total_time
    doc = {
        "file-path": file_path,
        "file-id": file_id,
        "status": status,
        "num-messages": num_messages,
        "total-time": total_time,
        "total-events": total_events,
        "total-bytes": total_bytes,
        "avg-rate": avg_rate
    }
    print("------< ", doc)
    if endpoint:
        requests.put(endpoint+"/file-complete", json=doc)


def write_branches_to_arrow(messaging, topic_name, file_path, file_id, attr_name_list,
                            chunk_size, server_endpoint, event_limit=None,
                            object_store=None):

    scratch_writer = None

    event_iterator = XAODEvents(file_path, attr_name_list)
    transformer = XAODTransformer(event_iterator)

    batch_number = 0
    total_events = 0
    total_bytes = 0
    for pa_table in transformer.arrow_table(chunk_size, event_limit):
        if object_store:
            if not scratch_writer:
                scratch_writer = _open_scratch_file(args.result_format, pa_table)
            _append_table_to_scratch(args.result_format, scratch_writer, pa_table)

        total_events = total_events + pa_table.num_rows
        batches = pa_table.to_batches(chunksize=chunk_size)

        for batch in batches:
            if messaging:
                key = file_path + "-" + str(batch_number)

                sink = pa.BufferOutputStream()
                writer = pa.RecordBatchStreamWriter(sink, batch.schema)
                writer.write_batch(batch)
                writer.close()
                messaging.publish_message(
                    topic_name,
                    key,
                    sink.getvalue())

                total_bytes = total_bytes + len(sink.getvalue().to_pybytes())

                avg_cell_size = len(sink.getvalue().to_pybytes()) / len(
                    attr_name_list) / batch.num_rows
                print("Batch number " + str(batch_number) + ", "
                      + str(batch.num_rows) +
                      " events published to " + topic_name,
                      "Avg Cell Size = " + str(avg_cell_size) + " bytes")
                batch_number += 1

    if object_store:
        _close_scratch_file(args.result_format, scratch_writer)
        print("Writing parquet to ", args.request_id, " as ", file_path.replace('/', ':'))
        object_store.upload_file(args.request_id, file_path.replace('/', ':'), "/tmp/out")
        os.remove("/tmp/out")

    print("===> Total Events ", total_events)
    print("===> Total Bytes ", total_bytes)

    if server_endpoint:
        post_status_update(server_endpoint, "File " + file_path + " complete")

    put_file_complete(server_endpoint, file_path, file_id, "success",
                      num_messages=batch_number, total_time="??",
                      total_events=total_events, total_bytes=total_bytes)



def presetup():
    'Called for setup before things start going'

def callback(channel, method, properties, body):
    transform_request = json.loads(body)
    _request_id = transform_request['request-id']
    _file_path = transform_request['file-path']
    _file_id = transform_request['file-id']
    _server_endpoint = transform_request['service-endpoint']

    print(f'Processing {_file_path}')
    try:
        result_root_file = write_root_file(file_path=_file_path)
        # write_branches_to_arrow(messaging=messaging, topic_name=_request_id,
        #                         file_path=_file_path, file_id=_file_id,
        #                         attr_name_list=columns,
        #                         chunk_size=chunk_size, server_endpoint=_server_endpoint,
        #                         object_store=object_store)
    except Exception as error:
        transform_request['error'] = str(error)
        channel.basic_publish(exchange='transformation_failures',
                              routing_key=_request_id + '_errors',
                              body=json.dumps(transform_request))
        put_file_complete(_server_endpoint, _file_path, _file_id,
                          "failure", 0, 0.0)
    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Transform xAOD files into flat n-tuples.')

    # parser.add_argument("--limit", dest='limit', action='store',
    #                     default=None,
    #                     help='Max number of events to process')

    parser.add_argument('--result-destination', dest='result_destination', action='store',
                        default='object-store', help='object-store is the only thing available at this time',
                        choices=['object-store'])

    parser.add_argument('--result-format', dest='result_format', action='store',
                        default='arrow', help='arrow, parquet', choices=['arrow', 'parquet'])

    parser.add_argument("--dataset", dest='dataset', action='store',
                        default=None,
                        help='JSON Dataset document from DID Finder')

    parser.add_argument('--rabbit-uri', dest="rabbit_uri", action='store',
                        default='host.docker.internal')

    parser.add_argument('--request-id', dest='request_id', action='store',
                        default=None, help='Request ID to read from queue')

    parser.add_argument('--code-path', dest="code_path", action='store', default='/code',
                        help='Path where the 6 files have been written containing the code that is to be compiled and run.')

    # Print help if no args are provided
    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()

    args = parser.parse_args()

    if args.result_destination == 'object-store':
        object_store = ObjectStoreManager(os.environ['MINIO_URL'],
                                          os.environ['MINIO_ACCESS_KEY'],
                                          os.environ['MINIO_SECRET_KEY'])
        print("Object store initialized to ", object_store.minio_client)

    # Get RabbitMQ set up 
    rabbitmq = pika.BlockingConnection(pika.URLParameters(args.rabbit_uri))
    _channel = rabbitmq.channel()

    # Do pre-running setup. This is done before we have to start grabbing any files off the queue
    presetup()

    # Next start picking files off the input queue.
    # Each operation is going to take a very long time (10-15 seconds or so), so don't
    # prefetch anything.
    _channel.basic_qos(prefetch_count=1)

    _channel.basic_consume(queue=args.request_id,
                            auto_ack=False,
                            on_message_callback=callback)
    print("Atlas C++ xAOD Transformer")
    _channel.start_consuming()
