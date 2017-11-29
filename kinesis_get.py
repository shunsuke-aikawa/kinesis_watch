import sys
import time
import json
import boto3
from boto3.session import Session
from  datetime import datetime




client = boto3.client('kinesis')


def get_kinesis(key):
    try:
        res = client.get_records(
            ShardIterator=key,
            Limit=123
        )
        return res

    except Exception as e:
       print('Kinesis put record excption')
       print(e)
       sys.exit



def get_iterator(name, date, id):
    t = datetime.strptime(date, '%Y/%m/%d')
    res = client.get_shard_iterator(
        StreamName=name,
        ShardId=id,
        ShardIteratorType='AT_TIMESTAMP',
        Timestamp=t
    )
    return res


def main(stream, date, shard_id):

    try:

        res = get_iterator(stream, date ,shard_id)

        itr = res['ShardIterator']

        while True:

            response = get_kinesis(itr)


            for record in response['Records']:
                print(record['Data'])


            nxt = response['NextShardIterator']

            time.sleep(2)
            if nxt == None or nxt == '':

                break

            itr = nxt


    except Exception as e:
        print('Exception exit')
        print(e)




if __name__ == '__main__':

    if len(sys.argv) < 3:
        error = "ERROR: Please enter  stream name and shard id\n"
        command = "{} python {} [stream name] [start date %Y/%m/%d] [shard id]".format(error, sys.argv[0])
        print(command)
        exit()

    print('----------start-------------')



    stream = sys.argv[1]
    date = sys.argv[2]

    if len(sys.argv) < 4:
        shard_id = 'shardId-000000000000'
    else:
        shard_id = sys.argv[3]

    main(stream, date, shard_id)

