import time

from kafka import KafkaConsumer
import happybase
import sys

class HBaseConnect():
    def __init__(self, host):
        self.host = host
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = happybase.Connection(self.host, timeout=200000)

        except Exception as ex:
            self.conn.close()

    def connect_table(self, table):
        self.tbl_conn = self.conn.table(table)

    def put_row_data(self, row_key, datadict):
        self.tbl_conn.put(row_key, datadict)

    def conn_close(self):
        self.conn.close()
        

if __name__ == "__main__":

    consumer = KafkaConsumer('feature', bootstrap_servers=[
        '192.168.1.101:6667'], api_version=(0, 9))

    hbase = HBaseConnect('192.168.1.101')
    hbase.connect_table('feature')

    print ('start consumer')

    try:
        for message in consumer:

            t1 = time.time()

            split_data = message.value.split(b'::')

            row_key = split_data[0]
            print ('row_key ::', row_key)

            if row_key.split(b'_')[2] == b'p':

                rgb = split_data[1]
                id = split_data[2]
                rect = split_data[3]
                count = split_data[4].decode('utf-8')
                dnum = split_data[5].decode('utf-8')
                tnum = split_data[7].decode('utf-8')
                dt_h = split_data[8].decode('utf-8')
                dt = split_data[8].decode('utf-8')

                datadict = {'cf:rgb|%s|%s' % (dt, count): rgb, 'cf:rect|%s|%s' % (dt, count): rect,
                            'cf:regdate|%s|%s' % (dt, count): dt, 'cf:id|%s|%s' % (dt, count): str(id),
                            'cf:dnum': dnum, 'cf:tnum': tnum}

                hbase.put_row_data(row_key, datadict)

            elif row_key.split(b'_')[2] == b'c':

                rgb = split_data[1]
                id = split_data[2]
                rect = split_data[3]
                count = split_data[4].decode('utf-8')
                dnum = split_data[5].decode('utf-8')
                tnum = split_data[7].decode('utf-8')
                dt_h = split_data[8].decode('utf-8')
                dt = split_data[8].decode('utf-8')

                datadict = {'cf:rgb|%s|%s' % (dt, count): rgb, 'cf:rect|%s|%s' % (dt, count): rect,
                            'cf:regdate|%s|%s' % (dt, count): dt, 'cf:id|%s|%s' % (dt, count): str(id),
                            'cf:dnum': dnum, 'cf:tnum': tnum}

                hbase.put_row_data(row_key, datadict)

            elif row_key.split(b'_')[2] == b'f':

                rgb = split_data[1]
                rect = split_data[2]
                id = split_data[3]
                count = split_data[4].decode('utf-8')
                dt_h = split_data[5].decode('utf-8')
                dt = split_data[6].decode('utf-8')
                prob = split_data[7].decode('utf-8')

                datadict = {
                    'cf:rgb|%s|%s' % (dt, count): rgb,
                    'cf:rect|%s|%s' % (dt, count): rect,
                    'cf:regdate|%s|%s' % (dt, count): dt,
                    'cf:faceId|%s|%s' % (dt, count): id,
                    'cf:prob|%s|%s' % (dt, count): prob
                }

                hbase.put_row_data(row_key, datadict)

    except Exception as e:
        print("In Except :: ", e)
        pass
