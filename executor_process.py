# -*- coding: utf-8 -*-

import sys
import os
import struct
import ctypes
from datetime import datetime, timedelta, date
import time
import json
import gc
import base64
import logging
import numpy as np
from kafka import KafkaProducer
import xml.etree.ElementTree as ET
import socket
import configparser
import cv2
from filelock import FileLock
from log import configure_logging


def init(sysPath):
    global decLib, decodeProc, osLib, mInference, isInit, encLib, setRgbData, encodeProc, sockLib, sockInit, sockAppend, sockAppendMeta

    sys.path.append('/home/Streaming_Server_new/yolo2')
    sys.path.append('/home/Streaming_Server_new')

    
    logger = logging.getLogger(__name__)
    configure_logging('test.log', 'CRITICAL')

    # lock acquire for init yolo model on specific gpu
    
    for item in sysPath.split(","):
        sys.path.insert(0, item)

    file_path = sys.path[0] + '/foo.lock'
    lock_path = file_path + '.lock'

    lock = FileLock(lock_path, timeout=-1)
    with lock:
        open(file_path, 'a')

    lock.acquire()
    try:
        import yolov2 as infer
    finally:
        lock.release()
        
    # init C++ libraries for image processing
    
    prop = configparser.RawConfigParser()
    prop.read(sys.path[0]+'/SparkConfig.properties')

    brokerList = prop.get('KafkaConfig', 'metadata.broker.list').split(",")

    hAddr = prop.get('HbaseConfig', 'hbase.address')
    hTableName = prop.get('HbaseConfig', 'hbase.table')

    osLib = ctypes.cdll.LoadLibrary('libc.so.6')

    decLib = ctypes.cdll.LoadLibrary(prop.get('CdllConfig', 'decoder.path'))
    decLib.init()

    decodeProc = decLib.decodeProc
    decodeProc.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_int]
    decodeProc.restype = ctypes.POINTER(ctypes.c_uint8)

    sockLib = ctypes.cdll.LoadLibrary(
        prop.get('CdllConfig', 'sockclient.path'))

    sockInit = sockLib.init
    sockInit.argtypes = [ctypes.c_char_p, ctypes.c_int]

    sockAppend = sockLib.append
    sockAppend.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_int]

    sockAppendMeta = sockLib.appendMeta
    sockAppendMeta.argtypes = [ctypes.POINTER(
        ctypes.c_char), ctypes.c_uint8, ctypes.c_int, ctypes.c_int, ctypes.c_ulonglong, ctypes.c_uint, ctypes.c_int, ctypes.POINTER(ctypes.c_char)]

    sockAppendInfo = sockLib.appendInfo
    sockAppendInfo.argtypes = [ctypes.POINTER(
        ctypes.c_char), ctypes.c_float, ctypes.c_float, ctypes.c_float, ctypes.c_float]

    sockSend = sockLib.sockSend
    sockClose = sockLib.sockClose
    
    # get yolo v2 instance 
    mInference = infer.YOLOV2()
    isInit = True
    

    
def proc(record, sysPath):
    
    start_time = time.time()

    if(isInit == False):
        init(sysPath)

    # parse kafka record (metadata + image frame)
        
    msg = record[1]
    msgByte = bytearray(msg)
    nalUnit = bytearray([0, 0, 0, 1])
    splitByte = msgByte.split(nalUnit)

    curFrame = 0
    isHeader = True

    spark_start = spark_end = time.time()
    pre_time = time.time() - start_time

    for sByte in splitByte:
        if isHeader == True:

            header = "".join(map(chr, sByte))
            
            # parse metadata 
            header = header.split('<?xml version="1.0"?>')[1]
            xml = header.split("</camera_db")[0].replace("<camera_db>", "")
            root = ET.fromstring(xml)
            cameraIp = root.findtext("cam_ip")

            time_sec = root.findtext("time_sec")
            time_millisec = root.findtext("time_millisec")
            active = root.findtext("active")
            route_ip = root.findtext("route_ip")
            route_port = root.findtext("route_port")

            cameraFps = root.findtext("cam_fps")
            cameraModelId = root.findtext("cam_model_id")
            cameraModelType = root.findtext("cam_model_type")
            cameraResolutionHeight = root.findtext("cam_resolution_height")
            cameraResolutionWidth = root.findtext("cam_resolution_width")
            view_location = root.findtext("view_location")

            isHeader = False
            continue

        dt = datetime.fromtimestamp(
            int(time_sec)).strftime('%Y%m%d%H%M%S')
        dt_ymd = dt[:8]
        dt_hms = dt[8:]
        dt_ms = dt[10:]

        # parse image frame 
        frameSize = len(sByte)+4
        if frameSize == 0 or frameSize <= 4:
            continue
        t1 = time.time()

        frameData = (ctypes.c_uint8 * (frameSize)).from_buffer(nalUnit+sByte)
        pointer = ctypes.cast(ctypes.addressof(frameData),
                              ctypes.POINTER(ctypes.c_uint8))
        
        # decode image frame
        result = decodeProc(pointer, frameSize)
        t2 = time.time()
        output("DECODE DURATION :: "+str(t2-t1)+" :: ")
        try:
            t1 = time.time()
            # transform C++ type image frame data -> python numpy type
            addr = ctypes.addressof(result.contents)
            buffer = np.ctypeslib.as_array(ctypes.cast(result, ctypes.POINTER(ctypes.c_ubyte)), shape=(720, 1280, 3))

            # append image frame to list 
            mInference.append_to_list(buffer)
            t2 = time.time()
            isExist = True

            fCount = fCount + 1
            break
        except Exception as e: 
            print("In Except :: ", e)
            pass
    print ("CHUCK COMPLETE\n")

    # append metadata to metadata list 
    bufferList.append({"msgByte": msgByte, "cameraIp": cameraIp, "time_sec": time_sec, "time_millisec": time_millisec, "routeIp": route_ip,
                        "routePort": route_port, "active": active, "cameraFps": cameraFps, "cameraModelId": cameraModelId,
                        "cameraModelType": cameraModelType, "cameraResolutionHeight": cameraResolutionHeight,
                        "cameraResolutionWidth": cameraResolutionWidth, "view_location": view_location})

    if fCount >= 1:
        print ('fCount :: ', fCount)
        done()
        spark_end = time.time()

    spark_time = spark_end - spark_start
    print ("CHUCK COMPLETE\n")
    
    
def done():
    # inference 
    inferenceResult = mInference.inference()

    t2 = time.time()

    # parse inference result
    for i in range(len(inferenceResult)):

        t1 = time.time()
        inferenceObj = []
        print ("DETECT COUNT ::: ", len(inferenceResult[i]))

        for temp in inferenceResult[i]:

            dt = datetime.fromtimestamp(int(bufferList[i]['time_sec'])).strftime('%Y%m%d%H%M%S')
            dt_ymd = dt[:8]
            dt_hms = dt[8:]
            dt_ms = dt[10:]
            dt_h = dt[8:10]

            # inference result : rgb, bbox
            rgb = base64.b64encode(temp['img'])
            rect = bytes(str(temp['bbox']), 'utf-8')
            
            # append metadata
            temp['camera_ip'] = bufferList[i]['cameraIp']
            temp['time_sec'] = int(bufferList[i]['time_sec'])
            temp['time_millisec'] = int(bufferList[i]['time_millisec'])
            
            dt = bytes(dt, 'utf-8')
            dt_h = bytes(dt_h, 'utf-8')
            
            if temp['type'] == 'person':
                p_count = int(p_count) + 1

                row_key = '%s_%s_p' % (bufferList[i]['cameraIp'], dt_ymd)
                row_key = bytes(row_key, 'utf-8')
                p_count = bytes(str(p_count), 'utf-8')
                p_dnum = bytes(str(p_dnum), 'utf-8')
                p_tnum = bytes(str(p_tnum), 'utf-8')

                send_data = bytearray(row_key + b'::' + rgb + b'::' + p_count + b'::' + rect + b'::' + feature_count
                                      + b'::' + p_dnum + b'::' + p_tnum + b'::' + dt_h + b'::' + dt)

                producer.send(topic='feature', value= send_data)
            
            elif temp['type'] == 'car':
                
                row_key = '%s_%s_c' % (bufferList[i]['cameraIp'], dt_ymd)
                row_key = bytes(row_key, 'utf-8')
                c_count = bytes(str(c_count), 'utf-8')
                c_dnum = bytes(str(c_dnum), 'utf-8')
                c_tnum = bytes(str(c_tnum), 'utf-8')

                send_data = bytearray(row_key + b'::' + rgb + b'::' + c_count + b'::' + rect + b'::' + feature_count
                                      + b'::' + c_dnum + b'::' + c_tnum + b'::' + dt_h + b'::' + dt)

                producer.send(topic='feature', value = send_data)
                print ('p_hbase_t ::::', time.time() - p_hbase_t)

            inferenceObj.append(temp)
    
        t2 = time.time()

    del bufferList[:]
    print ("RELEASE BUFFER :: ", bufferList)
    fCount = 0
    isExist = False
    
    

