#!/usr/bin/env python
#
# Copyright 2015 The ChEMBL group.
# Author: Nathan Dedman <ndedman@ebi.ac.uk>
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Implementation of an S3-like storage server based on local files.

Useful to test features that will eventually run on S3, or if you want to
run something locally that was once running on S3.

We don't support all the features of S3, but it does work with the
standard S3 client for the most basic semantics. To use the standard
S3 client with this module:

    c = S3.AWSAuthConnection("", "", server="localhost", port=8888,
                             is_secure=False)
    c.create_bucket("mybucket")
    c.put("mybucket", "mykey", "a value")
    print c.get("mybucket", "mykey").body


Don't forget to increase ULIMIT!
"""

import bisect
import datetime
import hashlib
import os
import os.path
import urllib
import logging
import glob
import getpass
import re


from tornado import escape
from tornado import httpserver
from tornado import ioloop
from tornado import web
from pymongo import MongoClient
from pymongo import ASCENDING
import bson
from bson.binary import Binary

from tornado.log import enable_pretty_logging


def start(port,debug=False):
    """Starts the pymongo S3 server"""
    application = mongoS3(debug)
    http_server = httpserver.HTTPServer(application)
    
    # Utilize all CPUs
    if not debug:
        http_server.bind(port)
        http_server.start(0)
    else:
        enable_pretty_logging()
        http_server.listen(port)
    
    ioloop.IOLoop.current().start()


class mongoS3(web.Application):
    """Implementation of an S3-like storage server based on MongoDB using PyMongo    
    
    * Added compatibility with the s3cmd command line utility
    * File names of arbitrary length are supported (stored as meta data) - we're handling long IUPAC names!
    * Multipart upload suported
    
    """
    def __init__(self, debug=False):
        web.Application.__init__(self, [            
            (r"/", RootHandler),
            (r"/([^/]+)/(.+)", ObjectHandler),
            (r"/([^/]+)/", BucketHandler),
            (r"/ping",StatusHandler),
            (r'/(favicon.ico)', web.StaticFileHandler, {"path": ""}),
            
            # s3cmd
            ('http://s3.amazonaws.com/', s3cmdlHandler),
            (r"(http://.+.s3.amazonaws.com.*)", s3cmdlHandler),
            
        ],debug=debug)
        
        # Lazy connect the client
        self.client = MongoClient(connect=False) 
        self.S3 = self.client.S3
        self.metadata = self.client.metadata
    
    

class StatusHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("GET")
    # Send a simple 'PONG' to show we're alive!
    def get(self):
        self.set_header('Content-Type', 'application/json')
        self.finish({'response':'pong','UTC':datetime.datetime.now().isoformat()})
    
    
class BaseRequestHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("PUT", "GET", "DELETE", "HEAD","POST","OPTIONS")
    
    def _get_bucket_names(self):
        return self.application.S3.collection_names(include_system_collections=False)
    
    
         
    def render_xml(self, value,**kwargs):
        assert isinstance(value, dict) and len(value) == 1
        self.set_header("Content-Type", "application/xml; charset=UTF-8")
        name = value.keys()[0]
        parts = []
        parts.append('<' + escape.utf8(name) +' xmlns="http://s3.amazonaws.com/doc/2006-03-01/">')
        parts.append('<Owner><ID>'+getpass.getuser()+'</ID><DisplayName>'+getpass.getuser()+'</DisplayName></Owner>')
        self._render_parts(value.values()[0], parts)
        parts.append('</' + escape.utf8(name) + '>')

        if 'code' in kwargs.keys():
            self.set_status(kwargs['code'])
        self.finish('<?xml version="1.0" encoding="UTF-8"?>' +
            
                    ''.join(parts))

    def _render_parts(self, value, parts=[]):
        if isinstance(value, (unicode, bytes)):
            parts.append(escape.xhtml_escape(value))
        elif isinstance(value, int) or isinstance(value, long):
            parts.append(str(value))
        elif isinstance(value, datetime.datetime):
            parts.append(value.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        elif isinstance(value, dict):
            for name, subvalue in value.iteritems():
                if not isinstance(subvalue, list):
                    subvalue = [subvalue]
                for subsubvalue in subvalue:
                    parts.append('<' + escape.utf8(name) + '>')
                    self._render_parts(subsubvalue, parts)
                    parts.append('</' + escape.utf8(name) + '>')
        else:
            raise Exception("Unknown S3 value type %r", value)
        
    def _error(self,**kwargs):
        
        bucket_name = object_name = None
        
        if hasattr(self,'bucket_name'):
            bucket_name = self.bucket_name
        
        if hasattr(self,'object_name'):
            object_name = self.object_name
        
        
        s3errorcodes_bucket = {'NSK':'NoSuchKey','NSB':'NoSuchBucket','BNE':'BucketNotEmpty',"BAE":"BucketAlreadyExists"}
        s3errorcodes_object = {'NSB':'NoSuchBucket','NSK':'NoSuchKey'}
        
        errormessage_object = {404:'The specified key does not exist.'}
        errormessage_bucket = {404:{'NSB':'The specified bucket does not exist.'},409:{'BNE':'The bucket you tried to delete is not empty.','BAE':'The requested bucket name is not available. Please select a different name and try again.'}}
        
        if self.__class__.__name__== 'BucketHandler':
            s3errorcodes = s3errorcodes_bucket
            errormessage = errormessage_bucket
            bucket_name = self.bucket_name
            object_name = None
        
        if self.__class__.__name__== 'ObjectHandler':
            s3errorcodes = s3errorcodes_object
            errormessage = errormessage_object
            
        if hasattr(self,'s3cmd'):
            returnDict = {'Error':{}}
            errorDict = returnDict['Error']
            errorDict['Code'] = s3errorcodes[kwargs['s3code']]
            if self.__class__.__name__ == 'BucketHandler':
                errorDict['Message'] = errormessage[kwargs['code']][kwargs['s3code']]
            else:
                errorDict['Message'] = errormessage[kwargs['code']]
            errorDict['Resource'] = '/%s/%s' % (bucket_name,object_name)
            self.render_xml(returnDict,code=kwargs['code'])
            
        else:
            raise web.HTTPError(kwargs['code'])
    
    

class s3cmdlHandler(web.RequestHandler):    
    def prepare(self):
        # Handle s3 urls here
        self.s3cmd = True
        
        if self.application.settings['debug']:
            print "%s %s" % (self.__class__.__name__, self.request.method)
        
        s3match = re.match('(?:http://)(.+)(?:.s3.amazonaws.com\/)(.*)',self.request.uri)
        
        self.prefix = self.get_argument("prefix", u"")
        self.delimiter = self.get_argument("delimiter", u"")
        self.partNumber = self.get_argument("partNumber",u"")
        self.uploadId = self.get_argument("uploadId",u"")
        
                
        try:            
            bucket_name = s3match.group(1)
        except:
            bucket_name = False
        
        try:
            if s3match.group(2).startswith('?'):
                object_name = prefix
            else:
                object_name = s3match.group(2)
        except:
            object_name = False
        
        if object_name:
            if '?uploads' in object_name:
                self.uploads = True
            
            if '?delete' in object_name:
                self.delete = True
            
        if object_name:
            object_name = object_name.split('?')[0]
        
        if self.request.uri == 'http://s3.amazonaws.com/':
            self.__class__ = RootHandler
            
        if bucket_name and not object_name:
            self.__class__ = BucketHandler
            self.bucket_name = bucket_name

        if bucket_name and object_name:
            self.__class__ = ObjectHandler
            self.bucket_name = bucket_name
            self.object_name = object_name

            

class RootHandler(BaseRequestHandler):
    def get(self):
        buckets = []
        bucket_names = self._get_bucket_names()
        
        for bucket_name in bucket_names:           
            bucket_meta = self.application.metadata[bucket_name].find()
            buckets.append({                    
                "Name": bucket_name,
                "CreationDate":bucket_meta.next()['created'],
            })
        self.render_xml({"ListAllMyBucketsResult": {
            "Buckets": {"Bucket": buckets},
        }})

    

class BucketHandler(BaseRequestHandler):
    
    def _get_bucket_cursor(self,bucket_name):
        return self.application.S3[bucket_name]
    
    def _remove_bucket(self,bucket_name):
        self.application.S3[bucket_name].drop()
        self.application.metadata[bucket_name].drop()
    
    def get(self, bucket_name):
        if hasattr(self,'bucket_name'):
            bucket_name = self.bucket_name
            
        prefix = self.get_argument("prefix", u"")
        marker = self.get_argument("marker", u"")
        max_keys = int(self.get_argument("max-keys", 50000))
        terse = int(self.get_argument("terse", 0))
        
        if bucket_name not in self._get_bucket_names():
            self._error(code=404,s3code='NSB')
            return
        
        objects = []
        contents = []
        
        for bucket_object in self._get_bucket_cursor(bucket_name).find({'partNumber': None}):
                objects.append(bucket_object)
        
        start_pos = 0
        
        # FIX THIS!!
        if marker:
            start_pos = bisect.bisect_right(objects, marker, start_pos)
        if prefix:
            start_pos = bisect.bisect_left(objects, prefix, start_pos)

        truncated = False
        for _object in objects[start_pos:]:
            if not _object['object_name'].startswith(prefix):
                break
            if len(contents) >= max_keys:
                truncated = True
                break
            
            c = {"Key": _object['object_name'],"ETag":_object['md5']}
            if not terse:
                c.update({
                    "LastModified":_object['added'],
                    "Size":_object['size'],
                })
            contents.append(c)
            marker = _object['object_name']
        self.render_xml({"ListBucketResult": {
            "Name": bucket_name,
            "Prefix": prefix,
            "Marker": marker,
            "MaxKeys": max_keys,
            "IsTruncated": truncated,
            "Contents": contents
        }})

    def put(self, bucket_name):
        # Create bucket and metadata
        if hasattr(self,'bucket_name'):
            bucket_name = self.bucket_name
        
        if bucket_name in self._get_bucket_names():
            self._error(code=409,s3code='BAE')
            return
        
        self.application.S3.create_collection(bucket_name)
        self.application.metadata[bucket_name].insert({"created":datetime.datetime.utcnow()})
        self.application.S3[bucket_name].ensure_index([("partNumber",ASCENDING)])
        self.finish()

    def delete(self, bucket_name):
        if hasattr(self,'bucket_name'):
            bucket_name = self.bucket_name

         
        if bucket_name not in self._get_bucket_names():
            self._error(code=404,s3code='NSB')
            return
                        
        if self.application.S3[bucket_name].count() > 0:
            self._error(code=409,s3code='BNE')
            return
        
        self._remove_bucket(bucket_name)
        self.set_status(204)
        self.finish()
    
    def post(self, bucket_name):
        if hasattr(self,'bucket_name'):
            bucket_name = self.bucket_name
        
        if bucket_name not in self._get_bucket_names():
            self._error(code=404,s3code='NSB')
            return
        self._remove_bucket(bucket_name)
        self.set_status(204)
        self.finish()
        
    def head(self,bucket_name):    
        if hasattr(self,'bucket_name'):
            bucket_name = self.bucket_name

        if bucket_name not in self._get_bucket_names():
            self._error(code=404,s3code='NSB')
            return

        self.set_header('Date', '"%s"' % datetime.datetime.utcnow())
        self.finish()
        
    
class ObjectHandler(BaseRequestHandler):
    
    def _object_md5(self,bucket_object):
        object_md5 = hashlib.md5()
        object_md5.update(bucket_object)
        return object_md5.hexdigest()
    
    def _get_bucket_object(self,**kwargs):
        if '_id' in kwargs.keys():
            object_id = kwargs['_id']
            object_field = '_id'
        if 'object_name' in kwargs.keys():
            object_id = kwargs['object_name']
            object_field = 'object_name'
        
        if 'bucket_name' in kwargs.keys():
            bucket_name = kwargs['bucket_name']
        
        return self.application.S3[bucket_name].find_one({object_field:object_id},{'partNumber': None})
       
    def get(self,*args):
        
        if hasattr(self,'bucket_name') and hasattr(self,'object_name'):
            bucket_name = self.bucket_name
            object_name = self.object_name    
        else:
            bucket_name,object_name = args
            
        
        prefix = self.get_argument("prefix", u"")
        marker = self.get_argument("marker", u"")
        acl = self.get_argument("acl", u"")
        
        object_name = urllib.unquote(object_name)
        
        if bucket_name not in self._get_bucket_names():
            self._error(code=404,s3code='NSB')
            return
        
        bucket_object = self._get_bucket_object(bucket_name=bucket_name,object_name=object_name)
        
        if bucket_object:
            self.set_header("Content-Type", "application/unknown")
            self.set_header('etag', '"%s"' % bucket_object['md5'])
            self.set_header("Last-Modified", bucket_object['added'])
            
            if 'multipart' in bucket_object.keys():
                print "MULTIPART"
                self.set_header("Content-Length",bucket_object['size'])
                for parts in self.application.S3[bucket_name].find({'object_name':object_name},{'partNumber': {'$exists':'true'}}):
                        print parts['partNumber']
                        self.write(parts['object'])
                        self.flush()
                    
                self.finish()
            else:
                self.finish(bucket_object['object'])
        else:
            self._error(code=404,s3code='NSK')
            return

    def put(self, *args):
        
        if self.bucket_name and self.object_name:
            bucket_name = self.bucket_name
            object_name = self.object_name
        else:
            bucket_name,object_name = args
            
        original_name = urllib.unquote(object_name)

        if bucket_name not in self._get_bucket_names():
            self._error(code=404,s3code='NSB')
            return
        
        # Insert object and then calculate computed md5 of stored object, size, then update and return
        
        # If the object already exists, delete contents and add updated timestamp and update
        existance = self.application.S3[bucket_name].find({"object_name":original_name})       
        
        if existance.count() > 0 and self.partNumber == None:
            existance_id = existance.next()['_id']
            update_object = Binary(self.request.body)
            object_size = update_object.__len__()
            object_md5 = self._object_md5(update_object)
            self.application.S3[bucket_name].update({"_id":existance_id},{'$set': {'object':update_object,'md5':object_md5,'updated':datetime.datetime.utcnow(),'size':object_size}})
            self.set_header('etag', '"%s"' % object_md5)
            self.finish()
            return
        
        if self.partNumber:
            tobeinserted = {'object_name':original_name,'object':Binary(self.request.body),'partNumber':self.partNumber}
        else:
            tobeinserted = {'object_name':original_name,'object':Binary(self.request.body)}
            
        inserted_object_id = self.application.S3[bucket_name].insert_one(tobeinserted).inserted_id
        inserted_object = self._get_bucket_object(bucket_name=bucket_name,_id=inserted_object_id)
        
        object_size = inserted_object['object'].__len__()
        object_md5 = self._object_md5(inserted_object['object'])
        self.application.S3[bucket_name].update({'_id':inserted_object_id},{'$set': {'md5':object_md5,'updated':datetime.datetime.utcnow(),'added':datetime.datetime.utcnow(),'size':object_size}})
        self.set_header('etag', '"%s"' % object_md5)
        
        
        self.finish()
        
    def post(self, *args):
        # Add entry into bucket and flag as multipart upload
        if self.bucket_name and self.object_name:
            bucket_name = self.bucket_name
            object_name = self.object_name
        else:
            bucket_name,object_name = args
        
        if bucket_name not in self._get_bucket_names():
            self._error(code=404,s3code='NSB')
            return
        
        original_name = urllib.unquote(object_name)
        bucket_object = Binary(self.request.body)
        object_size = bucket_object.__len__()
        object_md5 = self._object_md5(bucket_object)
        
        if self.uploadId:
            # We have a multipart upload, so iterate over the parts to generate the md5 hash and calculate size
            # This is the last call made after the mutlipart upload with the uploadId
            mupmd5 = hashlib.md5()
            mupsize = 0
            for mup in self.application.S3[bucket_name].find({'object_name':object_name}):
                mupmd5.update(mup['object'])
                mupsize += mup['size']
                
            self.application.S3[bucket_name].insert_one({'object_name':object_name,'object':bucket_object,'multipart':True,'md5':mupmd5.hexdigest(),'size':mupsize,'added':datetime.datetime.utcnow(),'updated':datetime.datetime.utcnow(),})
        
        self.render_xml({"InitiateMultipartUploadResult": {
            "Bucket": bucket_name,
            "Prefix": self.prefix,
            "Key":object_name,
            "UploadId":object_name
        }})
        
    def delete(self, *args):
        
        if self.bucket_name and self.object_name:
            bucket_name = self.bucket_name
            object_name = self.object_name
        else:
            bucket_name,object_name = args
            
        original_name = urllib.unquote(object_name)
        
        bucket_object = self._get_bucket_object(bucket_name=bucket_name,object_name=object_name)
        
        if bucket_object:
            self.set_status(204)
            self.application.S3[bucket_name].remove({"_id":bucket_object['_id']})
            self.finish()
        else:
            self._error(code=404,s3code='NSK')
            return
        
    def head(self, *args):
        
        if hasattr(self,'bucket_name') and hasattr(self,'object_name'):
            bucket_name = self.bucket_name  
            object_name = self.object_name    
        else:
            bucket_name,object_name = args
        
        object_name = urllib.unquote(object_name)
        
        
        bucket_object = self._get_bucket_object(bucket_name=bucket_name,object_name=object_name)
        
        if bucket_object:
            self.set_header('etag', '"%s"' % bucket_object['md5'])
            self.set_header('last-modified', '"%s"' % bucket_object['updated'])
            self.finish()        
        else:
            self._error(code=404,s3code='NSK')
            return
        

        
if __name__ == "__main__":
    start(8080,debug=False)

