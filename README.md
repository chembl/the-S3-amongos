# the-S3-amongos
S3 (AWS Simple Storage Service) server clone using MongoDB, PyMongo and Tornado.

* Based on the original Facebook file server implementation: https://github.com/tornadoweb/tornado/blob/master/demos/s3server/s3server.py
* It has the basic programmatic support of the original, but also supports the s3cmd command line tool and mutlipart uploads. GridFS is not implemented so we use the default chunk size used by s3cmd (15Mb)
* Utilises all CPUs (debug=False)
* As of yet, does not feature ACLs or authentication (to come!)
* Multipart uploads appear as the contiguous orginal (although existing as 15Mb chunks)

Functional POC, but needs some code refactorying, cleanup, tests.
