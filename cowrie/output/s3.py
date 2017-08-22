"""
Send downloaded/uplaoded files to S3 (or compatible)
"""

from __future__ import division, absolute_import

import os

from twisted.internet import defer

from txaws.credentials import AWSCredentials
from txaws.service import AWSServiceRegion

import cowrie.core.output


class Output(cowrie.core.output.Output):

    def __init__(self, cfg):
        credentials = AWSCredentials(
            cfg.get("output_s3", "access_key_id"),
            cfg.get("output_s3", "secret_access_key")
        )
        region = AWSServiceRegion(
            creds=credentials,
            region=cfg.get("output_s3", "region"),
            s3_uri=cfg.get("output_s3", "endpoint")
        )
        self.client = region.get_s3_client()
        self.bucket = cfg.get("output_s3", "bucket")
        cowrie.core.output.Output.__init__(self, cfg)

    def start(self):
        pass

    def stop(self):
        pass

    def write(self, entry):
        if entry["eventid"] == "cowrie.session.file_download":
            self.upload(entry['shasum'], entry["outfile"])

        elif entry["eventid"] == "cowrie.session.file_upload":
            self.upload(entry['shasum'], entry['outfile'])

    @defer.inlineCallbacks
    def upload(self, shasum, filename):
        print("Uploading file with sha {} ({}) to S3".format(shasum, filename))
        if not self.client.head_object(self.bucket, shasum):
            with open(filename, 'rb') as fp:
                yield self.client.put_object(self.bucket, shasum, fp.read(), 'application/octet-stream')
