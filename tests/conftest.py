from __future__ import annotations

import json
import os
import secrets
import traceback

from boto3.session import Session
from cloudscale import Cloudscale  # type: ignore
from functools import cached_property
from pydantic import BaseModel
from pydantic import Field
from pytest import fixture
from typing import Dict


class Model(BaseModel):

    class Config:
        arbitrary_types_allowed = True
        keep_untouched = (cached_property, )


class ObjectUsers(Model):
    """ Creates object users that can later be cleaned up. """

    api: Cloudscale
    region: str
    users: Dict[str, ObjectUser] = Field(default_factory=dict)

    @classmethod
    def from_api_token(cls, api_token: str, region: str) -> ObjectUsers:
        return cls(api=Cloudscale(api_token=api_token), region=region)

    def create(self) -> ObjectUser:
        user = self.api.objects_user.create(f's3f-{secrets.token_hex(8)}')

        self.users[user['id']] = ObjectUser(
            region=self.region,
            id=user['id'],
            access_key=user['keys'][0]['access_key'],
            secret_key=user['keys'][0]['secret_key'],
        )

        return self.users[user['id']]

    def cleanup(self):
        for id, user in self.users.items():
            user.cleanup()
            self.api.objects_user.delete(id)


class ObjectUser(Model):
    region: str
    id: str
    access_key: str
    secret_key: str

    @property
    def endpoint_url(self):
        return f'https://objects.{self.region}.cloudscale.ch'

    @cached_property
    def s3_session(self):
        return Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    @cached_property
    def s3_client(self):
        return self.s3_session.client(
            service_name='s3',
            endpoint_url=self.endpoint_url,
        )

    def cleanup(self):

        for bucket in self.s3_client.list_buckets().get('Buckets', ()):
            try:
                self.cleanup_bucket(bucket['Name'])
            except Exception:
                traceback.print_exc()

    def cleanup_bucket(self, bucket_name: str):

        # Make sure we got the rights to delete
        self.s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps({
                'Version': '2012-10-17',
                'Statement': [
                    {
                      "Effect": "Allow",
                      "Principal": {
                        "AWS": [
                          "*"
                        ]
                      },
                      "Action": [
                            "s3:BypassGovernanceRetention",
                            "s3:DeleteObjectVersion",
                            "s3:DeleteObject",
                      ],
                      "Resource": "*"
                    },
                ]
            })
        )

        def delete_objects(objs):
            for obj in objs:
                self.s3_client.delete_object(
                    Bucket=bucket_name,
                    Key=obj['Key'],
                    VersionId=obj['VersionId'],
                )

        delete_objects(
            self.s3_client.list_objects(Bucket=bucket_name).get(
                'Contents', ()))

        delete_objects(
            self.s3_client.list_object_versions(Bucket=bucket_name).get(
                'Versions', ()))

        delete_objects(
            self.s3_client.list_object_versions(Bucket=bucket_name).get(
                'DeleteMarkers', ()))

        self.s3_client.delete_bucket(Bucket=bucket_name)


@fixture(scope='function')
def users():
    factory = ObjectUsers.from_api_token(
        api_token=os.environ["CLOUDSCALE_API_TOKEN"],
        region=os.environ["CLOUDSCALE_REGION"],
    )

    yield factory
    factory.cleanup()


@fixture(scope='function')
def user(factory):
    return factory.create_user()
