import json
import pytest

from botocore.errorfactory import ClientError
from datetime import datetime
from datetime import timedelta

from .conftest import ObjectUsers
from .util import random_bucket_name


def test_governance_lock(users: ObjectUsers):

    # Prepare two users and a shared bucket
    owner = users.create()
    guest = users.create()

    bucket_name = random_bucket_name()

    # Object locking only works if we enable it during creation. There's no
    # way to do it afterwards.
    owner.s3_client.create_bucket(
        Bucket=bucket_name,
        ObjectLockEnabledForBucket=True
    )

    # Additionally we need to configure the default retention.
    owner.s3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            'ObjectLockEnabled': 'Enabled',
            'Rule': {
                'DefaultRetention': {
                    'Mode': 'GOVERNANCE',
                    'Years': 10,
                }
            }
        }
    )

    # This might be Ceph-specific. Here we disable two things for everyone:
    #
    # - The ability to override the governance retention.
    # - The ability to delete object versions.
    #
    # A guest user is added with a limited set of actions.
    #
    owner.s3_client.put_bucket_policy(
        Bucket=bucket_name,
        Policy=json.dumps({
            'Version': '2012-10-17',
            'Statement': [
                {
                  "Effect": "Deny",
                  "Principal": {
                    "AWS": [
                      "*"
                    ]
                  },
                  "Action": [
                        "s3:BypassGovernanceRetention",
                        "s3:DeleteObjectVersion",
                  ],
                  "Resource": "*"
                },
                {
                    'Effect': 'Allow',
                    'Principal': {
                        'AWS': f'arn:aws:iam:::user/{guest.id}',
                    },
                    'Action': [
                        's3:PutObject',
                        's3:PutObjectAcl',
                        's3:GetObject',
                        's3:GetObjectAcl',
                        's3:DeleteObject',
                    ],
                    'Resource': [
                        '*',
                    ]
                }
            ]
        })
    )

    # Upload a test key
    owner.s3_client.put_object(
        Bucket=bucket_name,
        Body='0xdeadbeef',
        Key='test-key'
    )

    # Ensure the lock mode has been set
    data = owner.s3_client.head_object(
        Bucket=bucket_name,
        Key='test-key'
    )

    assert data['ObjectLockMode'] == 'GOVERNANCE'
    assert data['ObjectLockRetainUntilDate'].replace(tzinfo=None) \
        >= (datetime.utcnow() + timedelta(days=360*10))

    # Make sure the additional user can access s3
    obj = guest.s3_client.head_object(
        Bucket=bucket_name,
        Key='test-key',
    )

    # Governance retention does not mean we cannot delete an object...
    guest.s3_client.delete_object(
        Bucket=bucket_name,
        Key='test-key',
    )

    # ...we just can't delete a version, even if we try to force it.
    with pytest.raises(ClientError):
        guest.s3_client.delete_object(
            Bucket=bucket_name,
            Key='test-key',
            VersionId=obj['VersionId'],
            BypassGovernanceRetention=True,
        )

    # The same goes for the owner (interestingly, this seems to be idempotent)
    owner.s3_client.delete_object(
        Bucket=bucket_name,
        Key='test-key',
        BypassGovernanceRetention=True,
    )

    # But even the owner cannot bypass the retention
    with pytest.raises(ClientError):
        owner.s3_client.delete_object(
            Bucket=bucket_name,
            Key='test-key',
            VersionId=obj['VersionId'],
            BypassGovernanceRetention=True,
        )
