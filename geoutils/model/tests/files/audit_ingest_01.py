"""This is a simple dictionary structure that presents a typical
:class:`geoutils.Auditer` ingest object structure.

In this case, age of audits AUDIT_01 > AUDIT_03 > AUDIT_02 > AUDIT_04

"""
AUDIT_01 = {
    'row_id': '09221956447764569211_ingest_daemon',
    'tables': {
        'audit': {
            'cf': {
                'cq': {
                    'ingest_daemon|start': '1415589090.12',
                    'ingest_daemon|finish': '1415589090.21',
                },
            },
        },
    },
}

AUDIT_02 = {
    'row_id': '09221956437718808093_ingest_daemon',
    'tables': {
        'audit': {
            'cf': {
                'cq': {
                    'ingest_daemon|start': '1415599135.88',
                    'ingest_daemon|finish': '1415599135.97',
                },
            },
        },
    },
}

AUDIT_03 = {
    'row_id': '09221956447616086912_ingest_daemon',
    'tables': {
        'audit': {
            'cf': {
                'cq': {
                    'ingest_daemon|start': '1415589238.57',
                    'ingest_daemon|finish': '1415589238.69',
                },
            },
        },
    },
}

AUDIT_04 = {
    'row_id': '09221956375225656441_dummy',
    'tables': {
        'audit': {
            'cf': {
                'cq': {
                    'dummy|whatever': 'blah',
                },
            },
        },
    },
}
