#!/usr/bin/env python

"""
Query condor history or status from ElasticSearch
"""

import datetime
import json
import pprint
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

import dateutil.parser
import pytz
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

parser = ArgumentParser(
    description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter
)
parser.add_argument(
    "--site", nargs="+", default=None, help="restrict query to these glidein sites"
)
parser.add_argument(
    "--before",
    default=datetime.datetime.now(pytz.UTC),
    type=dateutil.parser.parse,
    help="emit records only before this date",
)
parser.add_argument(
    "--after",
    default=datetime.datetime.now(pytz.UTC) - datetime.timedelta(days=12),
    type=dateutil.parser.parse,
    help="emit records only after this date",
)
parser.add_argument(
    "--all", default=False, action="store_true", help="emit all keys in JSON output"
)
parser.add_argument(
    "-l", "--limit", default=None, type=int, help="number of records to emit"
)
parser.add_argument("query", choices=["jobs", "slots"], help="index to query")
args = parser.parse_args()

client = Elasticsearch("http://elk-1.icecube.wisc.edu:9200", timeout=10)

if args.query == "jobs":

    s = (
        Search(using=client, index="condor")
        .filter("range", EnteredCurrentStatus={"lte": args.before,},)
        .filter("range", JobCurrentStartDate={"gte": args.after,})
        .filter(
            "query_string",
            query='Requestgpus:[1 TO *] AND NOT Owner: pyglidein AND NOT Cmd:"/usr/bin/condor_dagman"',
            analyze_wildcard=True,
        )
    )
    if args.site:
        sites = " OR ".join([f'"{site}"' for site in args.site])
        s = s.filter(
            "query_string",
            query=f"MATCH_EXP_JOBGLIDEIN_ResourceName: ({sites})",
            analyze_wildcard=True,
        )
    s = s.sort({"EnteredCurrentStatus": {"order": "desc", "unmapped_type": "boolean"}})

else:
    s = (
        Search(using=client, index="condor_status")
        .filter("range", DaemonStartTime={"gte": args.after,},)
        .filter("range", LastHeardFrom={"lte": args.before,},)
        .filter("range", TotalGPUs={"gt": 0})
    )
    if args.site:
        s = s.filter("match", site=" OR ".join([f'"{site}"' for site in args.site]))
    s = s.sort({"LastHeardFrom": {"order": "desc", "unmapped_type": "boolean"}})

if args.limit is None:
    gen = s.scan()
else:
    gen = s[: args.limit].execute()

if args.query == "jobs":
    accept = {
        "CommittedTime",
        "RemoteWallClockTime",
        "Requestgpus",
        "RequestCpus",
        "RequestMemory",
        "RequestDisk",
        "LastHoldReason",
        "JobStatus",
        "GlobalJobId",
        "site",
    }.__contains__
    if args.all:
        accept = lambda f: True

    def hit_to_dict(hit):
        slot, glidein_id, host = hit.LastRemoteHost.split("@")
        return {
            **{k: v for k, v in hit.to_dict().items() if accept(k)},
            **{"slot": slot, "glidein_id": glidein_id, "host": host},
            **{
                "start_time": hit.JobCurrentStartDate,
                "end_time": hit.EnteredCurrentStatus,
            },
        }


else:
    accept = {
        "TotalCpus",
        "TotalDisk",
        "TotalGPUs",
        "TotalGPUs_normalized",
        "TotalMemory",
        "site",
    }.__contains__
    if args.all:
        accept = lambda f: True

    def hit_to_dict(hit):
        slot, glidein_id, host = hit.Name.split("@")

        return {
            **{k: v for k, v in hit.to_dict().items() if accept(k)},
            **{"slot": slot, "glidein_id": glidein_id, "host": host},
            **{"start_time": hit.DaemonStartTime, "end_time": hit.LastHeardFrom},
        }


for hit in gen:
    print(json.dumps(hit_to_dict(hit)))
