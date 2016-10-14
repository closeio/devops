"""Find AWS security groups not assigned to instances or ELBs."""

import boto3

client = boto3.client('ec2')

resp = client.describe_security_groups()


sgs = {}
for sg in resp['SecurityGroups']:
    sgs[sg['GroupId']] = sg['GroupName']
print "Total", len(sgs)

resp = client.describe_instances(MaxResults=999)

for r in resp['Reservations']:
    for i in r['Instances']:
        for sg in i['SecurityGroups']:
            if sg['GroupId'] in sgs:
                del sgs[sg['GroupId']]

print "After removing instances", len(sgs)

client = boto3.client('elb')
resp = client.describe_load_balancers(PageSize=399)
for elb in resp['LoadBalancerDescriptions']:
    for sg in elb['SecurityGroups']:
        if sg in sgs:
            del sgs[sg]
print "After removing elbs", len(sgs)


print
for sg in sgs:
    print sg, sgs[sg]
