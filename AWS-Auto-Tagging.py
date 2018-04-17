import boto3
from botocore.client import Config
import os
import json

def print_output(message):
    #This function is an alternative to standard "print" which also adds the text to an SNS output variable (sent later)
    global snsoutput
    snsoutput += message + "\t\r\n"
    print message

def read_s3_input(filename):
    #This function gets the account-specific JSON file from S3
    client = boto3.client('s3', config=Config(signature_version='s3v4'))
    response = client.get_object(Bucket=os.environ['bucketname'],Key=filename)
    return json.loads(response['Body'].read())

def get_account_inventory():
    #This function reads in the four JSON files from S3 and returns their inventory
    inventory = {}
    filename = "individualaccounts/nocostcenterjson/" + awsAccountNumber + "-inventory.json"
    inventory['nocostcenterresources'] = read_s3_input(filename)
    filename = "individualaccounts/nocostcenterjson/" + awsAccountNumber + "-snapshots-inventory.json"
    inventory['nocostcentersnapshots'] = read_s3_input(filename)
    filename = "individualaccounts/json/" + awsAccountNumber + "-inventory.json"
    inventory['resources'] = read_s3_input(filename)
    filename = "individualaccounts/json/" + awsAccountNumber + "-snapshots-inventory.json"
    inventory['snapshots'] = read_s3_input(filename)
    return inventory

def get_aws_account_id():
    #This function returns the AWS account number
    client = boto3.client('sts')
    account_id = client.get_caller_identity()["Account"]
    return account_id

def send_sns_report(subject,message,topic):
    #This function reports actions taken via SNS
    client = boto3.client('sns')
    response = client.publish(
        TopicArn=topic,
        Message=message,
        Subject=subject
    )

    return response

def get_active_regions():
    #This function considers whether or not the region has untagged Resources

    regions = []

    for region in inventory['nocostcenterresources']:
        regionresources = 0
        for resourcetype in inventory['nocostcenterresources'][region]:
            regionresources += len(inventory['nocostcenterresources'][region][resourcetype])
        regionresources += len(inventory['nocostcentersnapshots'][region]['ebssnapshot'])
        if regionresources > 0:
            regions.append(region)

    return regions

def index_ec2_instances():
    #This function indexes all of the EC2 instances so we don't have to keep searching through them
    instances = {}
    for region in regions:
        instances[region] = {}
        for instance in inventory['resources'][region]['ec2instance']:
            instances[region][instance['InstanceId']] = instance
    return instances

def index_ebs_volumes():
    #This function indexes all of the EBS volumes so we don't have to keep searching through them
    volumes = {}
    for region in regions:
        volumes[region] = {}
        for volume in inventory['resources'][region]['ebsvolume']:
            volumes[region][volume['VolumeId']] = volume
    return volumes

def create_ec2_or_ebs_tag(resource,key,value,region):
    #This function creates a tag on a resource
    client = boto3.client('ec2',region_name=region)

    try:
        response = client.create_tags(
            Resources=[resource],
            Tags=[
            {
                'Key': key,
                'Value': value
            },
        ])

    except Exception as e:
        print_output("Couldn't create tag: " + str(e))

def stop_ec2_instance(instance,region):
    #This function stops an EC2 instance. Currently only a dryrun for safety.
    client = boto3.client('ec2',region_name=region)

    try:
        response = client.stop_instances(InstanceIds=[instance], DryRun=True)
        return "Stopped " + region + " instance " + instance
    except Exception as e:
        return "Couldn't stop " + region + " instance " + instance + ": " + str(e)

def process_ebs_volumes():
    #This function will consider all EBS volumes in active regions, and copy CostCenter tags from instances
    global actionstaken
    print_output("Checking EBS Volumes for tags obtainable from EC2 instances")

    instances = index_ec2_instances()

    for region in regions:
        for volume in inventory['resources'][region]['ebsvolume']:

            #If there's no tags, create a blank list
            try:
                volume['Tags']
            except KeyError:
                volume['Tags'] = []

            #Find the Volume CostCenter tag
            volumecostcenter = ""
            for tag in volume['Tags']:
                if tag['Key'].lower() == "costcenter":
                    volumecostcenter = tag['Value']

            #If a CostCenter was not found, find the attached instance
            if volumecostcenter == "":
                try:
                    attachedinstance = instances[region][volume['Attachments'][0]['InstanceId']]
                except IndexError:
                    #This means there's no attached instance. Initialize a blank variable to move on.
                    attachedinstance = {}

                #If there's no tags, create a blank list
                try:
                    attachedinstance['Tags']
                except KeyError:
                    attachedinstance['Tags'] = []

                #Find the Instance CostCenter tag
                instancecostcenter = ""
                for tag in attachedinstance['Tags']:
                    if tag['Key'].lower() == "costcenter":
                        instancecostcenter = tag['Value']

                #If a CostCenter was found, apply it to the Volume (live and in our data)
                if instancecostcenter != "":
                    print_output("Applying CostCenter " + instancecostcenter + " to " + region + " volume " + volume['VolumeId'])
                    actionstaken += 1
                    create_ec2_or_ebs_tag(volume['VolumeId'],"CostCenter",instancecostcenter,region)
                    newtag = {
                        "Key": "CostCenter",
                        "Value": instancecostcenter
                    }
                    volume['Tags'].append(newtag)

def process_ebs_snapshots():
    #This function will consider all EBS snapshots in active regions, and copy CostCenter tags from instances
    global actionstaken
    print_output("Checking EBS Snapshots for tags obtainable from EBS volumes")

    volumes = index_ebs_volumes()

    for region in regions:
        for snapshot in inventory['snapshots'][region]['ebssnapshot']:

            #If there's no tags, create a blank list
            try:
                snapshot['Tags']
            except KeyError:
                snapshot['Tags'] = []

            #Find the Volume CostCenter tag
            snapshotcostcenter = ""
            for tag in snapshot['Tags']:
                if tag['Key'].lower() == "costcenter":
                    snapshotcostcenter = tag['Value']

            #If a CostCenter was not found, find the attached instance
            if snapshotcostcenter == "":
                try:
                    sourcevolume = volumes[region][snapshot['VolumeId']]
                except KeyError:
                    #This means the source volume no longer exists. Create a blank variable to move on.
                    sourcevolume = {}

                #If there's no tags, create a blank list
                try:
                    sourcevolume['Tags']
                except KeyError:
                    sourcevolume['Tags'] = []

                #Find the Volume CostCenter tag
                volumecostcenter = ""
                for tag in sourcevolume['Tags']:
                    if tag['Key'].lower() == "costcenter":
                        volumecostcenter = tag['Value']

                #If a CostCenter was found, apply it to the Snapshot
                if volumecostcenter != "":
                    print_output("Applying CostCenter " + volumecostcenter + " to " + region + " snapshot " + snapshot['SnapshotId'])
                    actionstaken += 1
                    create_ec2_or_ebs_tag(snapshot['SnapshotId'],"CostCenter",volumecostcenter,region)


def process_ec2_instances():
    #This function will check EC2 instances in all active regions, and stop any untagged ones - CERTAIN ACCOUNTS ONLY

    #This is the list of accounts we will perform these actions on. Provided by Mike Izumi
    activeaccounts = [ "245173971655", "106804898684", "532164495553", "042377186981", "012127553673", "169702226014" ]

    #Just end the function now if our account number is not the allowed list
    if awsAccountNumber not in activeaccounts:
        print_output("This account is not enabled for stopping EC2 instances")
        return 0

    global actionstaken
    print_output("Checking for untagged EC2 Instances")

    #We'll report instances stopped separately to the main report
    instancesstopped = 0
    instanceoutput = ""

    #Loop through all instances in the nocostcenter list, and stop anything found
    for region in regions:
        for instance in inventory['nocostcenterresources'][region]['ec2instance']:
            #Rackspace Passport Bastions don't get tagged. Ignore those.
            if "rackspace passport bastion" not in instance['Name'].lower():
                actionstaken += 1
                instancesstopped += 1
                reportline = stop_ec2_instance(instance['Id'],region)
                print_output(reportline)
                instanceoutput += reportline + "\t\r\n"

    #If any instances were stopped, send a separate email to report those
    if instancesstopped > 0:
        instanceoutput += "Instances stopped: " + str(instancesstopped)
        subject = "Untagged EC2 instances stopped for account " + awsAccountNumber
        try:
            topic = os.environ['snstopic2']
        except KeyError:
            topic = ""
        if topic != "":
            send_sns_report(subject,instanceoutput,topic)

def process_account_rules():
    #This function automatically applies a blanket set of tags for anything in specified accounts

    #This is the list of accounts we will perform these actions on. Provided by Mike Izumi
    activeaccounts = {
        "436307203483": [
            {
                "Key": "CostCenter",
                "Value": "ITL/DGB-11"
            },
            {
                "Key": "Service",
                "Value": "MacOS"
            },
            {
                "Key": "Owner",
                "Value": "Eric Kelling"
            }
        ]
    }

    #Just end the function now if our account number is not the allowed list
    if awsAccountNumber not in activeaccounts:
        print_output("This account does not have default tags to apply to remaining resources")
        return 0

    global actionstaken
    print_output("Automatically applying account tags to untagged resources")

    #Loop through all instances in the nocostcenter list, and tag anything found
    for region in regions:

        if len(inventory['nocostcentersnapshots'][region]['ebssnapshot']) > 0:
            client = boto3.client('ec2',region_name=region)
            for resource in inventory['nocostcentersnapshots'][region]['ebssnapshot']:
                actionstaken += 1
                print_output ("Tagging ebssnapshot " + resource['Id'])
                try:
                    response = client.create_tags(
                        Resources=[resource['Id']],
                        Tags=activeaccounts[awsAccountNumber]
                    )
                except Exception as e:
                    print_output("Couldn't create tag: " + str(e))

        for resourcetype in inventory['nocostcenterresources'][region]:

            if resourcetype in [ "ec2instance", "ebsvolume", "ebssnapshot" ]:
                client = boto3.client('ec2',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])
                    try:
                        response = client.create_tags(
                            Resources=[resource['Id']],
                            Tags=activeaccounts[awsAccountNumber]
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

            if resourcetype == "redshift":
                client = boto3.client('redshift',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])
                    try:
                        response = client.create_tags(
                            ResourceName=resource['Id'],
                            Tags=activeaccounts[awsAccountNumber]
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

            if resourcetype == "s3bucket":
                client = boto3.client('s3',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])
                    try:
                        response = client.put_bucket_tagging(
                            Bucket=resource['Id'],
                            Tagging={
                                'TagSet': activeaccounts[awsAccountNumber]
                            }
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

            if resourcetype == "elbv1":
                client = boto3.client('elb',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])
                    try:
                        response = client.add_tags(
                            LoadBalancerNames=[resource['Id']],
                            Tags=activeaccounts[awsAccountNumber]
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

            if resourcetype == "elbv2":
                client = boto3.client('elbv2',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])
                    try:
                        response = client.add_tags(
                            ResourceArns=[resource['Id']],
                            Tags=activeaccounts[awsAccountNumber]
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

            if resourcetype == "rdsinstance":
                client = boto3.client('rds',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])
                    try:
                        response = client.add_tags_to_resource(
                            ResourceName=resource['Id'],
                            Tags=activeaccounts[awsAccountNumber]
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

            if resourcetype == "elasticache":
                client = boto3.client('elasticache',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])
                    resourcearn = "arn:aws:elasticache:" + region + ":" + awsAccountNumber + ":cluster:" + resource['Id']
                    try:
                        response = client.add_tags_to_resource(
                            ResourceName=resourcearn,
                            Tags=activeaccounts[awsAccountNumber]
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

            if resourcetype == "lambda":
                client = boto3.client('lambda',region_name=region)
                for resource in inventory['nocostcenterresources'][region][resourcetype]:
                    actionstaken += 1
                    print_output ("Tagging " + resourcetype + " " + resource['Id'])

                    #Lambda uses a completely different tag format. Reformat.
                    lambdatags = {}
                    for tag in activeaccounts[awsAccountNumber]:
                        lambdatags[tag['Key']] = tag['Value']

                    try:
                        response = client.tag_resource(
                            Resource=resource['Id'],
                            Tags=lambdatags
                        )
                    except Exception as e:
                        print_output("Couldn't create tag: " + str(e))

def lambda_handler(event, context):
    #This is the function actually called by Lamda

    #We're going to count how many actions are taken, and only send the SNS report if > 0
    global actionstaken
    actionstaken = 0

    #Initialize a variable where output will go. This is so we can "print" but also send the whole output via SNS
    global snsoutput
    snsoutput = ""

    #Figure out the AWS account number we're running under
    global awsAccountNumber
    awsAccountNumber = get_aws_account_id()

    #Read the nocostcenter inventory from S3
    global inventory
    inventory = get_account_inventory()

    #Get the list of regions with untagged resources
    global regions
    regions = get_active_regions()

    #Work through EBS volumes
    process_ebs_volumes()

    #Work through EBS Snapshots
    process_ebs_snapshots()

    #Work through EC2 instances
    process_ec2_instances()

    #Work through account-wide rules
    process_account_rules()

    #Send the output via SNS
    if actionstaken > 0:
        print_output("Actions taken: " + str(actionstaken))
        subject = "Mystique-autotag output" + awsAccountNumber
        try:
            topic = os.environ['snstopic']
        except KeyError:
            topic = ""
        if topic != "":
            send_sns_report(subject,snsoutput,topic)

    print_output("Script completed successfully.")
    return "Script completed successfully."

#This calls the function if we're running from local CLI instead of from Lambda
if __name__ == '__main__':
    lambda_handler({},{})
