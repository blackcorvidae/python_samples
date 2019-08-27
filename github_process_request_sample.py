import os, sys
import signal
import tornado.ioloop
import tornado.web
import tornado.options
from github import Github
import json
import re
from pymongo import MongoClient
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson import json_util
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import logging
import graypy

import traceback

# my_logger = logging.getLogger('monqcle_webhook_class_logger')
# my_logger.setLevel(logging.DEBUG)
# log_handler = graypy.GELFHandler('52.99.99.177', 12201)
# my_logger.addHandler(log_handler)
# my_logger.debug('MonQcle Webhook Class, running...')

secret = "nonodontSharesecrets"

org_repos = []

r = "monocle"
r_full = "legalscienceio/%s" % (r)
org_repos.append(r_full)

r = "monqcle"
r_full = "legalscienceio/%s" % (r)
org_repos.append(r_full)

#r = "monqcle_node"
#r_full = "legalscienceio/%s" % (r)
#org_repos.append(r_full)

#print org_repos


def process_monqclebot_issues(query, trigger, issue_id, issue_state):
    
    issues = get_issues(issue_id, issue_state)
    
    
    for issue in issues:
        use_was_notified = False
        #print issue
        #print issue.id
        labels = issue.get_labels()
        for label in labels:
            #print label.name
            if label.name == "monqclebot-notified":
                #print "User has already been NOTIFIED BY MONQCLEBOT."
                use_was_notified = True
        #print issue.state
        ##print issue.body
        
        if issue.state == "closed" and use_was_notified:
            #print "This ticket is closed and the user has already been notified.  YAY!!!"
            q = 0
        else:
            #pattern = r"On behalf of: .*"
            pattern = r"\(([^)]+)\)"
            try:
                result = re.findall(pattern, issue.body)
                #print "Found?"
                #print result
            except TypeError as e:
                #print "not a string or buffer, so skip it."
                result = []
            except:
                e = sys.exc_info()[0]
                result = []
            if len(result) > 0:
                #print "REGEX 'On behalf of:' ---------------->"
                #print result
                for u in result:
                    usr = get_monqcle_user(u)
                    #print "User"
                    #print usr
                    if usr is not None:
                        usrj = list(usr) #json.dumps(list(usr), sort_keys=True, indent=4, default=json_util.default)
                        if len(usrj) > 0:
                            #print usrj[0]
                            #print usrj[0]["email"]
                            usr_email = usrj[0]["email"]
                        else:
                            usr_email = u
                    else:
                        usr_email = u
                        
                    #collect comments in digest
                    message_body = "<p><strong>%s</strong></p>\n<p>%s</p>\n" % (issue.title, issue.body)
                    for comment in issue.get_comments():
                        #print comment.body
                        message_body = "%s\n<p>--------------------</p>\n<p>%s</p>\n<em>(<a href='%s'>%s</a> - @%s)</em>\n" % (message_body, comment.body, comment.html_url, comment.created_at, comment.user.login)
                    
                    #is open or closed
                    issue_msg = "<p>This issue is <strong>%s</strong></p>." % (issue.state)
                    message_body = "%s\n<p>--------------------</p>\n%s\n" % (message_body, issue_msg)
                    
                    if not use_was_notified:
                        if send_notification(usr_email, "MonQcle - Issue Digest", message_body):
                            #print "MONQCLEBOT notified user."
                            #apply label
                            if issue.state == "closed":
                                issue.add_to_labels("monqclebot-notified")
#             else:
#                 print "CAn't find no users...."
            #print '<----------------------------------------------------------------------------------------------------------->'
    return True



    
def add_issue(issue):
    g = Github("8888888888888")
    if issue == "null":
        return False
    my_repo = g.get_repo(r_full, lazy=False)
    new_issue = my_repo.create_issue(issue)
    return new_issue.number

def get_issues(id, state):
    g = Github("8888888888888")
    #print "ISSUES ------------------**************------------------ "
    issues = []
     
     
    if id is not None: #get issue by ID and state
        #print "issue by ID"
        issues = g.search_issues(str(id), repo=r_full, type='Issues', updated='>2018-01-01T00:00:00Z')   
    else:
        #print "ALL issues"
        issues = g.search_issues('', repo=r_full, type='Issues', updated='>2018-01-01T00:00:00Z')

    
    
    return issues
    

def get_monqcle_user(username):
    #PROD
    host = "mongodb://fake:fakepass@ds05asdfasdfasdf1224-a0.mongolab.com:51224,ds051224-a1.mongolab.com:51224/sym_lawatlas_api_dev?replicaSet=rs-ds051224"
    datasets_host = "mongodb://fake:fakepass@ds051asdfasdfasdfasdf224-a0.mongolab.com:51224,ds051224-a1.mongolab.com:51224/datasets?replicaSet=rs-ds051224"
    sym_client = MongoClient(host)
    my_symfony_db = sym_client.sym_lawatlas_api_dev
    datasets_client = MongoClient(datasets_host)
    my_datasets_db = datasets_client.datasets
    monqcle_user = my_symfony_db.User.find({"username":username })
    return monqcle_user


def send_notification(email, message_subject, message_body):
    # me == my email address
    # you == recipient's email address
    me = "bot@legalscience.io"
    you = email  
    #you = "user10@legalscience.io"   
    
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = message_subject.encode("utf-8")
    msg['From'] = me
    msg['To'] = you
    
    message_body = message_body.encode("utf-8")
    
    # Create the body of the message (a plain-text and an HTML version).
    text = message_body # "Hi!\nHow are you?\nHere is the link you wanted:\nhttps://www.python.org"
    html = """\
    <html>
      <head></head>
      <body>
           %s
        </p>
      </body>
    </html>
    """ % (message_body)
    
    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    
    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)
    
    # Send the message via local SMTP server.
    s = smtplib.SMTP('localhost')
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    try:
        s.sendmail(me, you, msg.as_string())
    except:
        s.quit()
        return False
    s.quit()        
    return True   
