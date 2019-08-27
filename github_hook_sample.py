import os, sys
import signal
import tornado.ioloop
import tornado.web
import tornado.options

from monqcle_github import process_monqclebot_issues

import json
import re
import logging
import graypy

import traceback

# my_logger = logging.getLogger('monqcle_webhook_server_logger')
# my_logger.setLevel(logging.DEBUG)
# log_handler = graypy.GELFHandler('2.9.5999992.113', 12201)
# my_logger.addHandler(log_handler)
# my_logger.debug('MonQcle Webhook API Server, running...')
# 
# logging.getLogger("tornado.access").addHandler(log_handler)
# logging.getLogger("tornado.application").addHandler(log_handler)
# logging.getLogger("tornado.general").addHandler(log_handler)

pid = str(os.getpid())
pidfile = "/tmp/monqcle_github_webhook.pid"

if os.path.isfile(pidfile):
    #logging.error( "%s already exists, exiting" % (pidfile) )
    #my_logger.error('MonQcle Producation API (Data) Server, PID already exists.')
    print "no system exit on pidfile check"
    #sys.exit()
file(pidfile, 'w').write(pid)

#term handler
def signal_term_handler(signal, frame):
    #my_logger.warning('MonQcle Github API Server, SIGTERM.')
    #logging.warning( 'got SIGTERM' )
    os.remove(pidfile)
    #my_logger.warning('MonQcle Github API Server, PID file removed, closing app.')
    #logging.warning( 'cleaned up PID, exiting...' )
    sys.exit(0)
    
signal.signal(signal.SIGTERM, signal_term_handler)
signal.signal(signal.SIGINT, signal_term_handler)


secret = "dontShareSecrets"

org_repos = []

r = "monocle"
r_full = "legalscienceio/%s" % (r)
org_repos.append(r_full)

r = "monqcle"
r_full = "legalscienceio/%s" % (r)
org_repos.append(r_full)


class MyAppException(tornado.web.HTTPError):

    pass

class GithubHandler(tornado.web.RequestHandler):
    def post(self):
        #print "GITHUB handler"
        #my_logger.debug('GITHUB handler, checking request...')
        #print json.loads(self.request.body)
        if self.request.body:
            try:
                json_data = json.loads(self.request.body)
                self.request.arguments.update(json_data)
                self.write(json_data)
                
                
                query = self.get_argument('query', None)
                trigger = self.get_argument('trigger', None)
                issue_id = self.get_argument('issue', None)
                issue_state = self.get_argument('state', None)
                
                if "issue" in json_data:
                    if "number" in json_data["issue"]:
                        issue_id = json_data["issue"]["number"]
                        query = self.get_argument('query', None)
                        trigger = self.get_argument('trigger', None)
                        issue_state = self.get_argument('state', None)
                    
                        
                process_monqclebot_issues(query, trigger, issue_id, issue_state)
            except ValueError:
                message = 'Unable to parse JSON.'
                self.send_error(400, message=message) # Bad Request
    get = post

class MainHandler(tornado.web.RequestHandler):
    def post(self):
        query = self.get_argument('query', None)
        trigger = self.get_argument('trigger', None)
        issue_id = self.get_argument('issue', None)
        issue_state = self.get_argument('state', None)
        
        process_monqclebot_issues(query, trigger, issue_id, issue_state)
        
        self.write("<h1>MonQcle Issues API</h1><img src='yodawg.jpg'/><br /><img src='yodawg_monocle.jpg'/>")
        self.finish()
    get = post
    
        
    


if __name__ == "__main__":
    try:
        application = tornado.web.Application([
            (r"/", MainHandler),
            (r"/webhook", GithubHandler),
            (r"/(yodawg.jpg)", tornado.web.StaticFileHandler, {'path':'./'}),
            (r"/(yodawg_monocle.jpg)", tornado.web.StaticFileHandler, {'path':'./'})
        ])
        application.listen(8881)
        tornado.ioloop.IOLoop.current().start()
#     except Exception:
#         logging.error("exception in asynchronous operation",exc_info=True)
#         sys.exit(1)
    finally:
        if os.path.isfile(pidfile):
            #my_logger.debug('MonQcle Github API Server, Unlinking PID and closing.')
            os.unlink(pidfile)
