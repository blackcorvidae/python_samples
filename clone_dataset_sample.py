from threading import Thread
import re
import uuid
from random import randint
import time
import datetime
from datetime import timedelta
import signal
import csv, os, sys, getopt
from pymongo import MongoClient
from bson.dbref import DBRef
from bson.objectid import ObjectId
from copy import deepcopy
import htmlentitydefs

import logging
import logging.handlers
import logging.config

#logging.basicConfig(filename='monqcle_clone.log',level=logging.DEBUG)
logging.config.fileConfig('log.conf')

logging.debug('This message should go to the log file')
logging.info('So should this')
logging.warning('And this, too')

question_lookup = {}
record_lookup = {}

pid = str(os.getpid())
pidfile = "/tmp/monqcle_clone.pid"

if os.path.isfile(pidfile):
    logging.error( "%s already exists, exiting" % (pidfile) )
    sys.exit()
file(pidfile, 'w').write(pid)

#term handler
def signal_term_handler(signal, frame):
    logging.warning( 'got SIGTERM' )
    os.remove(pidfile)
    logging.warning( 'cleaned up PID, exiting...' )
    sys.exit(0)
    
signal.signal(signal.SIGTERM, signal_term_handler)
signal.signal(signal.SIGINT, signal_term_handler)

class CloneDataset(Thread):
    
    def __init__(self, val, row, dataset, new_dataset):
        ''' Constructor. '''
        #logging.info("Constructor called")
        Thread.__init__(self)
        
        self.val = val
        self.row = row
        
        self.dataset = dataset
        self.new_dataset = new_dataset
        
        global record_lookup
        
        #PROD
        host = "mongodb://fake:fakepassword@ds051222222224-a0.mongolab.com:51224,ds051224-a1.mongolab.com:51224/sym_lawatlas_api_dev?replicaSet=rs-ds051224" #"localhost"
        datasets_host = "mongodb://fake:fakepassword@ds051222222224-a0.mongolab.com:51224,ds051224-a1.mongolab.com:51224/datasets?replicaSet=rs-ds051224" #"localhost"
        
        
        self.sym_client = MongoClient(host)
        self.my_symfony_db = self.sym_client.sym_lawatlas_api_dev
        self.datasets_client = MongoClient(datasets_host)
        self.my_dataset_import_db = self.datasets_client.datasets
        self.TAG_RE = re.compile(r'<[^>]+>')
        self.html_pattern = re.compile("&(\w+?);")
        self.day_delta = timedelta(days=1)

    def add_citation(self, citation, law):
        #look up and add citation
        #logging.info("new citation")
        u = str(uuid.uuid1())
        laws = []
        #logging.info("Law I want to cite")
        #print law
        law_dbref = DBRef("Law", ObjectId(law), "sym_lawatlas_api_dev")
        laws.append( law_dbref )
        #strip pincites, store original in path, look for urls
        path = ""
        urls = self.find_url_in_text(citation)
        if urls:
            path = ', '.join(str(url) for url in urls)
        else:
            path = citation
        citation = re.sub('\(([A-Za-z0-9 ]+?)\)','', citation.rstrip())
        cited = self.my_symfony_db.Citation.insert({"coder": "global", "description": citation, "laws": laws, "title": citation, "text": citation, "path": path, "markup_id": u})
        return cited

    def add_law(self, dataset, law, jurisdictions, effective, through, title):
        #add law
        #logging.info("Add Law")
        jurisdictions_dbrefs = []
        jurisdiction_names = ''
        for jurisdiction in jurisdictions:
            if jurisdiction is not None:
                jurisdiction_names = jurisdiction_names + jurisdiction["name"] + " - "
                jurisdictions_dbrefs.append( DBRef( 'Jurisdiction', ObjectId( jurisdiction["_id"] ), "sym_lawatlas_api_dev" )  )
        #ready the law, the insert and update
        law_html = self.html_entity_decode(law)
        law_text = self.remove_tags(law_html)
        law_id = self.my_symfony_db.Law.insert({"text": law_text,
                                                  "html": law_html,
                                                  "datasets":self.datasets,
                                                  "jurisdictions": jurisdictions_dbrefs,
                                                  "version": 0,
                                                  "effective": effective,
                                                  "through": through,
                                                  "title": title + jurisdiction_names})

        #update with series_root equals _id
        updated_series = self.my_symfony_db.Law.update(
            {"_id": ObjectId(law_id)},
            {
                "$set": {
                    "series_root": str(law_id)
                }
            }
        )

        return law_id

    def add_row(self, fixed_row, dataset_name):
        #add law
        fixed_row_doc = self.my_dataset_import_db[dataset_name].insert(fixed_row)
        return fixed_row_doc

    def remove_tags(self, ztext):
        return self.TAG_RE.sub('', ztext)

    def html_entity_decode_char(self, m, defs=htmlentitydefs.entitydefs):
        try:
            return defs[m.group(1)]
        except KeyError:
            return m.group(0)

    def html_entity_decode(self, zstring):
        return self.html_pattern.sub(self.html_entity_decode_char, zstring)

    def find_url_in_text(self, my_string):
        z_search = re.findall(r'(https?://\S+)', my_string)
        if z_search:
            return z_search
        else:
            return []
        
    def copyCitation(self, citation):
        #logging.info( "Citation to copy" )
        #print citation
        
        citation_doc2 = deepcopy(citation)
        citation_doc2["_id"] = None
        del citation_doc2["_id"]
        
        #fix datasets
        citation_doc2["datasets"] = []
        new_dataset_dbref = DBRef("Dataset", self.new_dataset["_id"], "sym_lawatlas_api_dev")
        citation_doc2["datasets"].append(new_dataset_dbref)
        citation_doc2["dataset_ids"] = []
        citation_doc2["dataset_ids"].append(str(self.new_dataset["_id"]))
        
        #fix question ids
        #citation_doc2["question_ids"] = None
        new_question_ids = []
        for qi in citation_doc2["question_ids"]:
            #print "question id"
            #print qi
            if qi in question_lookup:        
                #print "i found it!"
                #print question_lookup[qi]
                new_question_ids.append(question_lookup[qi])
        citation_doc2["question_ids"] = new_question_ids
                
        
        #fix record_ids
        #citation_doc2["record_ids"] = None
        citation_doc2["record_ids"] = []
        citation_doc2["record_ids"].append(str(self.row["_id"]))
        
        new_citation_id = self.my_symfony_db.Citation.insert(citation_doc2)
        
        return new_citation_id

    def run(self):
        #fixed array
        fixed_import = deepcopy(self.row)
        fixed_import["version"] = 1
        
        old_id = str(self.row["_id"])
        
        
        #save row, update later, so we have an ID for the citations
        
        #print "This rows citations"
        #print fixed_import["_CITATIONS"]
        
        #print "Citation Lookup, to fix..."
        #print citation_lookup
        
        if "_CITATIONS" in fixed_import:
            # print fixed_import["_CITATIONS"]
            try:
                for citekey, citation in fixed_import["_CITATIONS"].iteritems():
                    #print "FIX, this..."
                    #print citation
                    new_citations = []
                    for c in citation:
                        new_citations.append(citation_lookup[c])

                    fixed_import["_CITATIONS"][citekey] = new_citations
                    fixed_import[citekey] = new_citations
            except:
                logging.warning("no key....")
        
        #update row here
        del fixed_import['_id']
        
        #print "Add to DB"
        #print self.new_dataset["slug"]
        new_record = self.add_row(fixed_import, "%s.rows" % (self.new_dataset["slug"]))
        
        #print "New Record"
        #print new_record
        record_lookup[old_id] = str(new_record)
        


def update_migration_list(clone_request, dataset, successful):
        #add law
        logging.info("Update CloneDataset List")
        
        if successful:
            dataset_dbref = DBRef("Dataset", ObjectId(dataset["_id"]), "sym_lawatlas_api_dev")
            
            updated_migration_item = my_symfony_db.CloneRequest.update(
                {"_id":ObjectId(clone_request["_id"])},
                {
                    "$set": {
                        "auto_clone_done": True,
                        #"migrate_level_3": True,
                        "dataset": dataset_dbref,
                        "auto_clone_do": False,
                        "auto_cloned_on": datetime.datetime.utcnow(),
                        "note":"%s cloned successfully." % (dataset["title"])
                    }
                }
            )
            #print updated_migration
            logging.info("CloneDataset List Updated.")
            
            #print preview_item
            updated_migration = True
        else:
            updated_migration = False
            logging.warning("CloneDataset List WAS NOT Updated.")

        return updated_migration


def put_question_in_order(question, is_child = False):
    global sorted_questions
    global childrenz
    #print '------------ put_question_in_order ---------------'
    #print question["question"]
    if is_child:
        logging.info("Children are in nested array so don't add them here")
        #print "<>DO NOT Add to List"
        #print question["question"]
            
        if question in sorted_questions:
            sorted_questions.remove(question)
            #print "Already in LIST, removed"
    else:
        #print "<>"
        if question not in sorted_questions:
            
            qid = str(question["_id"])
            
            if qid in childrenz:
                print "THIS IS A CHILD"
            else:
                print "<>Add to List"
                print question["question"]
                sorted_questions.append(question)
                
                if "children" in question:
                    for kid in question["children"]:
                        childrenz.append(kid.id)
                
        
def copyDataset(dataset):

    dataset_doc = my_symfony_db.Dataset.find_one({"_id":ObjectId(dataset.id)})
    #print "Dataset to copy"
    #print dataset_doc
    
    #make new slug
    #print dataset_doc["slug"]
    
    ts = int(time.time())
    new_slug = "%s-%s" % (dataset_doc["slug"], ts)
    #print new_slug

    dataset_doc2 = deepcopy(dataset_doc)
    
    dataset_doc2["_id"] = None
    del dataset_doc2["_id"]
    
    global question_lookup
    dataset_doc2["questions"] = None
    #iterate, copy and link questions
    #del dataset_doc2["questions"]
    
    global childrenz
    childrenz = []
     
    count_out_questions = 1
    
    
    global dataset_questions
    dataset_questions = []
    for q in dataset_doc['questions']:
        #print q
        
#         print " +++ "
#         print count_out_questions
        count_out_questions = count_out_questions + 1
#         print " +++ "
        
        if q.id not in childrenz:
            new_question = copyQuestion(q)
            new_question_dbref = DBRef("Question", ObjectId( new_question ), "sym_lawatlas_api_dev")
        
        if new_question_dbref not in dataset_questions:
            dataset_questions.append(new_question_dbref)
#         else:
#             print "CHIDLDLDHDLDHDIREN"
    dataset_doc2["questions"] = dataset_questions
    
    
    #print "Question Tree -->"
    #print question_lookup
    
    dataset_doc2["slug"] = new_slug
    #dataset_doc2.save()
    #print "Clone dataset"
    #print dataset_doc2
    
    #insert
    new_dataset_id = my_symfony_db.Dataset.insert(dataset_doc2)
    #new_dataset_id = "0"
    return new_dataset_id
    
def copyQuestion(question, is_db_ref = False):
    global childrenz
    
    global dataset_questions
    
    question_doc = my_symfony_db.Question.find_one({"_id":ObjectId( question.id )})
    questionidz = question.id
    question_doc2 = deepcopy(question_doc)
    question_doc2["_id"] = None
    del question_doc2["_id"]
    
    if "responses" in question_doc:
        question_doc2["responses"] = None
        response_lookup = {}
        new_responses = []
        for r in question_doc["responses"]:
            #print r
            new_response = copyResponse(r)
            new_response_dbref = DBRef("Response", ObjectId( new_response ), "sym_lawatlas_api_dev")
            new_responses.append(new_response_dbref)
        question_doc2["responses"] = new_responses
        
    if "children" in question_doc:
        question_doc2["children"] = None
        
        new_questions = []
        for q in question_doc["children"]:
#             print "Child to Clone"
#             print q
#             
            childrenz.append(q.id)
            
            new_question = copyQuestion(q, True)
            new_question_dbref = DBRef("Question", ObjectId( new_question ), "sym_lawatlas_api_dev")
            new_questions.append(new_question_dbref)
            if new_question_dbref not in dataset_questions:
                dataset_questions.append(new_question_dbref)
            
        question_doc2["children"] = new_questions
    
    #Leave the slug as a lookup table for the value when you iterate the row, but switch keys to the id, after update
    #question_doc2["slug"] = new_slug
    #question_doc2.save()

    #insert
    #new_question_id = "541b271bb3783d6f6c8b4567"
    new_question_id = my_symfony_db.Question.insert(question_doc2)
    
    
    #update question tree
    #print "Update question tree"
    old_id = str(question_doc.get('_id'))
    #print "old_id"
    #print old_id
    question_lookup[old_id] = str(new_question_id)
    #print question_lookup
    
    
    #update slug to be id
    
    return new_question_id

def copyResponse(response):
    #print "Response Id"
    #print response.id
    response_doc = my_symfony_db.Response.find_one({"_id":ObjectId(response.id)})
    #print "Response to copy"
    #print response_doc
    
    response_doc2 = deepcopy(response_doc)
    response_doc2["_id"] = None
    del response_doc2["_id"]
    #print "Clone response"
    #print response_doc2
    
    #insert
    #new_response_id = "541b271bb3783d6f6c8b4567"
    new_response_id = my_symfony_db.Response.insert(response_doc2)
    
    return new_response_id

def getCitations(dataset, new_dataset):
    #print "All Citations"
    for cite in my_symfony_db.Citation.find({'datasets.$id': ObjectId(dataset["_id"])}):
        #print "cite"
        #print cite
        copyCitation(cite, new_dataset)
                
def copyCitation(citation, new_dataset):
    #print "Citation to copy"
    #print citation
    
    citation_doc2 = deepcopy(citation)
    citation_doc2["_id"] = None
    del citation_doc2["_id"]
    
    #fix datasets
    citation_doc2["datasets"] = []
    new_dataset_dbref = DBRef("Dataset", new_dataset["_id"], "sym_lawatlas_api_dev")
    citation_doc2["datasets"].append(new_dataset_dbref)
    citation_doc2["dataset_ids"] = []
    citation_doc2["dataset_ids"].append(str(new_dataset["_id"]))
    
    #fix question ids
    #citation_doc2["question_ids"] = None
    new_question_ids = []
    if "question_ids" in citation_doc2:
        for qi in citation_doc2["question_ids"]:
            #print "question id"
            #print qi
            if qi in question_lookup:        
                #print "i found it!"
                #print question_lookup[qi]
                new_question_ids.append(question_lookup[qi])
        citation_doc2["question_ids"] = new_question_ids
            
    
    #fix record_ids
    #citation_doc2["record_ids"] = None
    
    #records haven't been created yet, leave as lookup table
    #citation_doc2["record_ids"] = []
    #citation_doc2["record_ids"].append(str(self.row["_id"]))
    
    
    
    #print "Cloned citation"
    #print citation_doc2
    
    #insert
    #new_citation_id = "541b271bb3783d6f6c8b4567"
    new_citation_id = my_symfony_db.Citation.insert(citation_doc2)
    
    #update lookup table
    old_id = str(citation.get('_id'))
    citation_lookup[old_id] = str(new_citation_id)
    
    
    return new_citation_id

def updateCitations(new_dataset):
    #print "Update All Citations"
    for cite in my_symfony_db.Citation.find({'datasets.$id': ObjectId(new_dataset["_id"])}):
        #print "update cite"
        #print cite
        updateCitation(cite)

def updateCitation(citation):
    #print "Citation to update"
    #print citation
    #fix record_ids
    new_record_ids = []
    if "record_ids" in citation:
        for record_id in citation["record_ids"]:
            #print record_id
            try:
                #print record_lookup[record_id]
                new_record_ids.append(record_lookup[record_id])
            except:
                logging.warning( "no key...no citation updates..")
        #citation["record_ids"] = None
        #citation["record_ids"] = []
        #citation["record_ids"] = new_record_ids
        updated_citation = my_symfony_db.Citation.update(
                {"_id": ObjectId(citation["_id"])},
                {
                    "$set": {
                        "record_ids": new_record_ids
                    }
                }
            )
        
        logging.info( "Citation is UPDATED here")
        #for updated_citation_here in my_symfony_db.Citation.find({'_id': ObjectId(citation["_id"])}):
        #    print updated_citation_here
        
        #print updated_citation
        
        return updated_citation
    else:
        logging.error("updateCitation failed - no 'record_ids' key.")
        return False

    
# Run following code when the program starts
if __name__ == '__main__':
    try:
        dataset_name = ''
        new_dataset_name = ''
        start_time = time.time()
        
        #PROD
        host = "mongodb://fake:fakepassword@ds0111-a0.mongolab.com:51224,ds051224-a1.mongolab.com:51224/sym_lawatlas_api_dev?replicaSet=rs-ds051224"
        datasets_host = "mongodb://fake:fakepassword@ds0111-a0.mongolab.com:51224,ds051224-a1.mongolab.com:51224/datasets?replicaSet=rs-ds051224"
        
        
        
        #host = 'mongodb://localhost'
        #datasets_host = 'mongodb://localhost'
        lawatlas_host = "mongodb://fake:fakepassword@candidate.00000000019.mongolayer.com:10477,candidate.18.mongolayer.com:10439/datasets?replicaSet=set-53bea23520a65acd1100240d"
        
        sym_client = MongoClient(host)
        my_symfony_db = sym_client.sym_lawatlas_api_dev
        
        datasets_client = MongoClient(datasets_host)
        my_dataset_export_db = datasets_client.datasets
        
        #{"auto_migrate_do":True, "auto_migrate_done":{"$ne":True} }
        while True:
            for m in my_symfony_db.CloneRequest.find({"auto_clone_do":True, "auto_clone_done":{"$ne":True} }):
                
                #print m
                
                dataset = m["dataset"]
                dataset_doc = my_symfony_db.Dataset.find_one({"_id":ObjectId(dataset.id)})
        
                slug = dataset_doc["slug"]
                
                new_dataset_id = copyDataset(dataset)
                new_dataset_doc = my_symfony_db.Dataset.find_one({"_id":ObjectId(new_dataset_id)})
                
                #citations
                global citation_lookup
                citation_lookup = {}
                getCitations(dataset_doc, new_dataset_doc)
                
                #print "Citation lookup"
                #print citation_lookup
                
                #print new_dataset_id
                
                export_dataset = "%s.rows" % (slug)
                dnum = 0
                logging.info("Threading clone process")
                for export in my_dataset_export_db[export_dataset].find():
                    # print export
                    
                    myThreadObLoop = CloneDataset(4, export, dataset_doc, new_dataset_doc)
                    myThreadObLoop.setName('Thread ' + str(dnum) )
                    myThreadObLoop.start()
                    myThreadObLoop.join()
                    
                    dnum = dnum + 1
                
                
                updateCitations(new_dataset_doc)
                
                #print "Record Tree-->"
                #print record_lookup
                record_lookup = {}
                
                        
                #print "Question Tree -->"
                #print question_lookup
                question_lookup = {}
                
                
    #             #update the list on success              
                successful = True   
                update_migration_list(m, new_dataset_doc, successful) 
                
                
            logging.info( "Finished searching for clones, waiting 10 seconds." )
            time.sleep(10)
            
        
        logging.info('Main Terminating...')
        logging.info("--- %s seconds ---" % (time.time() - start_time))
    finally:
        if os.path.isfile(pidfile):
            os.unlink(pidfile)
