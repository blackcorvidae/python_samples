# -*- coding: utf-8 -*-
__author__ = 'macbook'
import sys
import traceback
import time

# millis = int(round(time.time() * 1000))
# millis = 0


from py2neo import Graph
import py2neo
from py2neo import Graph, Node, Relationship

from selenium import webdriver

# g = Graph("http://neo4j:4234475a@ec2-54-145-193-41.compute-1.amazonaws.com:27474/db/data/", bolt_port=27687)
# g = Graph("http://neo4j:Ghwm5hao@localhost:7474/db/data/", bolt_port=7687)

browser = webdriver.Chrome()
browser2 = webdriver.Chrome()
browser3 = webdriver.Chrome()


try:
    browser.get('http://codes.ohio.gov/orc/')  # Load page
    # time.sleep(1)  # Make sure we had enough time to load everything

    al = {
        'name': 'Ohio Revised Code',
        'path': '',
    }
    # print table.text
    # command = "MERGE (s:law {name:'statutes'}) MERGE (s)-[:Parent]->(a:law  {name: {name}, type: {type}, path:{path}, inserted:{inserted} }) return id(a) as id"
    # al['id'] = g.data(command, name=al['name'], path=al['path'], type=al['type'], inserted=al['inserted'])[0]['id']

    table = browser.find_elements_by_tag_name("h2")
    for title in table:
        a = title.find_element_by_tag_name("a")
        print a.text
        title_name = a.text
        print title_name
        bl = {
            'name': title_name.replace('\n',' '),
            'path':  '',
        }
        # if (len(bl['name']) > 2):
        #     command = "MATCH (t:law) WHERE ID(t) = {parent_id} MERGE (t)-[:Parent]->(a:law  {name: {name}, type: {type}, path:{path}, inserted:{inserted} }) return id(a) as id"
        #     bl['id'] = g.data(command, name=bl['name'], path=bl['path'], type=bl['type'], inserted=bl['inserted'], parent_id = al['id'])[0]['id']
        # else:
        #     bl['id'] = al['id']
        #
        href = a.get_attribute('href')
        if(href):
            # print href
            browser2.get(href)
            for chapter in browser2.find_elements_by_tag_name("h2"):
                a = chapter.find_element_by_tag_name("a")
                chap_name =  a.text
                print '      ', chap_name
                # print '     ',c.text, c_text
                #
                cl = {
                    'name': chap_name.replace('\n', ' '),
                    'path': bl['name'],
                }

                # if (len(cl['name']) > 2):
                #     command = "MATCH (t:law) WHERE ID(t) = {parent_id} MERGE (t)-[:Parent]->(a:law  {name: {name}, type: {type}, path:{path}, inserted:{inserted} }) return id(a) as id"
                #     cl['id'] = g.data(command, name=cl['name'], path=cl['path'], type=cl['type'], inserted=cl['inserted'], parent_id=bl['id'])[0]['id']
                # else:
                #     cl['id'] = bl['id']
                a = chapter.find_element_by_tag_name("a")
                href = a.get_attribute('href')
                if (href):
                    # print href
                    browser3.get(href)
                    count = 1;
                    for section in browser3.find_elements_by_tag_name("h2"):
                        a = section.find_element_by_tag_name("a")
                        sec_name = a.text
                        print '          ',sec_name
                        # ps = section.find_elements_by_xpath('.//following-sibling::*[1][self::p]')
                        # ps = section.find_elements_by_xpath('./following-sibling::p[following-sibling::h2[1]]')
                        xpath = '//h2['+str(count)+']/following-sibling::p[count(preceding-sibling::h2) = '+str(count)+']'
                        ps = section.find_elements_by_xpath(xpath)
                        text = ''
                        for p in ps:
                            text = text + p.text + '\n'

                        count = count+1;

                        sl = {
                            'name': sec_name.replace('\n',' '),
                            'path': cl['path'] + ' > ' + cl['name'],
                            'text': text
                        }
                        print sl['path'].split(' > ')
                        # if (len(sl['name']) > 2):
                        #     command = "MATCH (t:law) WHERE ID(t) = {parent_id} MERGE (t)-[:Parent]->(a:law  {name: {name}, type: {type}, text: {text}, path:{path}, inserted:{inserted} }) return id(a) as id"
                        #     sl['id'] = g.data(command, name=sl['name'], path=sl['path'], type=sl['type'], text=sl['text'],
                        #                       inserted=sl['inserted'], parent_id=cl['id'])[0]['id']
                        # else:
                        #     sl['id'] = cl['id']






except Exception, e:
    traceback.print_exc()
    pass

finally:
    browser.quit()
    browser2.quit()
    browser3.quit()
    # browser4.quit()
    # browser5.quit()
    sys.exit()
