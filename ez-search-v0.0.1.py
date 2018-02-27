# !/usr/bin/env python
# -*- coding: utf-8 -*-


import re
import os
import json
import time
import sqlite3
import hashlib
import base64
import cPickle
import gzip

import pprint

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options
from tornado.web import url


DOCUMENT_DIR = "document"      # 文档数据库目录
DICTIONARY_DIR = "dictionary"  # 词典数据库目录
FIELD_DIR = "field"            # 字段数据库目录
INVERTED_DIR = "inverted"      # 倒排列表数据目录


# init 初始化部分


def init_dir():
    for d in [DOCUMENT_DIR, DICTIONARY_DIR, FIELD_DIR, INVERTED_DIR]:
        if not os.path.exists(d):
            os.mkdir(d)
    dirs = ["%02X" % i for i in xrange(256)]
    return len([os.makedirs(i) for i in (os.path.join(INVERTED_DIR, "%s/%s" % (x, y)) for x in dirs for y in dirs)
                if not os.path.exists(i)])


# common 通用


def text_to_base64(s):
    return base64.b64encode(s)


def base64_to_text(b):
    return base64.b64encode(b)


def text_md5(t):
    return hashlib.md5(t).hexdigest().upper()


# sqlite 数据库相关


def create_db(db_name):
    conn = sqlite3.connect(db_name)
    return conn, conn.cursor()


def create_document_table(c, table_name):
    sql = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, document VARCHAR(2048));" % table_name
    return c.execute(sql)


def create_dictionary_table(c, table_name):
    sql = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, " \
          "word VARCHAR(24) NOT NULL UNIQUE, path VARCHAR(32));" % table_name
    return c.execute(sql)


def create_field_table(c, table_name):
    sql = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, " \
          "field VARCHAR(32) NOT NULL UNIQUE);" % table_name
    return c.execute(sql)


def close_db(con, cur):
    cur.close()
    con.close()


def insert_data(cur, table_name, data_dict):
    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join('?' * len(data_dict))
    insert_sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, columns, placeholders)
    cur.execute(insert_sql, data_dict.values())
    return cur.lastrowid


def search_data(cur, table_name, search_field, data_tuple):
    sql = "SELECT * FROM %s WHERE %s=?" % (table_name, search_field)
    cur.execute(sql, data_tuple)


def init_db(index, db_dir):
    db_path = os.path.join(db_dir, "{}.DB".format(text_md5(index)))
    return create_db(db_path)


# Document Manager 文档管理器


def document_insert(index, log_type, document_dict):  # 文档管理器, 底层由sqlite, 记录原始文档, 并返回ID
    con, cur = init_db(index, DOCUMENT_DIR)
    try:
        create_document_table(cur, log_type)
        return insert_data(cur, log_type, {"document": json.dumps(document_dict)})
    except Exception as error:
        raise Exception("document insert error: %s" % error)
    finally:
        con.commit()
        close_db(con=con, cur=cur)


def document_search(index, log_type, id_list):
    con, cur = init_db(index, DOCUMENT_DIR)
    try:
        rlist = []
        for i in id_list:
            search_data(cur, log_type, "id", (i,))
            rlist.append(cur.fetchone())
        return [r[1] for r in rlist if isinstance(r, tuple)]
    except Exception as error:
        raise Exception("document search error: %s" % error)
    finally:
        close_db(con=con, cur=cur)


# Indexer 索引构建器
T1 = {"a": "i like search engine",
      "b": "i like google"}
T2 = {"a": {"b": "i like search engine",
            "c": "i like google"}}
T3 = {"a": {"b": "i like search engine"},
      "c": {"d": "i like google"}}
T4 = {"a": {"b": {"c": "i like search engine"}}}
T5 = {"a": {"b": "i like search"},
      "c": "i like google"}


def analy_data(d):
    field = dict()
    for k1, v1 in d.iteritems():
        field[k1] = ""
        if isinstance(v1, dict):
            for k2, v2 in v1.iteritems():
                field["{}.{}".format(k1, k2)] = ""
                if isinstance(v2, dict):
                    for k3, v3 in v2.iteritems():
                        field["{}.{}.{}".format(k1, k2, k3)] = ""
                        if isinstance(v3, dict):
                            raise Exception("data error")
                        elif isinstance(v3, (str, unicode)):
                            field["{}.{}.{}".format(k1, k2, k3)] = v3.encode("utf-8")
                        del field["{}.{}".format(k1, k2)]
                elif isinstance(v2, (str, unicode)):
                    field["{}.{}".format(k1, k2)] = v2.encode("utf-8")
            del field[k1]
        elif isinstance(v1, str):
            field[k1] = v1
    return field


def test_analy_data():
    print analy_data(T1)
    print analy_data(T2)
    print analy_data(T3)
    print analy_data(T4)


def spliter(t):
    if isinstance(t, str):
        return re.split(r"\s+", t)
    raise Exception("text not str")


def test_spliter():
    print spliter("i like search")
    print spliter("search@163.com")
    print spliter("i/like/search")
    print spliter("i like search, call search@163.com")
    print spliter("127.0.0.1")


def inverted_list(wlist, document_id, field_id):  # word list
    return {i: [(document_id, field_id)] for i in set(wlist)}


def indexer(index, log_type, document, document_id):
    rdict = dict()
    for field, text in analy_data(document).iteritems():
        field_id = field_exists(index, log_type, field)  # insert field
        if not field_id:
            field_id = field_insert(index, log_type, field)
        for word, pt in inverted_list(spliter(text), document_id, field_id).iteritems():
            if word not in rdict:
                rdict[word] = pt
            rdict[word] = list(set(rdict[word] + pt))
    return rdict


def test_indexer():
    pprint.pprint(indexer("test", "test", T3, 1))


# Index Manage 索引管理器


def field_insert(index, log_type, field):
    con, cur = init_db(index, FIELD_DIR)
    try:
        create_field_table(cur, log_type)
        return insert_data(cur, log_type, {"field": field})
    except Exception as error:
        raise Exception("field insert error: %s" % error)
    finally:
        con.commit()
        close_db(con=con, cur=cur)


def field_exists(index, log_type, field):
    con, cur = init_db(index, FIELD_DIR)
    try:
        create_field_table(cur, log_type)
        search_data(cur, log_type, "field", (field,))
        r = cur.fetchone()
        if r:
            return r[0]
        return 0
    except Exception as error:
        raise Exception("field search exists error: %s" % error)
    finally:
        close_db(con, cur)


def field_search(index, log_type, field):
    con, cur = init_db(index, FIELD_DIR)
    try:
        create_field_table(cur, log_type)
        search_data(cur, log_type, "field", (field,))
        return cur.fetchone()
    except Exception as error:
        raise Exception("field search error: %s" % error)
    finally:
        close_db(con, cur)


def test_field_insert():
    print field_insert("test", "test", "a.c")


def test_field_exists():
    print field_exists("test", "test", "a.c")


def postings_path(file_name):  # file name == md5
    level1_dir, level2_dir = file_name[:2], file_name[2:4]
    return os.path.join(INVERTED_DIR, level1_dir, level2_dir, "{}.IF".format(file_name))


def inverted_dump(file_name, data):
    f = gzip.open(postings_path(file_name), 'wb')
    try:
        cPickle.dump(data, f, protocol=2)
        return True
    except Exception as error:
        raise Exception("inverted dumps error: %s" % error)
    finally:
        f.close()


def inverted_load(file_name):
    f = gzip.open(postings_path(file_name), 'rb')
    try:
        rlist = cPickle.load(f)
        return [tuple(r) for r in rlist]
    except Exception as error:
        raise Exception("inverted load error: %s" % error)
    finally:
        f.close()


def test_inverted_dump():
    print inverted_dump("test", json.dumps([(1, 2), (2, 3)]))


def test_inverted_load():
    print inverted_load("1A34C69A4D64F218E227E570358F4269")


def dictionary_insert(index, log_type, word):
    con, cur = init_db(index, DICTIONARY_DIR)
    try:
        create_dictionary_table(cur, log_type)
        ipath = text_md5("{}.{}.{}".format(index, log_type, word))
        return insert_data(cur, log_type, {"word": word, "path": ipath})
    except Exception as error:
        raise Exception("dictionary insert error: %s" % error)
    finally:
        con.commit()
        close_db(con, cur)


def dictionary_exists(index, log_type, word):
    con, cur = init_db(index, DICTIONARY_DIR)
    try:
        create_dictionary_table(cur, log_type)
        search_data(cur, log_type, "word", (word,))
        r = cur.fetchone()
        if r:
            return r[2]
        return False
    except Exception as error:
        raise Exception("dictionary exists error: %s" % error)
    finally:
        close_db(con, cur)


def dictionary_search(index, log_type, word):
    con, cur = init_db(index, DICTIONARY_DIR)
    try:
        create_dictionary_table(cur, log_type)
        search_data(cur, log_type, "word", (word,))
        return cur.fetchone()
    except Exception as error:
        raise Exception()


def test_dictionary_insert():
    print dictionary_insert("test", "test", "hello")


def test_dictionary_exists():
    print dictionary_exists("test", "test", "hello")


def test_dictionary_search():
    print dictionary_search("test", "test", "hello")


def add_document(index, log_type, body):
    try:
        _id = document_insert(index, log_type, body)
        _index_info = indexer(index, log_type, body, _id)
        for word, ilist in _index_info.iteritems():
            _list_file_name = dictionary_exists(index, log_type, word)
            if not _list_file_name:
                if dictionary_insert(index, log_type, word):
                    _list_file_name = text_md5("{}.{}.{}".format(index, log_type, word))
            else:
                ilist = list(set(ilist + inverted_load(_list_file_name)))
            inverted_dump(_list_file_name, ilist)
        return _id
    except Exception as error:
        raise Exception("add document error: %s" % error)


def test_add_document():
    print add_document("test", "test", T1)


# Index Search 索引检索器


def id_set(id_list, operation):
    try:
        if operation == "must":
            return list(reduce(lambda x, y: x & y, [set(idl) for idl in id_list]))
        elif operation == "should":
            return list(reduce(lambda x, y: x | y, [set(idl) for idl in id_list]))
        return []
    except:
        return []


def index_search(index, log_type, body, operation):
    id_list, rlist = [], []
    for fd, sd in body.iteritems():
        fd_info = field_search(index, log_type, fd.encode("utf-8"))
        sd_list = spliter(sd.encode("utf-8"))
        print sd_list
        for _sd in sd_list:
            dict_info = dictionary_search(index, log_type, _sd)
            if dict_info:
                inverted_info = inverted_load(dict_info[2])
                if fd_info:
                    id_list.append([i[0] for i in inverted_info if i[1] == fd_info[0]])
    id_list = id_set(id_list, operation)
    if id_list:
        r = document_search(index, log_type, id_list)
        if r:
            rlist += [json.loads(s) for s in r]
    return rlist


def test_index_search():
    print index_search("test", "test", {"a.b": "search"}, "must")


define("port", default=9200, type=int)


class SearchHandler(tornado.web.RequestHandler):
    def post(self, index, log_type):
        size = self.get_argument("size", 10, strip=True)
        result, rlist = dict(), []
        try:
            begin_time = time.time()
            search_body = json.loads(self.request.body)
            if "query" in search_body:
                query_body = search_body["query"]
                if "match" in query_body:
                    match_body = query_body["match"]
                    print match_body
                    rlist = index_search(index, log_type, match_body, "must")
                elif "match_all" in query_body:
                    pass
                elif "must" in query_body:
                    must_body = query_body["must"]
                    rlist = index_search(index, log_type, must_body, "must")
                elif "should" in query_body:
                    should_body = query_body["should"]
                    rlist = index_search(index, log_type, should_body, "must")
                else:
                    pass
            user_time = time.time() - begin_time
            result["took"], result["status"] = int(user_time * 1000), 0
            result["index"], result["type"] = index, log_type
            result["hits"] = rlist[:int(size)]
        except Exception as error:
            result["status"], result["error"] = 1, error
        finally:
            self.write(json.dumps(result))


class InsertHandler(tornado.web.RequestHandler):
    def post(self, index, log_type):
        result = dict()
        try:
            begin_time = time.time()
            insert_body = json.loads(self.request.body)
            document_id = add_document(index, log_type, insert_body)
            user_time = time.time() - begin_time
            result["status"], result["took"] = 0, int(user_time * 1000)
            result["index"], result["type"] = index, log_type
            result["id"], result["version"] = document_id, "null"
            result["result"], result["created"] = "created", True
        except Exception as error:
            result["status"], result["error"] = 1, error
        finally:
            self.write(json.dumps(result))


class Application(tornado.web.Application):
    def __init__(self, **overrides):
        handlers = [
            url(r'/(.*)/(.*)/_search', SearchHandler),
            url(r'/(.*)/(.*)', InsertHandler),
        ]
        settings = {
            'xsrf_cookies': False,
            'debug': True,
            'autoescape': None,
            'log_file_prefix': "tornado.log",
        }

        tornado.web.Application.__init__(self, handlers, **settings)


def main():
    init_dir()
    print "init dir success"
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    print "start little search server on port: %s" % options.port
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
