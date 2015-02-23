# Copyright 2009-2010 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modified by Adam Wallner - Bitbaro Mobile kft

# noinspection PyPackageRequirements
from bson.son import SON
# noinspection PyPackageRequirements
from bson import json_util
from pymongo import Connection, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure, AutoReconnect

import re

try:
    import json
except ImportError:
    import simplejson as json


def esc(s):
    return str(s).replace('"', '\\"')


class MongoHandler:
    mh = None

    _cursor_id = 0

    def __init__(self, mongos):
        self.connections = {}

        for host in mongos:
            args = {"server": host}

            out = MongoFakeStream()
            if len(mongos) == 1:
                name = "default"
            else:
                name = host.replace(".", "")
                name = name.replace(":", "")

            self._connect(args, out.ostream, name=name)

    def _get_connection(self, name=None, uri='mongodb://localhost:27017'):
        if name is None:
            name = "default"

        if name in self.connections:
            return self.connections[name]

        try:
            connection = Connection(uri, network_timeout=2)
        except (ConnectionFailure, ConfigurationError):
            return None

        self.connections[name] = connection
        return connection

    def _get_host_and_port(self, server):
        host = "localhost"
        port = 27017

        if len(server) == 0:
            return host, port

        m = re.search('([^:]+):([0-9]+)?', server)
        if m is None:
            return host, port

        handp = m.groups()

        if len(handp) >= 1:
            host = handp[0]
        if len(handp) == 2 and handp[1] is not None:
            port = int(handp[1])

        return host, port

    # noinspection PyMethodParameters
    def sm_object_hook(obj):
        if "$pyhint" in obj:
            temp = SON()
            # noinspection PyUnresolvedReferences
            for pair in obj['$pyhint']:
                temp[pair['key']] = pair['value']
            return temp
        else:
            return json_util.object_hook(obj)

    def _get_json(self, s, out):
        try:
            obj = json.loads(s, object_hook=json_util.object_hook)
        except (ValueError, TypeError):
            out('{"ok": 0, "errmsg": "couldn\'t parse json: %s"}' % esc(s))
            return None

        if not getattr(obj, '__iter__', False):
            out('{"ok": 0, "errmsg": "type is not iterable: %s"}' % s)
            return None

        return obj

    # noinspection PyUnusedLocal,PyUnresolvedReferences
    def _cmd(self, args, out, name=None, db=None, collection=None):
        if name is None:
            name = "default"

        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        cmd = self._get_json(args['cmd'], out)
        if cmd is None:
            return

        try:
            result = conn[db].command(cmd, check=False)
        except AutoReconnect:
            out('{"ok": 0, "errmsg": "wasn\'t connected to the db and ' +
                'couldn\'t reconnect", "name": "%s"}' % name)
            return
        except (OperationFailure, error):
            out('{"ok": 0, "errmsg": "%s"}' % error)
            return

        # debugging
        if result['ok'] == 0:
            result['cmd'] = args['cmd']

        out(json.dumps(result, default=json_util.default))

    # noinspection PyUnusedLocal
    def _hello(self, args, out, name=None, db=None, collection=None):
        out('{"ok": 1, "msg": "Uh, we had a slight weapons malfunction, but ' +
            'uh... everything\'s perfectly all right now. We\'re fine. We\'re ' +
            'all fine here now, thank you. How are you?"}')
        return

    # noinspection PyUnusedLocal
    def _status(self, args, out, name=None, db=None, collection=None):
        result = {"ok": 1, "connections": {}}

        for name, conn in self.connections.iteritems():
            result['connections'][name] = "%s:%d" % (conn.host, conn.port)

        out(json.dumps(result))

    # noinspection PyUnusedLocal
    def _connect(self, args, out, name=None, db=None, collection=None):
        """
        connect to a mongod
        """
        if "server" in args:
            try:
                uri = args['server']
            except Exception, e:
                print e
                out('{"ok": 0, "errmsg": "invalid server uri given"}')
                return
        else:
            uri = 'mongodb://localhost:27017'

        if name is None:
            name = "default"

        conn = self._get_connection(name, uri)
        if conn is not None:
            out('{"ok": 1, "server": "%s", "name": "%s"}' % (uri, name))
        else:
            out('{"ok": 0, "errmsg": "could not connect", "server": "%s", "name": "%s"}' % (uri, name))

    # noinspection PyUnusedLocal
    def _authenticate(self, args, out, name=None, db=None, collection=None):
        """
        authenticate to the database.
        """
        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        if db is None:
            out('{"ok": 0, "errmsg": "db must be defined"}')
            return

        if 'username' not in args:
            out('{"ok": 0, "errmsg": "username must be defined"}')

        if 'password' not in args:
            out('{"ok": 0, "errmsg": "password must be defined"}')

        if not conn[db].authenticate(args['username'], args['password']):
            out('{"ok": 0, "errmsg": "authentication failed"}')
        else:
            out('{"ok": 1}')

    # noinspection PyUnusedLocal
    def _disconnect(self, args, out, name=None, db=None, collection=None):
        """
        disconnect from the database
        """
        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        conn.disconnect()
        del self.connections[name]

        out('{"ok": 1}')

    def _find(self, args, out, name=None, db=None, collection=None):
        """
        query the database.
        """
        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        if db is None or collection is None:
            out('{"ok": 0, "errmsg": "db and collection must be defined"}')
            return

        criteria = {}
        if 'criteria' in args:
            criteria = self._get_json(args['criteria'][0], out)
            if None == criteria:
                return

        fields = None
        if 'fields' in args:
            fields = self._get_json(args['fields'][0], out)
            if fields is None:
                return

        limit = 0
        if 'limit' in args:
            limit = int(args['limit'][0])

        skip = 0
        if 'skip' in args:
            skip = int(args['skip'][0])

        cursor = conn[db][collection].find(spec=criteria, fields=fields, limit=limit, skip=skip)

        if 'sort' in args:
            sort = self._get_json(args['sort'][0], out)
            if sort is None:
                return

            stupid_sort = []

            for field in sort:
                if sort[field] == -1:
                    stupid_sort.append([field, DESCENDING])
                else:
                    stupid_sort.append([field, ASCENDING])

            cursor.sort(stupid_sort)

        if 'explain' in args and bool(args['explain'][0]):
            out(json.dumps({"results": [cursor.explain()], "ok": 1}, default=json_util.default))

        if not hasattr(self, "cursors"):
            setattr(self, "cursors", {})

        _id = MongoHandler._cursor_id
        MongoHandler._cursor_id += 1

        cursors = getattr(self, "cursors")
        cursors[_id] = cursor
        setattr(cursor, "id", _id)

        batch_size = 15
        if 'batch_size' in args:
            batch_size = int(args['batch_size'][0])

        self.__output_results(cursor, out, batch_size)

    # noinspection PyUnusedLocal
    def _more(self, args, out, name=None, db=None, collection=None):
        """
        Get more results from a cursor
        """
        if 'id' not in args:
            out('{"ok": 0, "errmsg": "no cursor id given"}')
            return

        _id = int(args["id"][0])
        cursors = getattr(self, "cursors")

        if _id not in cursors:
            out('{"ok": 0, "errmsg": "couldn\'t find the cursor with id %d"}' % _id)
            return

        cursor = cursors[_id]

        batch_size = 15
        if 'batch_size' in args:
            batch_size = int(args['batch_size'][0])

        self.__output_results(cursor, out, batch_size)

    def __output_results(self, cursor, out, batch_size=15):
        """
        Iterate through the next batch
        """
        batch = []

        try:
            while len(batch) < batch_size:
                batch.append(cursor.next())
        except AutoReconnect:
            out(json.dumps({"ok": 0, "errmsg": "auto reconnecting, please try again"}))
            return
        except OperationFailure, of:
            out(json.dumps({"ok": 0, "errmsg": "%s" % of}))
            return
        except StopIteration:
            # this is so stupid, there's no has_next?
            pass

        out(json.dumps({"results": batch, "id": cursor.id, "ok": 1}, default=json_util.default))

    def _insert(self, args, out, name=None, db=None, collection=None):
        """
        insert a doc
        """
        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        if db is None or collection is None:
            out('{"ok": 0, "errmsg": "db and collection must be defined"}')
            return

        if "docs" not in args:
            out('{"ok": 0, "errmsg": "missing docs"}')
            return

        docs = self._get_json(args['docs'], out)
        if docs is None:
            return

        safe = False
        if "safe" in args:
            safe = bool(args['safe'])

        result = {'oids': conn[db][collection].insert(docs)}
        if safe:
            result['status'] = conn[db].last_status()

        out(json.dumps(result, default=json_util.default))

    def __safety_check(self, args, out, db):
        safe = False
        if "safe" in args:
            safe = bool(args['safe'])

        if safe:
            result = db.last_status()
            out(json.dumps(result, default=json_util.default))
        else:
            out('{"ok": 1}')

    def _update(self, args, out, name=None, db=None, collection=None):
        """
        update a doc
        """
        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        if db is None or collection is None:
            out('{"ok": 0, "errmsg": "db and collection must be defined"}')
            return

        upsert = False
        if "upsert" in args:
            upsert = bool(args['upsert'])

        multi = False
        if "multi" in args:
            multi = bool(args['multi'])

        newobj = None
        if "newobj" in args:
            newobj = self._get_json(args['newobj'], out)
        if newobj is None:
            out('{"ok": 0, "errmsg": "missing newobj"}')
            return

        collection = conn[db][collection]

        criteria = None
        if "criteria" not in args:
            criteria = self._get_json(args['criteria'], out)
        if upsert and not multi and criteria is None:
            # Get the criteria from the object and indices
            ii = collection.index_information()
            criteria = {}
            for (ik, o) in ii.items():
                if ik == '_id_' or 'unique' in o and o['unique']:
                    key = o['key']
                    for k in dict(key):
                        if k in newobj:
                            criteria[k] = newobj[k]

        if not criteria:
            out('{"ok": 0, "errmsg": "missing criteria"}')
            return

        collection.update(criteria, newobj, upsert=upsert, multi=multi)

        self.__safety_check(args, out, conn[db])

    def _insert_or_update(self, args, out, name=None, db=None, collection=None):
        """
        insert or update a document in the DB
        """
        args['upsert'] = 1
        self._update(args, out, name, db, collection)

    def _remove(self, args, out, name=None, db=None, collection=None):
        """
        remove docs
        """
        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        if db is None or collection is None:
            out('{"ok": 0, "errmsg": "db and collection must be defined"}')
            return

        criteria = {}
        if "criteria" in args:
            criteria = self._get_json(args['criteria'], out)
            if criteria is None:
                return

        conn[db][collection].remove(criteria)

        self.__safety_check(args, out, conn[db])

    # noinspection PyUnusedLocal
    def _batch(self, args, out, name=None, db=None, collection=None):
        """
        batch process commands
        """
        requests = self._get_json(args['requests'], out)
        if requests is None:
            return

        out("[")

        first = True
        for request in requests:
            if "cmd" not in request:
                continue

            cmd = request['cmd']
            method = "GET"
            if 'method' in request:
                method = request['method']

            db = None
            if 'db' in request:
                db = request['db']

            collection = None
            if 'collection' in request:
                collection = request['collection']

            args = {}
            name = None
            if 'args' in request:
                args = request['args']
                if 'name' in args:
                    name = args['name']

            func = getattr(MongoHandler.mh, cmd, None)
            if callable(func):
                output = MongoFakeStream()
                func(args, output.ostream, name=name, db=db, collection=collection)
                if not first:
                    out(",")
                first = False

                out(output.get_ostream())
            else:
                continue

        out("]")

    def _ensure_index(self, args, out, name=None, db=None, collection=None):
        """
        Ensure if the collection has index, if not it will be created, if yes, the result will be cached
        """
        conn = self._get_connection(name)
        if conn is None:
            out('{"ok": 0, "errmsg": "couldn\'t get connection to mongo"}')
            return

        if db is None or collection is None:
            out('{"ok": 0, "errmsg": "db and collection must be defined"}')
            return

        keys = None
        if "keys" in args:
            keys = self._get_json(args['keys'], out)
        if keys is None:
            out('{"ok": 0, "errmsg": "missing keys"}')
            return

        options = {}
        if "options" in args:
            options = self._get_json(args['options'], out)

        try:
            name = conn[db][collection].ensure_index(keys.items(), **options)
            out('{"ok": 1, "name": "%s"}' % name)
        except Exception, e:
            out('{"ok": 0, "errmsg": "%s"}' % esc(e.message))


class MongoFakeStream:
    def __init__(self):
        self.str = ""

    def ostream(self, content):
        self.str = self.str + content

    def get_ostream(self):
        return self.str
