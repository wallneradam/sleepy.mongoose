# noinspection PyPackageRequirements
from restclient import GET, POST

import json
import unittest


class TestPOST(unittest.TestCase):
    def setUp(self):
        POST("http://localhost:27080/_connect")
        self._drop_collection()

    def _drop_collection(self):
        POST("http://localhost:27080/test/_cmd",
             params={'cmd': '{"drop" : "mongoose"}'})

    def test_insert_err1(self):
        s = POST("http://localhost:27080/_insert",
                 params={'docs': '[{"foo" : "bar"}]'},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 0)
        self.assertEquals(obj['errmsg'], 'db and collection must be defined')

    def test_insert_err2(self):
        s = POST("http://localhost:27080/test/_insert",
                 params={'docs': '[{"foo" : "bar"}]'},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 0)
        self.assertEquals(obj['errmsg'], 'db and collection must be defined')

    def test_insert_err3(self):
        s = POST("http://localhost:27080/test/mongoose/_insert",
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 0)
        self.assertEquals(obj['errmsg'], 'missing docs')

    def test_insert(self):
        s = POST("http://localhost:27080/test/mongoose/_insert",
                 params={'docs': '[{"foo" : "bar"}]'},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(type(obj['oids'][0]['$oid']).__name__, "unicode")

    def test_safe_insert(self):
        s = POST("http://localhost:27080/test/mongoose/_insert",
                 params={'docs': '[{"foo" : "bar"}]', 'safe': 1},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(type(obj['oids'][0]['$oid']).__name__, "unicode")
        self.assertEquals(obj['status']['ok'], 1)
        self.assertEquals(obj['status']['err'], None)

    def test_safe_insert_err1(self):
        s = POST("http://localhost:27080/test/mongoose/_insert",
                 params={'docs': '[{"_id" : "bar"}, {"_id" : "bar"}]', 'safe': 1},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['status']['ok'], 1)
        self.assertEquals(obj['status']['code'], 11000)

    def test_update_err1(self):
        s = POST("http://localhost:27080/_update",
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 0)
        self.assertEquals(obj['errmsg'], 'db and collection must be defined')

    def test_update_err2(self):
        s = POST("http://localhost:27080/test/_update",
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 0)
        self.assertEquals(obj['errmsg'], 'db and collection must be defined')

    def test_update_err3(self):
        s = POST("http://localhost:27080/test/mongoose/_update",
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 0)
        self.assertEquals(obj['errmsg'], 'missing criteria')

    def test_update_err4(self):
        s = POST("http://localhost:27080/test/mongoose/_update",
                 params={"criteria": "{}"},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 0)
        self.assertEquals(obj['errmsg'], 'missing newobj')

    def test_update(self):
        s = POST("http://localhost:27080/test/mongoose/_update",
                 params={"criteria": "{}", "newobj": '{"$set" : {"x" : 1}}'},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)

    def test_safe(self):
        s = POST("http://localhost:27080/test/mongoose/_update",
                 params={"criteria": "{}", "newobj": '{"$set" : {"x" : 1}}', "safe": "1"},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1)
        self.assertEquals(obj['n'], 0)
        self.assertEquals(obj['err'], None)

    def test_upsert(self):
        s = POST("http://localhost:27080/test/mongoose/_update",
                 params={"criteria": "{}", "newobj": '{"$set" : {"x" : 1}}', "upsert": "1", "safe": "1"},
                 async=False)

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)
        self.assertEquals(obj['n'], 1, s)

        s = GET("http://localhost:27080/test/mongoose/_find")
        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)
        self.assertEquals(obj['results'][0]['x'], 1, s)

    def test_multi(self):
        POST("http://localhost:27080/test/mongoose/_insert",
             params={"docs": '[{"x" : 1},{"x" : 1},{"x" : 1},{"y" : 1}]'},
             async=False)

        s = POST("http://localhost:27080/test/mongoose/_update",
                 params={"criteria": '{"x" : 1}', "newobj": '{"$set" : {"x" : 2}}', "multi": "1", "safe": "1"},
                 async=False)

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)
        self.assertEquals(obj['n'], 3, s)

    def test_remove(self):
        POST("http://localhost:27080/test/mongoose/_insert",
             params={"docs": '[{"x" : 1},{"x" : 1},{"x" : 1},{"y" : 1}]'},
             async=False)

        s = POST("http://localhost:27080/test/mongoose/_remove",
                 async=False)

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)

    def test_remove_safe(self):
        POST("http://localhost:27080/test/mongoose/_insert",
             params={"docs": '[{"x" : 1},{"x" : 1},{"x" : 1},{"y" : 1}]'},
             async=False)

        s = POST("http://localhost:27080/test/mongoose/_remove",
                 params={"criteria": '{"x" : 1}', "safe": 1},
                 async=False)

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)
        self.assertEquals(obj['n'], 3, s)

        s = POST("http://localhost:27080/test/mongoose/_remove",
                 params={"safe": "1"},
                 async=False)

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)
        self.assertEquals(obj['n'], 1, s)


if __name__ == '__main__':
    unittest.main()
