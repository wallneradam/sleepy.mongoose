# noinspection PyPackageRequirements
from restclient import GET, POST

import json
import unittest


class TestGET(unittest.TestCase):
    def setUp(self):
        POST("http://localhost:27080/_connect",
             params={'server': 'localhost:27017'})
        self._drop_collection()

    def _drop_collection(self):
        POST("http://localhost:27080/test/_cmd",
             params={'cmd': '{"drop" : "mongoose"}'})

    def test_hello(self):
        s = GET("http://localhost:27080/_hello")

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1)
        self.assertEquals(obj['msg'], "Uh, we had a slight weapons " +
                          "malfunction, but uh... everything's perfectly " +
                          "all right now. We're fine. We're all fine here " +
                          "now, thank you. How are you?")

    def test_find(self):
        POST("http://localhost:27080/test/mongoose/_insert",
             params={'docs': '[{"x" : 1},{"x" : 2},{"x" : 3}]'},
             async=False)

        s = GET("http://localhost:27080/test/mongoose/_find")

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['ok'], 1, s)
        self.assertEquals(type(obj['id']).__name__, "int", s)
        self.assertEquals(len(obj['results']), 3, s)

    def test_find_sort(self):
        POST("http://localhost:27080/test/mongoose/_insert",
             params={'docs': '[{"x" : 1},{"x" : 2},{"x" : 3}]'},
             async=False)

        s = GET("http://localhost:27080/test/mongoose/_find",
                {"sort": '{"x" : -1}'})

        self.assertEquals(type(s).__name__, "str")

        obj = json.loads(s)

        self.assertEquals(obj['results'][0]['x'], 3, s)
        self.assertEquals(obj['results'][1]['x'], 2, s)
        self.assertEquals(obj['results'][2]['x'], 1, s)


if __name__ == '__main__':
    unittest.main()
