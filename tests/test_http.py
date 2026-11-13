import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from run import Handler
from store import Store


class HttpTests(unittest.TestCase):
    def test_actual_mvcc_commands_validation_abort_and_http_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.store=Store(Path(tmp)/'db');thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            def request(method,path,body=None,headers=None):
                c=HTTPConnection(*server.server_address,timeout=3);c.request(method,path,None if body is None else json.dumps(body),{'Content-Type':'application/json',**(headers or {})});r=c.getresponse();status=r.status;raw=r.read();c.close();return status,json.loads(raw) if raw else None
            def command(action,tx,**kw):return request('POST','/api/workspaces/lab/commands',{'action':action,'tx':tx,**kw})
            try:
                self.assertEqual(request('POST','/api/workspaces',{'id':'lab','mode':'serializable','records':{'east':1,'west':1}})[0],201)
                for tx in ('A','B'):self.assertEqual(command('begin',tx,client=tx)[0],200);command('read',tx,key='east');command('read',tx,key='west')
                command('write','A',key='east',value=0);command('write','B',key='west',value=0);self.assertEqual(command('commit','A')[0],200)
                status,result=command('commit','B');self.assertEqual(status,409);self.assertEqual(result['event']['result']['transaction']['conflicts'][0]['type'],'read_changed')
                self.assertEqual(request('GET','/api/workspaces/lab')[1]['state']['head'],1);self.assertEqual(request('HEAD','/api/workspaces/lab'),(200,None));self.assertEqual(request('GET','/store.py')[0],404)
                self.assertEqual(request('GET','/api/workspaces',headers={'Host':'other.example'})[0],400);self.assertEqual(request('POST','/api/workspaces',{},headers={'Origin':'https://example.com'})[0],403)
            finally:server.shutdown();server.server_close();thread.join()
